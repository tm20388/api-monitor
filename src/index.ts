import { DurableObject } from "cloudflare:workers";

interface ErrorReport {
  event_id: string;
}

const WINDOW_MS = 60 * 60 * 1000;
const THRESHOLD = 10;

export class MyDurableObject extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Сохраняем совместимость с уже созданной таблицей.
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS errors (
        event_id TEXT PRIMARY KEY,
        received_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS errors_received_at
      ON errors(received_at);
    `);
  }

  async record(report: ErrorReport) {
    return this.ctx.blockConcurrencyWhile(async () => {
      const now = Date.now();

      const result = this.ctx.storage.transactionSync(() => {
        // Удаляем ошибки старше часа.
        this.ctx.storage.sql.exec(
          "DELETE FROM errors WHERE received_at <= ?",
          now - WINDOW_MS
        );

        // Повторное событие с тем же ID не считается.
        this.ctx.storage.sql.exec(
          `INSERT OR IGNORE INTO errors
           (event_id, received_at)
           VALUES (?, ?)`,
          report.event_id,
          now
        );

        const { count } = this.ctx.storage.sql
          .exec<{ count: number }>(
            "SELECT COUNT(*) AS count FROM errors"
          ).one();

        return {
          count,
          window_minutes: 60,
          threshold_reached: count >= THRESHOLD
        };
      });

      if (result.threshold_reached) {
        await this.ctx.storage.transaction(async (tx) => {
          const pending = await tx.get<boolean>("ping_pending");
          const lastSent = await tx.get<number>("last_ping_at");

          const cooldownPassed =
            lastSent === undefined || now - lastSent >= WINDOW_MS;

          // Одно ожидающее уведомление и пауза на час после успеха.
          if (!pending && cooldownPassed) {
            await tx.put("ping_pending", true);
            await tx.setAlarm(Date.now() + 1000);
          }
        });
      }

      return result;
    });
  }

  async alarm() {
    await this.ctx.blockConcurrencyWhile(async () => {
      const pending =
        await this.ctx.storage.get<boolean>("ping_pending");

      if (!pending) {
        return;
      }

      try {
        if (!this.env.PING_TOKEN) {
          throw new Error("Не задан PING_TOKEN");
        }

        console.log(
          "Отправка PING, объект:",
          this.ctx.id.toString()
        );

        const response = await fetch(
          "https://webhook.site/550ccb1b-f92e-4748-af88-f3dec520a14e",
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${this.env.PING_TOKEN}`
            },
            redirect: "manual",
            signal: AbortSignal.timeout(5000)
          }
        );

        const status = response.status;
        await response.body?.cancel();

        if (!response.ok) {
          throw new Error(`Сайт ответил HTTP ${status}`);
        }
      } catch (error) {
        console.error(
          "PING не доставлен, повтор через минуту:",
          error
        );

        await this.ctx.storage.setAlarm(Date.now() + 60_000);
        return;
      }

      await this.ctx.storage.transaction(async (tx) => {
        await tx.put("last_ping_at", Date.now());
        await tx.delete("ping_pending");
      });

      console.log("PING успешно отправлен на сайт");
    });
  }
}

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

function json(data: unknown, status = 200): Response {
  return Response.json(data, {
    status,
    headers: corsHeaders
  });
}

export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname !== "/report") {
      return json({ error: "Not found" }, 404);
    }

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders
      });
    }

    if (request.method !== "POST") {
      return json({ error: "Use POST /report" }, 405);
    }

    let report: ErrorReport;

    try {
      const body = await request.json<Partial<ErrorReport>>();

      if (
        typeof body?.event_id !== "string" ||
        !body.event_id.trim() ||
        body.event_id.length > 100
      ) {
        return json({ error: "Invalid event_id" }, 400);
      }

      report = {
        event_id: body.event_id
      };
    } catch {
      return json({ error: "Invalid JSON" }, 400);
    }

    // Один общий счётчик для всех пользователей.
    const stub = env.MY_DURABLE_OBJECT.getByName("app-api-errors");
    const result = await stub.record(report);

    return json(result);
  }
} satisfies ExportedHandler<Env>;
