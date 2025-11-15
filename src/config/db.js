// src/config/db.js
import "dotenv/config";
import postgres from "postgres";

/**
 * Conexão PostgreSQL (Neon) para o worker no VPS.
 *
 * - Usa driver TCP normal (postgres) em vez do client HTTP da Neon.
 * - SSL obrigatório (Neon exige TLS).
 * - Pool pequeno e estável (max 10 ligações).
 * - Timeouts explícitos + retries em CONNECT_TIMEOUT.
 */

const rawUrl = process.env.DATABASE_URL;

if (!rawUrl) {
  console.error("[db] ❌ DATABASE_URL não definido nas env vars");
  throw new Error("DATABASE_URL missing");
}

const DB_MAX_CONNECTIONS = Number.parseInt(
  process.env.DB_MAX_CONNECTIONS || "10",
  10
);
const DB_IDLE_TIMEOUT = Number.parseInt(
  process.env.DB_IDLE_TIMEOUT || "30",
  10
);
const DB_CONNECT_TIMEOUT = Number.parseInt(
  process.env.DB_CONNECT_TIMEOUT || "10",
  10
);
const DB_RETRIES = Number.parseInt(process.env.DB_RETRIES || "2", 10); // nº de retries extra em CONNECT_TIMEOUT

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let baseSql;

try {
  const u = new URL(rawUrl);
  const safeHost = u.hostname;
  const safeDb = u.pathname.replace(/^\//, "") || "(default)";

  console.log("[db] Using PostgreSQL", {
    host: safeHost,
    database: safeDb,
    max: DB_MAX_CONNECTIONS,
    idle_timeout: DB_IDLE_TIMEOUT,
    connect_timeout: DB_CONNECT_TIMEOUT,
    retries: DB_RETRIES,
  });

  baseSql = postgres(rawUrl, {
    ssl: "require",
    max: DB_MAX_CONNECTIONS,
    idle_timeout: DB_IDLE_TIMEOUT,
    connect_timeout: DB_CONNECT_TIMEOUT,
    prepare: false,
    onnotice: (msg) => {
      try {
        console.warn("[db][notice]", msg?.message || msg);
      } catch {
        console.warn("[db][notice]", msg);
      }
    },
  });
} catch (e) {
  console.error("[db] ❌ Erro ao inicializar ligação PostgreSQL:", e);
  throw e;
}

/**
 * Wrapper em volta do tagged template do postgres.
 * - Re-tenta queries em caso de CONNECT_TIMEOUT.
 * - Mantém métodos auxiliares (`sql.begin`, `sql.end`, etc.).
 */
async function sqlWrapper(strings, ...values) {
  let attempt = 0;

  // tentativas = 1 (original) + DB_RETRIES
  const maxAttempts = 1 + DB_RETRIES;

  while (true) {
    attempt++;
    try {
      return await baseSql(strings, ...values);
    } catch (e) {
      const isConnTimeout =
        e &&
        (e.code === "CONNECT_TIMEOUT" ||
          e.errno === "CONNECT_TIMEOUT" ||
          e.message?.includes("CONNECT_TIMEOUT"));

      if (isConnTimeout && attempt < maxAttempts) {
        console.warn("[db] CONNECT_TIMEOUT, retrying query", {
          attempt,
          maxAttempts,
          code: e.code,
        });
        // pequeno backoff
        await sleep(200 * attempt);
        continue;
      }

      // outros erros, ou esgotámos retries → manda para cima
      throw e;
    }
  }
}

// Copia propriedades úteis do client original para o wrapper
export const sql = Object.assign(sqlWrapper, baseSql);
export default sql;
