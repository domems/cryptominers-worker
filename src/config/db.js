// src/config/db.js
import "dotenv/config";
import postgres from "postgres";

/**
 * Conexão PostgreSQL (Neon) para o worker no VPS.
 * - TLS obrigatório
 * - Pool com limites
 * - Retries em CONNECT_TIMEOUT
 * - Loga SQL em caso de erro de sintaxe (42601) com placeholders
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
const DB_RETRIES = Number.parseInt(process.env.DB_RETRIES || "2", 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// constrói texto da query com placeholders $1, $2... (sem valores reais)
function buildQueryText(strings, values) {
  let text = "";
  for (let i = 0; i < strings.length; i++) {
    text += strings[i];
    if (i < values.length) {
      text += `$${i + 1}`;
    }
  }
  return text.trim();
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

async function sqlWrapper(strings, ...values) {
  const text = buildQueryText(strings, values);
  let attempt = 0;
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
        });
        await sleep(200 * attempt);
        continue;
      }

      // log detalhado para syntax error
      if (e && e.code === "42601") {
        console.error("[db] SQL syntax error", {
          code: e.code,
          message: e.message,
          position: e.position,
          query: text,
        });
      }

      throw e;
    }
  }
}

// Copia propriedades úteis do client original para o wrapper
export const sql = Object.assign(sqlWrapper, baseSql);
export default sql;
