// src/config/db.js
import "dotenv/config";
import postgres from "postgres";

/**
 * Conexão PostgreSQL (Neon) para o worker no VPS.
 *
 * - Usa driver TCP normal (postgres) em vez do client HTTP da Neon.
 * - SSL obrigatório (Neon exige TLS).
 * - Pool pequeno e estável (max 10 ligações).
 * - Timeouts explícitos para não ficares com sockets pendurados.
 */

const rawUrl = process.env.DATABASE_URL;

if (!rawUrl) {
  console.error("[db] ❌ DATABASE_URL não definido nas env vars");
  throw new Error("DATABASE_URL missing");
}

let sql;

try {
  const u = new URL(rawUrl);

  const safeHost = u.hostname;
  const safeDb = u.pathname.replace(/^\//, "") || "(default)";

  console.log("[db] Using PostgreSQL", {
    host: safeHost,
    database: safeDb,
  });

  sql = postgres(rawUrl, {
    // Neon TCP exige TLS
    ssl: "require",

    // Nº máximo de ligações no pool
    max: Number.parseInt(process.env.DB_MAX_CONNECTIONS || "10", 10),

    // Fecha ligações idle depois de X segundos
    idle_timeout: Number.parseInt(process.env.DB_IDLE_TIMEOUT || "30", 10),

    // Timeout (segundos) para estabelecer ligação
    connect_timeout: Number.parseInt(
      process.env.DB_CONNECT_TIMEOUT || "10",
      10
    ),

    /**
     * prepare: false → evita prepared statements server-side,
     * que com Neon/pooler/serverless às vezes causa chatices
     * em ligações recicladas.
     */
    prepare: false,

    /**
     * notices do servidor (ex: "statement_timeout", "deadlock", etc).
     * Não rebenta a app, só loga.
     */
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
 * Export principal do client SQL.
 * Usa sempre `import { sql } from "./config/db.js";`
 */
export { sql };
export default sql;
