// src/config/db.js
import "dotenv/config";
import postgres from "postgres";

/**
 * IMPORTANTE:
 * Neste worker estamos num VPS sempre ligado (Contabo),
 * por isso em vez do client HTTP do Neon usamos driver TCP normal.
 *
 * DATABASE_URL neste projeto deve ser algo como:
 *   postgres://user:pass@ep-young-pond-adyd4alk-pooler.c-2.us-east-1.aws.neon.tech/neondb
 */

const rawUrl = process.env.DATABASE_URL;

if (!rawUrl) {
  console.error("[db] ❌ DATABASE_URL não definido no .env");
  throw new Error("DATABASE_URL missing");
}
 
let host = "unknown";
try {
  // truque só para extrair o host sem chorar com o esquema postgres
  const u = new URL(rawUrl.replace(/^postgres(ql)?:\/\//, "http://"));
  host = u.hostname;
} catch {
  // ignore
}

console.log("[db] Using PostgreSQL host:", host);

// Cria cliente SQL com SSL obrigatório (Neon exige TLS)
export const sql = postgres(rawUrl, {
  ssl: "require",
  max: 10,              // nº máx de conexões
  idle_timeout: 20,     // segundos antes de fechar idle
  connect_timeout: 10,  // segundos para timeout de connect
});

export default sql;
