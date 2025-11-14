// src/config/db.js
import "dotenv/config";
import dns from "dns";
import fetchOrig from "node-fetch";
import { neon, neonConfig } from "@neondatabase/serverless";

// 1) Forçar IPv4 primeiro (IPv6 em Contabo é uma lotaria)
dns.setDefaultResultOrder?.("ipv4first");

// 2) Garantir que existe fetch global (Neon usa fetch por baixo)
if (!globalThis.fetch) {
  globalThis.fetch = fetchOrig;
}

// 3) Reutilizar conexões HTTP do Neon para não abrir mil sockets
neonConfig.fetchConnectionCache = true;

const rawUrl = process.env.DATABASE_URL;

if (!rawUrl) {
  console.error("[db] ❌ DATABASE_URL não definido no .env");
  throw new Error("DATABASE_URL missing");
}

// Log discreto só para confirmar host
let host = "unknown";
try {
  // trocamos o esquema para http só para o URL parser não chorar
  const u = new URL(rawUrl.replace(/^postgres(ql)?:\/\//, "http://"));
  host = u.hostname;
} catch {
  // ignore
}

console.log("[db] Using Neon DATABASE_URL host:", host);

// Cria conexão SQL
export const sql = neon(rawUrl);
export default sql;
