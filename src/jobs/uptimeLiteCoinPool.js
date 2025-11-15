// src/jobs/uptimeLiteCoinPool.js
import cron from "node-cron";
import fetch from "node-fetch";
import { sql } from "../config/db.js";
import { redis } from "../config/upstash.js";

function slotISO(d = new Date()) {
  const m = d.getUTCMinutes();
  const q = m - (m % 15);
  const t = new Date(
    Date.UTC(
      d.getUTCFullYear(),
      d.getUTCMonth(),
      d.getUTCDate(),
      d.getUTCHours(),
      q,
      0
    )
  );
  return t.toISOString();
}

async function fetchLitecoinPoolWorkers(apiKey) {
  const url = `https://www.litecoinpool.org/api?api_key=${apiKey}`;
  const resp = await fetch(url);
  const data = await resp.json().catch(() => null);
  return data && data.workers ? data.workers : {};
}

// Controle de slot para evitar atualizar horas mais que 1x/slot
let lastSlot = null;
const updatedInSlot = new Set();

function beginSlot(s) {
  if (s !== lastSlot) {
    lastSlot = s;
    updatedInSlot.clear();
  }
}

function dedupeForHours(ids) {
  const out = [];
  for (const id of ids) {
    if (!updatedInSlot.has(id)) {
      updatedInSlot.add(id);
      out.push(id);
    }
  }
  return out;
}

export async function runUptimeLTCPoolOnce() {
  const sISO = slotISO();
  beginSlot(sISO);

  // lock específico da LiteCoinPool
  const lockKey = `uptime:${sISO}:ltcpool`;
  const gotLock = await redis.set(lockKey, "1", { nx: true, ex: 14 * 60 });
  if (!gotLock) {
    console.log(
      `[uptime:ltcpool] lock ativo (${sISO}) – ignorado nesta instância.`
    );
    return { ok: true, skipped: true };
  }

  let hoursUpdated = 0;
  let statusToOnline = 0;
  let statusToOffline = 0;

  try {
    // agrupa por api_key
    const miners = await sql/*sql*/`
      SELECT id, worker_name, api_key
      FROM miners
      WHERE pool = 'LiteCoinPool'
        AND api_key IS NOT NULL AND api_key <> ''
        AND worker_name IS NOT NULL AND worker_name <> ''
    `;
    if (!miners.length) return { ok: true, updated: 0, statusChanged: 0 };

    const groups = new Map(); // api_key -> Miner[]
    for (const m of miners) {
      const k = m.api_key;
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(m);
    }

    for (const [apiKey, list] of groups) {
      // IDs segundo a API
      const onlineIdsRaw = [];
      const offlineIdsRaw = [];

      try {
        const workers = await fetchLitecoinPoolWorkers(apiKey);

        for (const m of list) {
          const info = workers?.[m.worker_name]; // match exato
          const apiOnline = !!(info && info.connected === true);
          if (apiOnline) onlineIdsRaw.push(m.id);
          else offlineIdsRaw.push(m.id); // sem info ou connected=false => offline
        }
      } catch (e) {
        console.error("[uptime:ltcpool] erro grupo", apiKey, e);
      }

      // 1) Horas online (dedupe por slot) — NÃO contar se em manutenção
      const onlineIdsForHours = dedupeForHours(onlineIdsRaw);
      if (onlineIdsForHours.length) {
        await sql/*sql*/`
          UPDATE miners
          SET total_horas_online = COALESCE(total_horas_online, 0) + 0.25
          WHERE id = ANY(${onlineIdsForHours})
            AND lower(COALESCE(status, '')) <> 'maintenance'
        `;
        hoursUpdated += onlineIdsForHours.length;
      }

      // 2) Status (IGNORAR manutenção)
      if (onlineIdsRaw.length) {
        const r1 = await sql/*sql*/`
          UPDATE miners
          SET status = 'online'
          WHERE id = ANY(${onlineIdsRaw})
            AND status IS DISTINCT FROM 'online'
            AND lower(COALESCE(status, '')) <> 'maintenance'
          RETURNING id
        `;
        statusToOnline += Array.isArray(r1)
          ? r1.length
          : r1?.count || 0;
      }

      if (offlineIdsRaw.length) {
        const r2 = await sql/*sql*/`
          UPDATE miners
          SET status = 'offline'
          WHERE id = ANY(${offlineIdsRaw})
            AND status IS DISTINCT FROM 'offline'
            AND lower(COALESCE(status, '')) <> 'maintenance'
          RETURNING id
        `;
        statusToOffline += Array.isArray(r2)
          ? r2.length
          : r2?.count || 0;
      }

      console.log(
        `[uptime:ltcpool] grupo apiKey=*** – workers: ${
          list.length
        }, onlineAPI: ${onlineIdsRaw.length}, offlineAPI: ${
          offlineIdsRaw.length
        }`
      );
    }

    const statusChanged = statusToOnline + statusToOffline;
    console.log(
      `[uptime:ltcpool] ${sISO} – horas+: ${hoursUpdated}, status->online: ${statusToOnline}, status->offline: ${statusToOffline}`
    );
    return { ok: true, updated: hoursUpdated, statusChanged };
  } catch (e) {
    console.error("⛔ uptime:ltcpool", e);
    return { ok: false, error: String(e?.message || e) };
  }
}

export function startUptimeLTCPool() {
  cron.schedule(
    "*/15 * * * *",
    async () => {
      try {
        await runUptimeLTCPoolOnce();
      } catch (e) {
        console.error("⛔ ltcpool cron:", e);
      }
    },
    { timezone: "Europe/Lisbon" }
  );
  console.log("[jobs] LiteCoinPool (*/15) agendado.");
}
