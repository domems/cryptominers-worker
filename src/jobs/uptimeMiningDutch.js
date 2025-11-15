// src/jobs/uptimeMiningDutch.js
import cron from "node-cron";
import fetch from "node-fetch";
import { sql } from "../config/db.js";
import { redis } from "../config/upstash.js";

/* ===== time slot (15 min, UTC) ===== */
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

/* ===== helpers ===== */
const norm = (s) => String(s ?? "").trim();
const low = (s) => norm(s).toLowerCase();
const tail = (s) => {
  const str = norm(s);
  const i = str.lastIndexOf(".");
  return i >= 0 ? str.slice(i + 1) : str;
};
const head = (s) => {
  const str = norm(s);
  const i = str.indexOf(".");
  return i >= 0 ? str.slice(0, i) : str;
};

function algoFromCoin(coin) {
  const c = String(coin || "").trim().toUpperCase();
  if (c === "BTC") return "sha256";
  if (c === "LTC" || c === "DOGE") return "scrypt";
  return "";
}
function mapCoinSlug(coin) {
  const c = String(coin || "").trim().toUpperCase();
  if (c === "BTC") return "bitcoin";
  if (c === "LTC") return "litecoin";
  if (c === "DOGE") return "dogecoin";
  return "";
}
function buildCandidateUrls({ coin, account_id, api_key }) {
  const base = "https://www.mining-dutch.nl";
  const algo = algoFromCoin(coin);
  const coinSlug = mapCoinSlug(coin);
  const mk = (name) =>
    `${base}/pools/${name}.php?page=api&action=getuserworkers&id=${encodeURIComponent(
      account_id
    )}&api_key=${encodeURIComponent(api_key)}`;

  const urls = [];
  if (algo) urls.push(mk(algo)); // sha256.php / scrypt.php
  if (coinSlug) urls.push(mk(coinSlug)); // bitcoin.php / litecoin.php / dogecoin.php
  if (algo === "sha256") urls.push(mk("scrypt"));
  if (algo === "scrypt") urls.push(mk("sha256"));
  if (!algo && !coinSlug) urls.push(mk("sha256"), mk("scrypt"));
  return urls;
}

function isOnlineFromWorker(w) {
  // Mining-Dutch: alive=1/0, hashrate number
  const alive = Number(w?.alive ?? w?.online ?? 0);
  if (!Number.isNaN(alive) && alive > 0) return true;
  const hr = Number(w?.hashrate ?? w?.hash ?? 0);
  if (!Number.isNaN(hr) && hr > 0) return true;
  const st = low(w?.status);
  if (
    st &&
    ["alive", "online", "active", "up", "working", "connected"].includes(st)
  )
    return true;
  return false;
}

/** parser robusto para os formatos da Mining-Dutch */
function parseWorkersPayload(data) {
  if (!data || typeof data !== "object") return null;

  // 1) formato com chave topo "getuserworkers"
  if (data.getuserworkers && data.getuserworkers.data) {
    const miners =
      data.getuserworkers.data.miners ||
      data.getuserworkers.data.workers ||
      [];
    if (Array.isArray(miners)) {
      return miners.map((v, i) => ({
        name: norm(v?.worker ?? v?.name ?? v?.username ?? String(i)),
        username: norm(v?.username ?? v?.worker ?? v?.name ?? String(i)),
        hashrate: Number(v?.hashrate ?? v?.hash ?? 0),
        alive: Number(v?.alive ?? 0),
        status: norm(v?.status ?? ""),
        raw: v || {},
      }));
    }
    if (miners && typeof miners === "object") {
      return Object.entries(miners).map(([k, v]) => ({
        name: norm(v?.worker ?? v?.name ?? v?.username ?? k),
        username: norm(v?.username ?? v?.worker ?? v?.name ?? k),
        hashrate: Number(v?.hashrate ?? v?.hash ?? 0),
        alive: Number(v?.alive ?? 0),
        status: norm(v?.status ?? ""),
        raw: v || {},
      }));
    }
  }

  // 2) formatos alternativos
  const node = data?.data?.workers ?? data?.workers ?? data?.data ?? null;
  if (node && typeof node === "object" && !Array.isArray(node)) {
    return Object.entries(node).map(([k, v]) => ({
      name: norm(v?.worker ?? v?.name ?? v?.username ?? k),
      username: norm(v?.username ?? v?.worker ?? v?.name ?? k),
      hashrate: Number(v?.hashrate ?? v?.hash ?? 0),
      alive: Number(v?.alive ?? 0),
      status: norm(v?.status ?? ""),
      raw: v || {},
    }));
  }
  if (Array.isArray(node)) {
    return node.map((v, i) => ({
      name: norm(v?.worker ?? v?.name ?? v?.username ?? String(i)),
      username: norm(v?.username ?? v?.worker ?? v?.name ?? String(i)),
      hashrate: Number(v?.hashrate ?? v?.hash ?? 0),
      alive: Number(v?.alive ?? 0),
      status: norm(v?.status ?? ""),
      raw: v || {},
    }));
  }

  return null;
}

async function fetchMiningDutchWorkers({ coin, account_id, api_key }) {
  const urls = buildCandidateUrls({ coin, account_id, api_key });
  let lastErr;

  for (const url of urls) {
    try {
      const res = await fetch(url, { timeout: 12_000 });
      if (!res.ok) {
        console.warn("[miningdutch] HTTP", res.status, url);
        lastErr = new Error(`HTTP ${res.status}`);
        continue;
      }
      const data = await res.json().catch(() => null);
      const list = parseWorkersPayload(data);
      if (!list) {
        console.warn(
          "[miningdutch] schema inesperado",
          url,
          JSON.stringify(data)?.slice(0, 300)
        );
        continue;
      }
      // normalização final (usar username como nome se existir)
      return list.map((w) => ({
        name: norm(w.name || w.username),
        hashrate: Number(w.hashrate || 0),
        status: norm(w.status || (Number(w.alive) > 0 ? "alive" : "")),
        alive: Number(w.alive || 0),
        raw: w.raw || {},
      }));
    } catch (e) {
      lastErr = e;
      console.warn("[miningdutch] erro", url, e?.message || e);
    }
  }
  throw lastErr ?? new Error("All MiningDutch endpoints failed");
}

/* ===== controle de slot para não somar horas 2x/slot ===== */
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

/* ===== job principal ===== */
export async function runUptimeMiningDutchOnce() {
  const sISO = slotISO();
  beginSlot(sISO);

  const lockKey = `uptime:${sISO}:miningdutch`;
  const gotLock = await redis.set(lockKey, "1", { nx: true, ex: 20 * 60 });
  if (!gotLock) {
    console.log(`[uptime:miningdutch] lock ativo (${sISO}) – skip.`);
    return { ok: true, skipped: true };
  }

  let hoursUpdated = 0;
  let statusToOnline = 0;
  let statusToOffline = 0;

  try {
    const miners = await sql/*sql*/`
      SELECT id, worker_name, api_key, coin
      FROM miners
      WHERE pool = 'MiningDutch'
        AND api_key IS NOT NULL AND api_key <> ''
        AND worker_name IS NOT NULL AND worker_name <> ''
    `;
    if (!miners.length)
      return { ok: true, updated: 0, statusChanged: 0 };

    // group by api_key + account_id (head(worker_name)) + coin
    const groups = new Map();
    for (const m of miners) {
      const account_id = head(m.worker_name);
      const key = `${m.api_key}::${account_id}::${m.coin || ""}`;
      if (!groups.has(key))
        groups.set(key, {
          account_id,
          api_key: m.api_key,
          coin: m.coin,
          list: [],
        });
      groups.get(key).list.push(m);
    }

    for (const [, grp] of groups) {
      const { account_id, api_key, coin, list } = grp;
      const onlineIdsRaw = [];
      const offlineIdsRaw = [];

      try {
        const workers = await fetchMiningDutchWorkers({
          coin,
          account_id,
          api_key,
        });

        // index por tail (lowercase)
        const byTail = new Map();
        for (const w of workers)
          byTail.set(low(tail(w.name) || w.name), w);

        for (const m of list) {
          const t = low(tail(m.worker_name));
          const info = byTail.get(t);
          const apiOnline = !!(info && isOnlineFromWorker(info));
          if (apiOnline) onlineIdsRaw.push(m.id);
          else offlineIdsRaw.push(m.id);
        }
      } catch (e) {
        console.error("[uptime:miningdutch] erro grupo", { account_id, coin }, e?.message || e);
        for (const m of list) offlineIdsRaw.push(m.id);
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

      // 2) Status (só quando muda) — IGNORAR manutenção
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
        `[uptime:miningdutch] acct=${account_id} coin=${
          coin || "-"
        } workers=${list.length} onlineAPI=${onlineIdsRaw.length} offlineAPI=${
          offlineIdsRaw.length
        }`
      );
    }

    const statusChanged = statusToOnline + statusToOffline;
    console.log(
      `[uptime:miningdutch] ${sISO} – horas+: ${hoursUpdated}, ->online: ${statusToOnline}, ->offline: ${statusToOffline}`
    );
    return { ok: true, updated: hoursUpdated, statusChanged };
  } catch (e) {
    console.error("⛔ uptime:miningdutch", e);
    return { ok: false, error: String(e?.message || e) };
  }
}

export function startUptimeMiningDutch() {
  cron.schedule(
    "*/15 * * * *",
    async () => {
      try {
        await runUptimeMiningDutchOnce();
      } catch (e) {
        console.error("⛔ miningdutch cron:", e);
      }
    },
    { timezone: "Europe/Lisbon" }
  );
  console.log("[jobs] MiningDutch (*/15) agendado.");
}
