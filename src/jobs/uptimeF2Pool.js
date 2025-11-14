import cron from "node-cron";
import fetch from "node-fetch";
import dns from "dns";
import { sql } from "../config/db.js";
import { redis } from "../config/upstash.js";

/* ===== limpar proxies e forçar IPv4-first ===== */
for (const k of ["HTTP_PROXY","http_proxy","HTTPS_PROXY","https_proxy","NO_PROXY","no_proxy"]) {
  if (process.env[k]) { console.warn(`[uptime:f2pool] ignorando ${k}=${process.env[k]}`); delete process.env[k]; }
}
dns.setDefaultResultOrder?.("ipv4first");

/* ===== slot 15m (UTC) ===== */
function slotISO(d = new Date()) {
  const m = d.getUTCMinutes(), q = m - (m % 15);
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), q, 0));
  return t.toISOString();
}

/* ===== helpers ===== */
const clean = (s) => String(s ?? "").normalize("NFKC").replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
function splitAccountWorker(row) {
  const wn = clean(row.worker_name); const i = wn.indexOf(".");
  if (i <= 0) return { account: "", worker: "" };
  return { account: wn.slice(0, i), worker: wn.slice(i + 1) };
}
function tail(s) { const str = clean(s); const i = str.lastIndexOf("."); return i >= 0 ? str.slice(i + 1) : str; }
const workerKey = (w) => { const s = clean(w).toLowerCase(); const k = s.replace(/^0+/, ""); return k === "" ? "0" : k; };

function f2slug(coin) {
  const c = String(coin ?? "").trim().toUpperCase();
  if (c === "BTC") return "bitcoin";
  if (c === "BCH") return "bitcoin-cash";
  if (c === "BSV") return "bitcoin-sv";
  if (c === "LTC") return "litecoin";
  if (c === "KAS") return "kaspa";
  if (c === "CFX") return "conflux";
  if (c === "ETC") return "ethereum-classic";
  if (c === "DASH") return "dash";
  if (c === "SC") return "sia";
  return c.toLowerCase();
}

/* ===== fetch util (timeout + retry/backoff) ===== */
async function fetchWithTimeout(url, opts = {}, timeoutMs = 15000) {
  const controller = new AbortController();
  const to = setTimeout(() => controller.abort(), timeoutMs);
  try { return await fetch(url, { ...opts, signal: controller.signal }); }
  finally { clearTimeout(to); }
}
async function tryJSON(url, opts, timeoutMs, expectOK = true) {
  const resp = await fetchWithTimeout(url, opts, timeoutMs);
  const text = await resp.text().catch(() => "");
  let data = null; try { data = text ? JSON.parse(text) : null; } catch {}
  if (expectOK && !resp.ok) { const err = new Error(`HTTP ${resp.status} ${resp.statusText} – ${text.slice(0,240)}`); err.httpStatus = resp.status; throw err; }
  return { resp, data };
}
async function backoff(fn, tries = 3, base = 700) {
  let lastErr = null;
  for (let i = 0; i < tries; i++) {
    try { return await fn(); } catch (e) { lastErr = e; await new Promise(r => setTimeout(r, base*(i+1) + Math.random()*200)); }
  }
  throw lastErr;
}

/* ===== NORMALIZADOR — devolve { name, hr, statusCode, lastMs } ===== */
function normalizeV2Workers(list) {
  const out = [];
  for (const it of list || []) {
    const hri = it?.hash_rate_info || it?.hashrate_info || it?.hashRateInfo || {};
    const name = clean(hri?.name ?? it?.name ?? it?.worker ?? "");
    const hr = Number(hri?.hash_rate ?? it?.hash_rate ?? it?.hashrate ?? 0);
    const lastRaw = Number(it?.last_share_at ?? it?.last_share ?? it?.last_share_time ?? 0);
    const lastMs = Number.isFinite(lastRaw) ? (lastRaw > 1e11 ? lastRaw : lastRaw * 1000) : 0;
    const statusCode = Number.isFinite(Number(it?.status)) ? Number(it.status) : NaN; // 0=online, 1=offline
    if (name) out.push({ name, hr, statusCode, lastMs });
  }
  return out;
}

/* ===== F2Pool v2 (token em miners.api_key) ===== */
async function f2poolV2Workers(account, coin, token) {
  if (!token) return { ok: false, status: 0, workers: [], endpoint: null, reason: "no_token" };
  const url = "https://api.f2pool.com/v2/hash_rate/worker/list";
  const headers = { "Content-Type": "application/json", "Accept": "application/json", "F2P-API-SECRET": token };
  const body = JSON.stringify({ currency: f2slug(coin), mining_user_name: account, page: 1, size: 200 });

  const { data, resp } = await backoff(() => tryJSON(url, { method: "POST", headers, body }, 15000, true), 2, 800);

  // 200 com erro lógico → falha (não marcar offline)
  if (data && typeof data.code === "number" && data.code !== 0) {
    return { ok: false, status: resp.status, workers: [], endpoint: url, reason: `v2_code_${data.code}`, msg: data.msg || "logical error" };
  }

  const arr = Array.isArray(data?.workers) ? data.workers
            : Array.isArray(data?.data?.workers) ? data.data.workers
            : Array.isArray(data?.data?.list) ? data.data.list
            : Array.isArray(data?.list) ? data.list
            : [];

  const workers = normalizeV2Workers(arr);
  if (!workers.length) return { ok: false, status: resp.status, workers: [], endpoint: url, reason: "v2_empty" };
  return { ok: true, status: resp.status, workers, endpoint: url };
}

/* ===== dedupe +0.25 por slot ===== */
let lastSlot = null;
const updatedInSlot = new Set();
function beginSlot(s) { if (s !== lastSlot) { lastSlot = s; updatedInSlot.clear(); } }
function dedupeForHours(ids) { const out = []; for (const id of ids) if (!updatedInSlot.has(id)) { updatedInSlot.add(id); out.push(id); } return out; }

/* ===== Bloqueio manutenção (status DB) ===== */
const IS_NOT_MAINT = sql`AND lower(COALESCE(status, '')) <> 'maintenance'`;

/* ===== Job ===== */
export async function runUptimeF2PoolOnce() {
  const t0 = Date.now(); const sISO = slotISO(); beginSlot(sISO);

  // lock por slot
  const lockKey = `uptime:${sISO}:f2pool`;
  const gotLock = await redis.set(lockKey, "1", { nx: true, ex: 14 * 60 });
  if (!gotLock) { console.log(`[uptime:f2pool] lock ativo (${sISO}) – ignorado.`); return { ok: true, skipped: true }; }

  let hoursUpdated = 0, statusToOnline = 0, statusToOffline = 0, groups = 0;

  try {
    const minersRaw = await sql/*sql*/`
      SELECT id, worker_name, coin, api_key
      FROM miners
      WHERE pool = 'F2Pool'
        AND worker_name IS NOT NULL
    `;
    if (!minersRaw.length) {
      console.log(`[uptime:f2pool] ${sISO} groups=0 miners=0 api=0 online(+hrs)=0 statusOn=0 statusOff=0 dur=${Date.now()-t0}ms`);
      return { ok: true, updated: 0, statusChanged: 0 };
    }

    const miners = minersRaw
      .map(r => { const { account, worker } = splitAccountWorker(r); return { ...r, account, worker, token: clean(r.api_key || "") }; })
      .filter(m => m.account && m.worker);

    // agrupa por (account, coin, token)
    const groupMap = new Map();
    for (const m of miners) { const key = `${m.account}|${m.coin ?? ""}|${m.token}`; if (!groupMap.has(key)) groupMap.set(key, []); groupMap.get(key).push(m); }
    groups = groupMap.size;

    for (const [k, list] of groupMap.entries()) {
      const [account, coin, token] = k.split("|");
      try {
        // sufixo normalizado -> [ids]
        const suffixToIds = new Map(); const allIds = [];
        for (const m of list) {
          allIds.push(m.id);
          const sfxNorm = workerKey(m.worker);
          if (!sfxNorm) continue;
          if (!suffixToIds.has(sfxNorm)) suffixToIds.set(sfxNorm, []);
          suffixToIds.get(sfxNorm).push(m.id);
        }

        console.log("[uptime:f2pool] GROUP START", {
          account, coin, miners: list.length, wantWorkers: Array.from(suffixToIds.keys()),
          auth: token ? "v2-token" : "no-token"
        });

        const { ok, status, workers, endpoint, reason, msg } = await f2poolV2Workers(account, coin, token || undefined);
        if (!ok) {
          console.warn("[uptime:f2pool] GROUP SKIPPED", { account, coin, httpStatus: status || 0, endpoint, error: reason || msg || "unknown" });
          continue; // NUNCA marcar offline sem dados de workers
        }

        // snapshot (até 5) para ver match, HR e status
        const snap = workers.slice(0, 5).map(w => {
          const k2 = workerKey(tail(w.name) || w.name);
          return `${w.name} -> key:${k2} hr:${w.hr} status:${w.statusCode}`;
        });
        console.log("[uptime:f2pool] API workers snapshot:", snap);

        // classificar: online visível (hr>0 OU status==0), horas só hr>0, offline se (hr==0 E status==1)
        const onlineIdsRaw = [];
        const onlineIdsForHoursRaw = [];
        const offlineIdsRaw = [];

        for (const w of workers) {
          const sufNorm = workerKey(tail(w.name) || w.name);
          if (!suffixToIds.has(sufNorm)) continue;

          const hrOnline = Number(w.hr) > 0;
          const statusOnline = Number(w.statusCode) === 0;     // 0 = online
          const definitelyOffline = Number(w.statusCode) === 1 && !hrOnline;

          if (hrOnline || statusOnline) onlineIdsRaw.push(...(suffixToIds.get(sufNorm) || []));
          if (hrOnline) onlineIdsForHoursRaw.push(...(suffixToIds.get(sufNorm) || []));
          if (definitelyOffline) offlineIdsRaw.push(...(suffixToIds.get(sufNorm) || []));
        }

        // 1) horas online (dedupe slot) — NÃO contar se em manutenção
        const onlineIdsForHours = dedupeForHours(onlineIdsForHoursRaw);
        if (onlineIdsForHours.length) {
          const r = await sql/*sql*/`
            UPDATE miners
            SET total_horas_online = COALESCE(total_horas_online, 0) + 0.25
            WHERE id = ANY(${onlineIdsForHours})
              ${IS_NOT_MAINT}
            RETURNING id
          `;
          hoursUpdated += (Array.isArray(r) ? r.length : (r?.count || 0));
        }

        // 2) status (IGNORAR manutenção)
        if (onlineIdsRaw.length) {
          const r1 = await sql/*sql*/`
            UPDATE miners
            SET status = 'online'
            WHERE id = ANY(${onlineIdsRaw})
              AND status IS DISTINCT FROM 'online'
              ${IS_NOT_MAINT}
            RETURNING id
          `;
          statusToOnline += (Array.isArray(r1) ? r1.length : (r1?.count || 0));
        }
        if (offlineIdsRaw.length) {
          const r2 = await sql/*sql*/`
            UPDATE miners
            SET status = 'offline'
            WHERE id = ANY(${offlineIdsRaw})
              AND status IS DISTINCT FROM 'offline'
              ${IS_NOT_MAINT}
            RETURNING id
          `;
          statusToOffline += (Array.isArray(r2) ? r2.length : (r2?.count || 0));
        }

        console.log("[uptime:f2pool] GROUP RESULT", {
          account, coin, endpoint,
          apiWorkers: workers.length,
          onlineAPI: onlineIdsRaw.length,
          offlineAPI: offlineIdsRaw.length,
          inc: onlineIdsForHours.length
        });
      } catch (e) {
        const code = e?.code || e?.errno || e?.type || "unknown";
        console.error("[uptime:f2pool] GROUP ERROR", { account, coin, code, err: String(e?.message || e) });
      }
    }

    console.log(`[uptime:f2pool] ${sISO} groups=${groups} online(+hrs)=${hoursUpdated} statusOn=${statusToOnline} statusOff=${statusToOffline} dur=${Date.now()-t0}ms`);
    return { ok: true, updated: hoursUpdated, statusChanged: statusToOnline + statusToOffline };
  } catch (e) {
    console.error("⛔ uptime:f2pool", e);
    return { ok: false, error: String(e?.message || e) };
  }
}

export function startUptimeF2Pool() {
  cron.schedule(
    "*/15 * * * *",
    async () => { try { await runUptimeF2PoolOnce(); } catch (e) { console.error("⛔ f2pool cron:", e); } },
    { timezone: "Europe/Lisbon" }
  );
  console.log("[jobs] F2Pool (*/15) agendado.");
}
