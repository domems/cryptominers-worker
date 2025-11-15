// src/jobs/uptimeBinance.js
import crypto from "crypto";
import cron from "node-cron";
import fetch from "node-fetch";
import { sql } from "../config/db.js";
import { redis } from "../config/upstash.js";

/* ===== Bases possíveis (rota automaticamente) ===== */
const CANDIDATE_BASES = [
  process.env.BINANCE_BASE,
  "https://api.binance.com",
  "https://api1.binance.com",
  "https://api2.binance.com",
  "https://api3.binance.com",
].filter(Boolean);

/* ===== slot 15 min (UTC) ===== */
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
const clean = (s) =>
  String(s ?? "").normalize("NFKC").replace(/[\u200B-\u200D\uFEFF]/g, "").trim();

const mask = (s, keep = 6) => {
  const t = String(s ?? "");
  if (!t) return "";
  return t.length <= keep + 2
    ? `${t.slice(0, 2)}***`
    : t.slice(0, keep) + "***" + t.slice(-2);
};

function mapAlgo(coin) {
  const c = String(coin ?? "").trim().toUpperCase();
  if (c === "BTC") return "sha256";
  if (c === "LTC") return "scrypt";
  if (c === "KAS" || c === "KASPA") return "kHeavyHash";
  return "";
}

/** extrai account & sufixo de "account.worker" */
function splitAccountWorker(row) {
  const wn = clean(row.worker_name);
  const i = wn.indexOf(".");
  if (i <= 0) return { account: "", worker: "" };
  return { account: wn.slice(0, i), worker: wn.slice(i + 1) };
}

/** sufixo depois do último "." (mantém zeros à esquerda) */
function tail(s) {
  const str = clean(s);
  const i = str.lastIndexOf(".");
  return i >= 0 ? str.slice(i + 1) : str;
}

/** chave normalizada do sufixo: lowercase + sem zeros à esquerda (001 ≡ 1; preserva "0") */
const workerKey = (w) => {
  const s = clean(w).toLowerCase();
  const k = s.replace(/^0+/, "");
  return k === "" ? "0" : k;
};

/* ===== assinatura e fetch ===== */
function signQuery(secret, params) {
  const qs = new URLSearchParams(params).toString();
  const sig = crypto.createHmac("sha256", secret).update(qs).digest("hex");
  return `${qs}&signature=${sig}`;
}

async function fetchWithRetry(url, opts = {}, retries = 2) {
  let attempt = 0;
  while (true) {
    attempt++;
    let resp;
    try {
      const controller = new AbortController();
      const to = setTimeout(
        () => controller.abort(),
        opts.timeout ?? 20_000
      );
      resp = await fetch(url, { ...opts, signal: controller.signal });
      clearTimeout(to);
    } catch (e) {
      if (attempt > retries) throw e;
      await new Promise((r) =>
        setTimeout(r, 300 * attempt + Math.random() * 300)
      );
      continue;
    }
    if (
      [451, 403, 401, 429].includes(resp.status) ||
      resp.status >= 500
    ) {
      if (attempt > retries) return resp;
      const ra =
        Number(resp.headers.get("retry-after")) || 350 * attempt;
      await new Promise((r) =>
        setTimeout(r, ra + Math.random() * 300)
      );
      continue;
    }
    return resp;
  }
}

async function pickBinanceBase() {
  for (const base of CANDIDATE_BASES) {
    try {
      const r = await fetchWithRetry(
        `${base}/api/v3/exchangeInfo`,
        { timeout: 7000 },
        1
      );
      if (r.ok) return { base, status: r.status };
      if (r.status === 451) {
        console.warn("[uptime:binance] base geoblocked:", base);
        continue;
      }
    } catch {
      // ignora erro e tenta próxima base
    }
  }
  return { base: null, status: 0 };
}

/* ===== API Binance ===== */
async function binanceListWorkers({
  base,
  apiKey,
  secretKey,
  algo,
  userName,
}) {
  const headers = { "X-MBX-APIKEY": apiKey };
  const pageSize = 200;
  let page = 1;
  const all = [];

  while (true) {
    const params = {
      algo,
      userName,
      pageIndex: page,
      sort: 0,
      timestamp: Date.now(),
      recvWindow: 20_000,
    };
    const url = `${base}/sapi/v1/mining/worker/list?${signQuery(
      secretKey,
      params
    )}`;
    const resp = await fetchWithRetry(url, { headers }, 2);

    console.log("[binance:api:list]", {
      account: userName,
      algo,
      page,
      httpStatus: resp.status,
      ok: resp.ok,
      apiKey: mask(apiKey),
      base,
    });

    if (resp.status === 451)
      return {
        ok: false,
        status: 451,
        reason: "geoblocked",
        workers: [],
      };
    if (resp.status === 403 || resp.status === 401)
      return {
        ok: false,
        status: resp.status,
        reason: "auth",
        workers: [],
      };
    if (!resp.ok)
      return {
        ok: false,
        status: resp.status,
        reason: "http",
        workers: [],
      };

    const data = await resp.json().catch(() => null);
    const arr = data?.data?.workerDatas || [];
    all.push(...arr);

    const pageSizeResp = Number(data?.data?.pageSize || 0);
    if (!arr.length || arr.length < (pageSizeResp || pageSize)) break;
    page += 1;
  }

  const workers = all.map((w) => ({
    workerName: clean(w?.workerName), // pode vir "account.worker"
    status: Number(w?.status ?? 0), // 1 valid, 2 invalid, 3 no longer valid
    hashRate: Number(w?.hashRate ?? 0),
    lastShareTime: Number(w?.lastShareTime ?? 0),
  }));

  return { ok: true, status: 200, workers };
}

function isOnlineBinance(w) {
  if (Number.isFinite(w.hashRate) && w.hashRate > 0) return true;
  return Number(w.status) === 1;
}

/* ===== controle de slot (dedupe horas) ===== */
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

/* ===== Job principal ===== */
export async function runUptimeBinanceOnce() {
  const t0 = Date.now();
  const sISO = slotISO();
  beginSlot(sISO);

  const lockKey = `uptime:${sISO}:binance`;
  const gotLock = await redis.set(lockKey, "1", {
    nx: true,
    ex: 14 * 60,
  });
  if (!gotLock) {
    console.log(
      `[uptime:binance] lock ativo (${sISO}) – ignorado nesta instância.`
    );
    return { ok: true, skipped: true };
  }

  let hoursUpdated = 0;
  let statusToOnline = 0;
  let statusToOffline = 0;
  let groupsCount = 0;
  let apiCalls = 0;

  try {
    const picked = await pickBinanceBase();
    if (!picked.base) {
      console.warn(
        "[uptime:binance] todas as bases indisponíveis/geoblocked."
      );
      return { ok: true, skipped: true, reason: "geoblocked_all" };
    }
    const BASE = picked.base;
    console.log("[uptime:binance] BASE escolhida:", BASE);

    const minersRaw = await sql/*sql*/`
      SELECT id, worker_name, api_key, secret_key, coin
      FROM miners
      WHERE pool = 'Binance'
        AND api_key IS NOT NULL AND secret_key IS NOT NULL
        AND worker_name IS NOT NULL
    `;
    if (!minersRaw.length) {
      console.log(
        `[uptime:binance] ${sISO} groups=0 miners=0 api=0 online(+hrs)=0 statusOn=0 statusOff=0 dur=${
          Date.now() - t0
        }ms`
      );
      return { ok: true, updated: 0, statusChanged: 0 };
    }

    const miners = minersRaw
      .map((r) => {
        const { account, worker } = splitAccountWorker(r);
        const algo = mapAlgo(r.coin);
        return {
          ...r,
          account: clean(account),
          worker: clean(worker),
          algo,
        };
      })
      .filter((m) => m.account && m.worker && m.algo);

    const groups = new Map(); // "api|secret|account|algo" -> Miner[]
    for (const m of miners) {
      const k = `${m.api_key}|${m.secret_key}|${m.account}|${m.algo}`;
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(m);
    }
    groupsCount = groups.size;

    for (const [key, list] of groups) {
      try {
        const [apiKey, secretKey, userName, algo] = key.split("|");

        const suffixToIds = new Map();
        const allIds = [];
        for (const m of list) {
          allIds.push(m.id);
          const sfxNorm = workerKey(m.worker);
          if (!sfxNorm) continue;
          if (!suffixToIds.has(sfxNorm)) suffixToIds.set(sfxNorm, []);
          suffixToIds.get(sfxNorm).push(m.id);
        }

        console.log("[uptime:binance] GROUP START", {
          account: userName,
          algo,
          miners: list.length,
          apiKey: mask(apiKey),
          secretKey: mask(secretKey),
          wantWorkers: Array.from(suffixToIds.keys()),
          base: BASE,
        });

        const { ok, status, reason, workers } = await binanceListWorkers({
          base: BASE,
          apiKey,
          secretKey,
          algo,
          userName,
        });
        apiCalls += 1;
        if (!ok) {
          console.warn("[uptime:binance] GROUP SKIPPED", {
            account: userName,
            algo,
            reason,
            httpStatus: status,
            base: BASE,
          });
          continue;
        }

        const onlineIdsRaw = [];
        for (const w of workers) {
          const sufNorm = workerKey(tail(w.workerName) || w.workerName);
          if (!suffixToIds.has(sufNorm)) continue;
          if (isOnlineBinance(w)) {
            onlineIdsRaw.push(...(suffixToIds.get(sufNorm) || []));
          }
        }

        const onlineSet = new Set(onlineIdsRaw);
        const offlineIdsRaw = allIds.filter((id) => !onlineSet.has(id));

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

        console.log("[uptime:binance] GROUP RESULT", {
          account: userName,
          algo,
          miners: list.length,
          apiWorkers: workers.length,
          onlineAPI: onlineIdsRaw.length,
          offlineAPI: offlineIdsRaw.length,
          inc: onlineIdsForHours.length,
          statusOn: statusToOnline,
          statusOff: statusToOffline,
        });
      } catch (e) {
        console.error(
          "[uptime:binance] GROUP ERROR",
          String(e?.message || e)
        );
      }
    }

    console.log(
      `[uptime:binance] ${sISO} groups=${groupsCount} api=${apiCalls} online(+hrs)=${hoursUpdated} statusOn=${statusToOnline} statusOff=${statusToOffline} dur=${
        Date.now() - t0
      }ms`
    );
    return {
      ok: true,
      updated: hoursUpdated,
      statusChanged: statusToOnline + statusToOffline,
    };
  } catch (e) {
    console.error("⛔ uptime:binance", e);
    return { ok: false, error: String(e?.message || e) };
  }
}

export function startUptimeBinance() {
  cron.schedule(
    "*/15 * * * *",
    async () => {
      try {
        await runUptimeBinanceOnce();
      } catch (e) {
        console.error("⛔ binance cron:", e);
      }
    },
    { timezone: "Europe/Lisbon" }
  );
  console.log("[jobs] Binance (*/15) agendado.");
}
