// src/status-service.js
import crypto from "crypto";
import fetch from "node-fetch";
import dns from "dns";
import { sql } from "./config/db.js";

/* ========= DNS / IPv4-first (melhor para muitas VPS) ========= */
if (typeof dns.setDefaultResultOrder === "function") {
  // força IPv4 primeiro, evita ficar pendurado em IPv6 marado
  dns.setDefaultResultOrder("ipv4first");
}

/* ========= logger ========= */
const TAG = "[status-service]";

function log(...args) {
  console.log(TAG, ...args);
}

function logWarn(...args) {
  console.warn(TAG, ...args);
}

function logError(...args) {
  console.error(TAG, ...args);
}

/* ========= cache ========= */
const statusCache = new Map(); // key: minerId -> { data, ts }
const CACHE_TTL_MS = 30 * 1000;

/* ========= helpers básicos ========= */
const toLower = (s) => String(s ?? "").toLowerCase();

function clean(s) {
  return String(s ?? "")
    .normalize("NFKC")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim();
}

function tail(name) {
  const s = String(name ?? "").trim().toLowerCase();
  if (!s) return "";
  const i = s.lastIndexOf(".");
  return i >= 0 ? s.slice(i + 1) : s;
}

function tailKey(name) {
  const t = tail(name);
  const k = t.replace(/^0+/, "");
  return k === "" ? "0" : k;
}

function normalizeStatus(v) {
  const s = String(v ?? "").trim().toLowerCase();
  const NEG = new Set([
    "unactive",
    "inactive",
    "offline",
    "down",
    "dead",
    "parado",
    "desligado",
    "inativa",
  ]);
  if (NEG.has(s)) return "offline";
  const POS = new Set([
    "active",
    "online",
    "alive",
    "running",
    "up",
    "ok",
    "ativo",
    "ligado",
    "ativa",
  ]);
  if (POS.has(s)) return "online";
  return "offline";
}

function splitAccountWorker(name) {
  const wn = clean(name);
  const i = wn.indexOf(".");
  if (i <= 0) return { account: "", worker: "" };
  return { account: wn.slice(0, i), worker: wn.slice(i + 1) };
}

function mapAlgo(coin) {
  const c = String(coin ?? "").trim().toUpperCase();
  if (c === "BTC") return "sha256";
  if (c === "LTC") return "scrypt";
  if (c === "KAS" || c === "KASPA") return "kHeavyHash";
  return "";
}

function f2slug(coin) {
  const c = String(coin ?? "").trim().toUpperCase();
  if (c === "BTC" || c === "BITCOIN") return "bitcoin";
  if (c === "BCH") return "bitcoin-cash";
  if (c === "BSV") return "bitcoin-sv";
  if (c === "LTC" || c === "LITECOIN") return "litecoin";
  if (c === "KAS" || c === "KASPA") return "kaspa";
  if (c === "CFX") return "conflux";
  if (c === "ETC") return "ethereum-classic";
  if (c === "DASH") return "dash";
  if (c === "SC" || c === "SIA") return "sia";
  return c.toLowerCase();
}

/* ===== Mining-Dutch utils ===== */
function mdSlug(coin) {
  const c = String(coin ?? "").trim().toUpperCase();
  if (c === "BTC") return "bitcoin";
  if (c === "LTC") return "litecoin";
  if (c === "DOGE") return "dogecoin";
  return "";
}

function mdAlgo(coin) {
  const c = String(coin ?? "").trim().toUpperCase();
  if (c === "BTC") return "sha256";
  if (c === "LTC" || c === "DOGE") return "scrypt";
  return "";
}

function buildMDUrls({ coin, account_id, api_key }) {
  const base = "https://www.mining-dutch.nl";
  const algo = mdAlgo(coin);
  const slug = mdSlug(coin);
  const mk = (name) =>
    `${base}/pools/${name}.php?page=api&action=getuserworkers&id=${encodeURIComponent(
      account_id
    )}&api_key=${encodeURIComponent(api_key)}`;

  const urls = [];
  if (algo) urls.push(mk(algo));
  if (slug) urls.push(mk(slug));
  if (algo === "sha256") urls.push(mk("scrypt"));
  if (algo === "scrypt") urls.push(mk("sha256"));
  if (!algo && !slug) {
    urls.push(mk("sha256"));
    urls.push(mk("scrypt"));
  }
  return urls;
}

function parseMDWorkersPayload(data) {
  if (!data || typeof data !== "object") return [];
  const top = data.getuserworkers;

  const mapObjToArr = (miners) => {
    if (Array.isArray(miners)) {
      return miners.map((v, i) => ({
        username: clean(v?.username ?? v?.worker ?? v?.name ?? String(i)),
        alive: Number(v?.alive ?? 0),
        hashrate: Number(v?.hashrate ?? v?.hash ?? 0),
        status: clean(v?.status ?? ""),
      }));
    }
    if (miners && typeof miners === "object") {
      return Object.entries(miners).map(([k, v]) => ({
        username: clean(v?.username ?? v?.worker ?? v?.name ?? k),
        alive: Number(v?.alive ?? 0),
        hashrate: Number(v?.hashrate ?? v?.hash ?? 0),
        status: clean(v?.status ?? ""),
      }));
    }
    return [];
  };

  if (top && top.data)
    return mapObjToArr(top.data.miners ?? top.data.workers ?? []);
  const node = data?.data?.workers ?? data?.workers ?? data?.data ?? null;
  if (node) return mapObjToArr(node);
  return [];
}

/* ========= HTTP util ========= */

const DEFAULT_TIMEOUT_MS = 20_000;
const SLOW_LOG_MS = 5_000;

async function fetchJSON(url, opts = {}, retries = 1) {
  let attempt = 0;
  const timeoutMs =
    typeof opts.timeout === "number" ? opts.timeout : DEFAULT_TIMEOUT_MS;

  while (true) {
    attempt++;
    const controller = new AbortController();
    const start = Date.now();
    const to = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, { ...opts, signal: controller.signal });
      clearTimeout(to);

      const ms = Date.now() - start;

      let text = "";
      try {
        text = await res.text();
      } catch {
        // ignore
      }

      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        // ignore
      }

      if (ms > SLOW_LOG_MS) {
        logWarn("[fetchJSON] slow", {
          url,
          status: res.status,
          ms,
          timeoutMs,
        });
      }

      return { res, json, raw: text };
    } catch (e) {
      clearTimeout(to);
      const ms = Date.now() - start;
      const isAbort = e && e.name === "AbortError";

      if (isAbort) {
        if (attempt > retries) {
          logWarn("[fetchJSON] timeout", { url, timeoutMs, ms, attempt });
          return { res: null, json: null, raw: null };
        }
      } else {
        if (attempt > retries) {
          logError("[fetchJSON] fetch failed", {
            url,
            ms,
            message: e?.message || String(e),
          });
          return { res: null, json: null, raw: null };
        }
      }

      await new Promise((r) =>
        setTimeout(r, 300 * attempt + Math.random() * 300)
      );
    }
  }
}

/* ========= Binance ========= */
const BINANCE_BASES = [
  process.env.BINANCE_BASE || "https://api.binance.com",
  "https://api1.binance.com",
  "https://api2.binance.com",
  "https://api3.binance.com",
];

function signQuery(secret, params) {
  const qs = new URLSearchParams(params).toString();
  const sig = crypto.createHmac("sha256", secret).update(qs).digest("hex");
  return `${qs}&signature=${sig}`;
}

async function pickBinanceBase() {
  for (const base of BINANCE_BASES) {
    const ping = await fetchJSON(
      `${base}/api/v3/exchangeInfo`,
      { timeout: 20_000 },
      1
    );
    if (ping.res?.ok) return base;
    if (ping.res && ping.res.status === 451) continue;
  }
  return null;
}

async function getServerTime(base) {
  const r = await fetchJSON(`${base}/api/v3/time`, { timeout: 20_000 }, 1);
  if (!r.res?.ok) return null;
  const t = Number(r?.json?.serverTime);
  return Number.isFinite(t) ? t : null;
}

async function signedGET({ base, path, apiKey, secretKey, params, skewMs = 0 }) {
  const headers = { "X-MBX-APIKEY": apiKey };
  const p = { ...params, timestamp: Date.now() + skewMs, recvWindow: 30_000 };
  const url = `${base}${path}?` + signQuery(secretKey, p);
  return fetchJSON(url, { headers, timeout: 20_000 }, 1);
}

/* ========= CORE: fetch status de UM miner, com rec já carregado opcional ========= */
async function fetchMinerStatusNormalized(minerId, preloaded) {
  const idStr = String(minerId);
  let rec = preloaded;

  // Carrega da BD se não foi injetado
  if (!rec) {
    try {
      const rows = await sql`
        SELECT id, api_key, secret_key, coin, pool, worker_name, status
        FROM miners
        WHERE id::text = ${idStr}
        LIMIT 1
      `;
      if (!rows.length) {
        return { id: idStr, error: "not_found" };
      }
      rec = rows[0];
    } catch (e) {
      logError("[fetchMinerStatusNormalized] db error", {
        id: idStr,
        code: e?.code,
        message: e?.message,
      });

      const errCode =
        e && (e.code === "CONNECT_TIMEOUT" || e.errno === "CONNECT_TIMEOUT")
          ? "db_connect_timeout"
          : "db_error";

      return {
        id: idStr,
        worker_status: "offline",
        hashrate_10min: 0,
        power: null,
        watts: null,
        source: null,
        worker_found: false,
        error: errCode,
      };
    }
  }

  // não consulta pool se está em manutenção
  const dbStatus = String(rec.status ?? "").trim().toLowerCase();
  if (dbStatus === "maintenance") {
    return {
      id: idStr,
      worker_status: "maintenance",
      hashrate_10min: 0,
      power: null,
      watts: null,
      source: null,
      worker_found: false,
    };
  }

  const api_key = rec.api_key;
  const secret_key = rec.secret_key;
  const coin = rec.coin;
  const poolRaw = String(rec.pool ?? "").trim();
  const poolLower = poolRaw.toLowerCase();

  const worker_name_db = rec.worker_name ?? "";
  const expectedTail = tail(worker_name_db);
  const expectedKey = tailKey(worker_name_db);
  if (!expectedTail) {
    return { id: idStr, error: "no_worker_name" };
  }

  let workers = [];
  let source = null;

  /* ====== POOLS ====== */

  // ViaBTC
  if (poolLower === "viabtc") {
    source = "ViaBTC";
    if (!api_key || !coin) {
      return { id: idStr, error: "viabtc_missing_config" };
    }

    const url = `https://www.viabtc.net/res/openapi/v1/hashrate/worker?coin=${encodeURIComponent(
      coin
    )}`;
    const { res: r, json: data } = await fetchJSON(
      url,
      {
        headers: { "X-API-KEY": api_key },
        timeout: 20_000, // VPS pode ter rota lenta para China
      },
      1
    );
    if (!r || !r.ok || !data || data.code !== 0) {
      logWarn("[ViaBTC] bad response", {
        id: idStr,
        status: r?.status,
        body: data,
      });
      return { id: idStr, error: "viabtc_error" };
    }
    const list = Array.isArray(data?.data?.data) ? data.data.data : [];
    workers = list.map((w) => ({
      worker_name: String(w.worker_name ?? "").trim(),
      worker_status: w.worker_status,
      hashrate_10min: Number(w.hashrate_10min ?? 0),
    }));
  }

  // LitecoinPool
  else if (poolLower === "litecoinpool") {
    source = "LiteCoinPool";
    if (!api_key) {
      return { id: idStr, error: "ltcp_missing_api_key" };
    }

    const url = `https://www.litecoinpool.org/api?api_key=${encodeURIComponent(
      api_key
    )}`;
    const { res: r, json: data } = await fetchJSON(
      url,
      { timeout: 15_000 },
      1
    );
    if (!r || !r.ok || !data || !data.workers) {
      logWarn("[LiteCoinPool] bad response", {
        id: idStr,
        status: r?.status,
        bodyKeys: data && Object.keys(data),
      });
      return { id: idStr, error: "ltcp_error" };
    }
    workers = Object.entries(data.workers).map(([name, info]) => ({
      worker_name: String(name ?? "").trim(),
      worker_status: info.connected ? "active" : "unactive",
      hashrate_10min: Number((info.hash_rate ?? 0) * 1000),
    }));
  }

  // Binance Pool
  else if (poolLower === "binance") {
    source = "Binance";
    if (!api_key || !secret_key || !coin) {
      return { id: idStr, error: "binance_missing_config" };
    }

    const { account } = splitAccountWorker(worker_name_db);
    if (!account) return { id: idStr, error: "binance_bad_worker" };
    const algo = mapAlgo(coin);
    if (!algo) return { id: idStr, error: "binance_bad_coin" };
    const base = await pickBinanceBase();
    if (!base) return { id: idStr, error: "binance_unavailable" };

    let L = await signedGET({
      base,
      path: "/sapi/v1/mining/worker/list",
      apiKey: api_key,
      secretKey: secret_key,
      params: { algo, userName: account, pageIndex: 1, pageSize: 200, sort: 0 },
    });

    if (L.res && !L.res.ok && L.json?.code === -1021) {
      const serverTime = await getServerTime(base);
      if (serverTime) {
        const skewMs = serverTime - Date.now();
        L = await signedGET({
          base,
          path: "/sapi/v1/mining/worker/list",
          apiKey: api_key,
          secretKey: secret_key,
          params: {
            algo,
            userName: account,
            pageIndex: 1,
            pageSize: 200,
            sort: 0,
          },
          skewMs,
        });
      }
    }

    if (!L.res || !L.res.ok) {
      logWarn("[Binance] list error", {
        id: idStr,
        status: L.res?.status,
        body: L.json,
      });
      return { id: idStr, error: "binance_error" };
    }

    const listArr = Array.isArray(L.json?.data?.workerDatas)
      ? L.json.data.workerDatas
      : [];

    workers = listArr.map((w) => ({
      worker_name: String(w?.workerName ?? "").trim(),
      worker_status: Number(w?.status ?? 0) === 1 ? "active" : "unactive",
      hashrate_10min: Number(w?.hashRate ?? 0),
    }));

    // fallback detail se não encontrar na list
    const seen = workers.some(
      (w) =>
        tail(w.worker_name) === expectedTail ||
        tailKey(w.worker_name) === expectedKey
    );

    if (!seen) {
      let D = await signedGET({
        base,
        path: "/sapi/v1/mining/worker/detail",
        apiKey: api_key,
        secretKey: secret_key,
        params: { algo, userName: account, workerName: expectedTail },
      });

      if (D.res && !D.res.ok && D.json?.code === -1021) {
        const serverTime = await getServerTime(base);
        if (serverTime) {
          const skewMs = serverTime - Date.now();
          D = await signedGET({
            base,
            path: "/sapi/v1/mining/worker/detail",
            apiKey: api_key,
            secretKey: secret_key,
            params: {
              algo,
              userName: account,
              workerName: expectedTail,
            },
            skewMs,
          });
        }
      }

      if (D.res && D.res.ok && D.json?.data) {
        const d = D.json.data;
        workers.push({
          worker_name: String(d?.workerName ?? expectedTail),
          worker_status:
            Number(d?.status ?? 0) === 1 ? "active" : "unactive",
          hashrate_10min: Number(d?.hashRate ?? 0),
        });
      }
    }
  }

  // F2Pool
  else if (poolLower === "f2pool") {
    source = "F2Pool";
    const { account } = splitAccountWorker(worker_name_db);
    if (!account) return { id: idStr, error: "f2_bad_worker" };

    const currency = f2slug(coin || "BTC");
    const url = "https://api.f2pool.com/v2/hash_rate/worker/list";
    const headers = {
      "Content-Type": "application/json",
      "F2P-API-SECRET": api_key,
    };
    const body = JSON.stringify({
      currency,
      mining_user_name: account,
      page: 1,
      size: 200,
    });

    const { res: r, json: data } = await fetchJSON(
      url,
      { method: "POST", headers, body, timeout: 15_000 },
      1
    );

    if (
      !r ||
      !r.ok ||
      (data && typeof data.code === "number" && data.code !== 0)
    ) {
      logWarn("[F2Pool] bad response", {
        id: idStr,
        status: r?.status,
        body: data,
      });
      return { id: idStr, error: "f2_error" };
    }

    const arr = Array.isArray(data?.workers)
      ? data.workers
      : Array.isArray(data?.data?.workers)
      ? data.data.workers
      : Array.isArray(data?.data?.list)
      ? data.data.list
      : Array.isArray(data?.list)
      ? data.list
      : [];

    workers = arr.map((item) => {
      const hri =
        item?.hash_rate_info ||
        item?.hashrate_info ||
        item?.hashRateInfo ||
        {};
      const name = clean(hri?.name ?? item?.name ?? item?.worker ?? "");
      const hr = Number(
        hri?.hash_rate ?? item?.hash_rate ?? item?.hashrate ?? 0
      );
      const last =
        Number(
          item?.last_share_at ??
            item?.last_share ??
            item?.last_share_time ??
            0
        );
      const lastMs = Number.isFinite(last)
        ? last > 1e11
          ? last
          : last * 1000
        : 0;
      const fresh = lastMs > 0 && Date.now() - lastMs < 90 * 60 * 1000;
      const online = hr > 0 || fresh;
      return {
        worker_name: name,
        worker_status: online ? "active" : "unactive",
        hashrate_10min: hr,
      };
    });
  }

  // MiningDutch
  else if (poolLower === "miningdutch") {
    source = "MiningDutch";
    const { account } = splitAccountWorker(worker_name_db);
    if (!account) return { id: idStr, error: "md_bad_worker" };
    if (!api_key) return { id: idStr, error: "md_missing_api_key" };

    const urls = buildMDUrls({
      coin,
      account_id: account,
      api_key,
    });

    let parsed = null;
    for (const url of urls) {
      const { res: r, json } = await fetchJSON(
        url,
        { timeout: 22_000 },
        1
      );
      if (!r || !r.ok) continue;
      const list = parseMDWorkersPayload(json);
      if (Array.isArray(list)) {
        parsed = list;
        break;
      }
    }

    if (!parsed) return { id: idStr, error: "md_error" };

    workers = parsed.map((w, i) => ({
      worker_name: String(w?.username ?? `w${i}`),
      worker_status:
        Number(w?.alive ?? 0) > 0 || Number(w?.hashrate ?? 0) > 0
          ? "active"
          : "unactive",
      hashrate_10min: Number(w?.hashrate ?? 0),
    }));
  }

  // Pool desconhecida
  else {
    return { id: idStr, error: "unsupported_pool" };
  }

  // pick worker por tail/key
  const expectedTailStr = tail(rec.worker_name);
  const expectedKeyStr = tailKey(rec.worker_name);
  const my =
    workers.find((w) => tail(w.worker_name) === expectedTailStr) ||
    workers.find((w) => tailKey(w.worker_name) === expectedKeyStr) ||
    null;

  const worker_status = my ? normalizeStatus(my.worker_status) : "offline";
  const hashrate_10min = my ? Number(my.hashrate_10min ?? 0) : 0;

  return {
    id: idStr,
    worker_status,
    hashrate_10min,
    power: null,
    watts: null,
    source,
    worker_found: !!my,
  };
}

/* ========= Single endpoint ========= */
export async function getMinerStatus(req, res) {
  const id = String(req.params.id ?? req.params.minerId ?? "").trim();
  if (!id) return res.status(400).json({ error: "invalid_id" });

  const wantRefresh =
    String(req.query.refresh ?? "") === "1" ||
    String(req.headers["x-refresh"] ?? "") === "1";

  const cache = statusCache.get(id);
  if (!wantRefresh && cache && Date.now() - cache.ts < CACHE_TTL_MS) {
    return res.json(cache.data);
  }

  try {
    const data = await fetchMinerStatusNormalized(id);
    statusCache.set(id, { data, ts: Date.now() });
    res.json(data);
  } catch (e) {
    logError("getMinerStatus error", {
      id,
      message: e?.message,
      stack: e?.stack,
    });
    res.status(500).json({ error: "internal_error" });
  }
}

/* ========= Batch endpoint (OTIMIZADO PARA VPS) ========= */

const STATUS_CONCURRENCY =
  Number(process.env.STATUS_CONCURRENCY || "3") || 3;

export async function getMinersStatusMany(req, res) {
  const raw = String(req.query.ids ?? "").trim();
  if (!raw) return res.status(400).json({ error: "ids_vazios" });

  const ids = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const now = Date.now();
  const out = [];
  const toFetch = [];

  // 1) tenta cache primeiro
  for (const id of ids) {
    const c = statusCache.get(id);
    if (c && now - c.ts < CACHE_TTL_MS) {
      out.push(c.data);
    } else {
      toFetch.push(id);
    }
  }

  // Se tudo veio de cache, respondemos já
  if (!toFetch.length) {
    const map = new Map(out.map((o) => [o.id, o]));
    const ordered = ids.map(
      (id) =>
        map.get(id) || {
          id,
          worker_status: "offline",
          hashrate_10min: 0,
          power: null,
          watts: null,
          worker_found: false,
          error: "no_cached_data",
        }
    );
    return res.json(ordered);
  }

  // helper para fallback quando não conseguimos ir ao DB
  function buildFallbackMany(errorCode) {
    const mapCached = new Map(out.map((o) => [o.id, o]));
    return ids.map((id) => {
      const cached = mapCached.get(id);
      if (cached) return cached;
      return {
        id,
        worker_status: "offline",
        hashrate_10min: 0,
        power: null,
        watts: null,
        worker_found: false,
        error: errorCode,
      };
    });
  }

  try {
    // 2) carregar recs das miners a partir da Neon
    let rows;
    try {
      rows = await sql`
        SELECT id, api_key, secret_key, coin, pool, worker_name, status
        FROM miners
        WHERE id::text = ANY(${toFetch})
      `;
    } catch (e) {
      logError("[getMinersStatusMany] db error", {
        ids: toFetch,
        code: e?.code,
        message: e?.message,
      });

      const errCode =
        e && (e.code === "CONNECT_TIMEOUT" || e.errno === "CONNECT_TIMEOUT")
          ? "db_connect_timeout"
          : "db_error";

      const ordered = buildFallbackMany(errCode);
      return res.json(ordered);
    }

    const mapMiner = new Map(rows.map((r) => [String(r.id), r]));

    // 3) concorrência limitada (ajustável por env para VPS)
    const CONC = STATUS_CONCURRENCY;
    const queue = [...toFetch];
    const outBatch = [];

    const workersPromises = [];
    for (let i = 0; i < CONC; i++) {
      workersPromises.push(
        (async () => {
          while (queue.length) {
            const id = queue.shift();
            if (!id) break;
            try {
              const rec = mapMiner.get(String(id)) || null;
              const data = await fetchMinerStatusNormalized(id, rec);
              statusCache.set(String(id), { data, ts: Date.now() });
              outBatch.push(data);
            } catch (e) {
              logError("[getMinersStatusMany] worker error", {
                id,
                message: e?.message,
              });
              outBatch.push({
                id: String(id),
                worker_status: "offline",
                hashrate_10min: 0,
                power: null,
                watts: null,
                worker_found: false,
                error: "internal_error",
              });
            }
          }
        })()
      );
    }

    await Promise.all(workersPromises);

    const mapOut = new Map(
      [...out, ...outBatch].map((o) => [o.id, o])
    );

    const ordered = ids.map(
      (id) =>
        mapOut.get(id) || {
          id,
          worker_status: "offline",
          hashrate_10min: 0,
          power: null,
          watts: null,
          worker_found: false,
          error: "no_data",
        }
    );

    return res.json(ordered);
  } catch (e) {
    logError("getMinersStatusMany fatal error", {
      message: e?.message,
      stack: e?.stack,
    });
    const ordered = buildFallbackMany("internal_error");
    return res.json(ordered);
  }
}
