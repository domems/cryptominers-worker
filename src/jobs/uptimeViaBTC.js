// src/jobs/uptimeViaBTC.js
import cron from "node-cron";
import fetchOrig from "node-fetch";
import { sql } from "../config/db.js";

/* =============================== */
/* Logger / Utils gerais           */
/* =============================== */
const fetch = globalThis.fetch || fetchOrig;

const TAG = "[uptime:viabtc]";
const DEBUG =
  String(process.env.DEBUG_UPTIME_VIABTC ?? "true").toLowerCase() === "true";

const dlog = (...args) => {
  if (DEBUG) {
    console.log(new Date().toISOString(), TAG, ...args);
  }
};

const mask = (s, keep = 4) => {
  const t = String(s ?? "");
  if (!t) return "";
  if (t.length <= keep + 2) return `${t.slice(0, 2)}***`;
  return t.slice(0, keep) + "***" + t.slice(-2);
};

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

// ===== helpers =====
const norm = (s) => String(s ?? "").trim();
const low = (s) => norm(s).toLowerCase();

/** usa só o sufixo depois do último "." (mantém zeros à esquerda) */
const tail = (s) => {
  const str = norm(s);
  const i = str.lastIndexOf(".");
  return i >= 0 ? str.slice(i + 1) : str;
};

function normalizeCoin(c) {
  const s = String(c ?? "").trim().toUpperCase();
  // Com a tua BD BTC/LTC isto passa direitinho
  return s === "BTC" || s === "LTC" ? s : "";
}

/** estado online sem falsos positivos (ex.: "unactive" NÃO é "active") */
const NEG_STATES = new Set(["unactive", "inactive", "offline", "down", "dead"]);
const POS_STATES = new Set(["active", "online", "alive", "running", "up", "ok"]);

function isOnlineFrom(w) {
  const hr = Number(w?.hashrate_10min ?? 0);
  if (Number.isFinite(hr) && hr > 0) return true;
  const ws = low(w?.worker_status ?? "");
  if (NEG_STATES.has(ws)) return false;
  if (POS_STATES.has(ws)) return true;
  return false;
}

/* =============================== */
/* API fetch + caches              */
/* =============================== */
const API_TTL_MS = 60_000; // cache em memória por grupo (api_key|coin)
const API_TIMEOUT_MS = 12_000;

const apiCache = new Map(); // key -> { workers, ts }
let lastSlot = null;
let slotCache = new Map(); // `${slot}|${api_key}|${coin}` -> workers
const updatedInSlot = new Set();

function beginSlot(s) {
  if (s !== lastSlot) {
    lastSlot = s;
    slotCache = new Map(); // limpa cache do slot quando muda
    updatedInSlot.clear(); // dedupe de incrementos por slot
  }
}
function dedupe(ids) {
  const out = [];
  for (const id of ids) {
    if (!updatedInSlot.has(id)) {
      updatedInSlot.add(id);
      out.push(id);
    }
  }
  return out;
}

/* ===== Public IP logging (com cache) ===== */

const PUBLIC_IP_TTL_MS = 10 * 60 * 1000; // 10 minutos
let lastPublicIpCheck = 0;
let lastPublicIp = null;

async function getPublicIp() {
  const now = Date.now();
  if (lastPublicIp && now - lastPublicIpCheck < PUBLIC_IP_TTL_MS) {
    return lastPublicIp;
  }

  try {
    const resp = await fetch("https://api.ipify.org?format=json");
    const data = await resp.json().catch(() => null);
    const ip = data?.ip || null;
    lastPublicIp = ip;
    lastPublicIpCheck = now;
    dlog("PUBLIC IP DETECTED", { ip });
    return ip;
  } catch (e) {
    console.error(TAG, "PUBLIC IP ERROR", e?.message || e);
    lastPublicIpCheck = now;
    return null;
  }
}

/* ===== backoff genérico ===== */
async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function backoff(fn, tries = 2, base = 700) {
  let lastErr = null;
  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      dlog("BACKOFF ERR", { try: i + 1, err: String(e?.message || e) });
      await sleep(base * (i + 1) + Math.random() * 200);
    }
  }
  throw lastErr;
}

/* ===== chamadas ViaBTC ===== */
async function fetchViaBTCListOnce(apiKey, coin) {
  const url = `https://www.viabtc.net/res/openapi/v1/hashrate/worker?coin=${coin}`;
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), API_TIMEOUT_MS);

  // LOG do IP público antes de chamar a ViaBTC (com cache para não spammar)
  try {
    const ip = await getPublicIp();
    if (ip) {
      dlog("OUTBOUND IP (via ipify)", { ip });
    } else {
      dlog("OUTBOUND IP (via ipify)", { ip: null });
    }
  } catch {
    // já foi logado no getPublicIp se der erro
  }

  dlog("FETCH BEGIN", {
    url,
    coin,
    apiKey: mask(apiKey),
  });

  try {
    const resp = await fetch(url, {
      headers: { "X-API-KEY": apiKey },
      signal: ac.signal,
    });

    const text = await resp.text().catch(() => "");
    dlog("FETCH RAW", {
      status: resp.status,
      ok: resp.ok,
      bodySample: text.slice(0, 400),
    });

    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch (e) {
      dlog("JSON PARSE ERROR", { err: String(e?.message || e) });
    }

    dlog("FETCH PARSED", {
      coin,
      apiKey: mask(apiKey),
      code: data?.code,
      count: Array.isArray(data?.data?.data) ? data.data.data.length : null,
    });

    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status} ${resp.statusText}`);
    }
    if (!data || data.code !== 0 || !Array.isArray(data.data?.data)) {
      throw new Error(
        `ViaBTC payload inválido/erro lógico (code=${data?.code ?? "?"})`
      );
    }

    const workers = data.data.data.map((w) => ({
      worker_name: String(w.worker_name ?? ""),
      worker_status: String(w.worker_status ?? ""),
      hashrate_10min: Number(w.hashrate_10min ?? 0),
    }));

    // pequeno snapshot dos primeiros workers
    dlog("FETCH WORKERS SNAPSHOT", {
      coin,
      apiKey: mask(apiKey),
      sample: workers.slice(0, 5),
    });

    return workers;
  } finally {
    clearTimeout(to);
  }
}

async function fetchViaBTCListWithRetries(apiKey, coin, tries = 2) {
  return backoff(() => fetchViaBTCListOnce(apiKey, coin), tries, 800);
}

/**
 * Obtém workers da ViaBTC para (apiKey, coin) com 2 camadas de cache:
 *  - cache do SLOT atual (reutiliza dentro do mesmo slot de 15 min)
 *  - cache temporário em memória com TTL (evita refetchs em execuções próximas)
 *
 * Retorna { workers, cache: "slot"|"memory"|"miss" }
 */
async function getViaBTCWorkersCached(apiKey, coin, slot) {
  const groupKey = `${apiKey}|${coin}`;
  const slotKey = `${slot}|${groupKey}`;

  // 1) cache por slot (mais forte)
  if (slotCache.has(slotKey)) {
    dlog("CACHE HIT (slot)", { groupKey: mask(groupKey) });
    return { workers: slotCache.get(slotKey), cache: "slot" };
  }

  // 2) cache com TTL
  const c = apiCache.get(groupKey);
  if (c && Date.now() - c.ts < API_TTL_MS) {
    dlog("CACHE HIT (memory)", { groupKey: mask(groupKey) });
    slotCache.set(slotKey, c.workers);
    return { workers: c.workers, cache: "memory" };
  }

  // 3) fetch real
  dlog("CACHE MISS → FETCH", { groupKey: mask(groupKey) });
  const workers = await fetchViaBTCListWithRetries(apiKey, coin, 2);
  apiCache.set(groupKey, { workers, ts: Date.now() });
  slotCache.set(slotKey, workers);
  return { workers, cache: "miss" };
}

/* =============================== */
/* Job principal                   */
/* =============================== */
export async function runUptimeViaBTCOnce() {
  const t0 = Date.now();
  const sISO = slotISO();
  beginSlot(sISO);

  let updated = 0;
  let totalMiners = 0;
  let totalGroups = 0;
  let workersRelevant = 0;
  let workersExtra = 0;
  let groupErrors = 0;
  let apiCalls = 0;

  let statusToOnline = 0;
  let statusToOffline = 0;

  const CONCURRENCY = 3;
  const inflight = new Set();
  const allTasks = [];

  try {
    const minersRaw = await sql/*sql*/`
      SELECT id, worker_name, api_key, coin, status
      FROM miners
      WHERE pool = 'ViaBTC'
        AND api_key IS NOT NULL AND api_key <> ''
        AND worker_name IS NOT NULL AND worker_name <> ''
    `;
    const miners = minersRaw
      .map((m) => ({ ...m, coin: normalizeCoin(m.coin) }))
      .filter((m) => m.coin);

    totalMiners = miners.length;

    if (!totalMiners) {
      console.log(
        `[uptime:viabtc] ${sISO} groups=0 miners=0 api=0 workers=0 extra=0 online=0 errs=0 statusOn=0 statusOff=0 dur=${
          Date.now() - t0
        }ms`
      );
      return { ok: true, updated: 0 };
    }

    const groups = new Map(); // `${api_key}|${coin}` -> Miner[]
    for (const m of miners) {
      const k = `${m.api_key}|${m.coin}`;
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(m);
    }
    totalGroups = groups.size;

    const groupEntries = Array.from(groups.entries());

    const processGroup = async ([k, list]) => {
      const [apiKey, coin] = k.split("|");

      try {
        // mapa tail -> [ids]
        const tailToIds = new Map();
        const allIds = [];
        for (const m of list) {
          allIds.push(m.id);
          const t = tail(m.worker_name);
          if (!t) continue;
          if (!tailToIds.has(t)) tailToIds.set(t, []);
          tailToIds.get(t).push(m.id);
        }
        const tailsWanted = Array.from(tailToIds.keys());

        dlog("GROUP START", {
          coin,
          apiKey: mask(apiKey),
          miners: list.length,
          tailsWanted,
        });

        // 1ª leitura (com cache & retries)
        const { workers: workers1, cache } = await getViaBTCWorkersCached(
          apiKey,
          coin,
          sISO
        );
        if (cache === "miss") apiCalls += 1;

        const workerMap1 = new Map(); // tail -> worker
        let extra1 = 0;
        for (const w of workers1 || []) {
          const tw = tail(w.worker_name);
          if (tailToIds.has(tw)) {
            workerMap1.set(tw, w);
          } else {
            extra1 += 1;
          }
        }
        workersRelevant += workerMap1.size;
        workersExtra += extra1;

        // LOG de match de workers 1
        const matchedSample1 = [];
        for (const t of tailsWanted) {
          const w = workerMap1.get(t);
          if (w) {
            matchedSample1.push({
              tail: t,
              worker_name: w.worker_name,
              worker_status: w.worker_status,
              hashrate_10min: w.hashrate_10min,
              onlineFrom: isOnlineFrom(w),
            });
          }
        }
        dlog("GROUP MATCH 1st READ", {
          coin,
          apiKey: mask(apiKey),
          tailsWanted,
          matchedFirstWorkers: matchedSample1.slice(0, 10),
        });

        const onlineIdsSet = new Set();
        const onlineIdsForHoursRaw = [];

        const offlineCandidateTails = new Set();

        // 1ª passagem
        for (const t of tailsWanted) {
          const w1 = workerMap1.get(t);
          if (!w1) {
            // não apareceu na API na 1ª leitura
            continue;
          }
          const ids = tailToIds.get(t) || [];
          const online1 = isOnlineFrom(w1);
          if (online1) {
            for (const id of ids) {
              if (!onlineIdsSet.has(id)) {
                onlineIdsSet.add(id);
                onlineIdsForHoursRaw.push(id);
              }
            }
          } else {
            offlineCandidateTails.add(t);
          }
        }

        const offlineIdsRaw = [];

        // ===== 2ª leitura só para confirmar offline =====
        if (offlineCandidateTails.size > 0) {
          try {
            dlog("GROUP 2nd READ BEGIN", {
              coin,
              apiKey: mask(apiKey),
              offlineCandidates: Array.from(offlineCandidateTails),
            });

            const workers2 = await fetchViaBTCListWithRetries(apiKey, coin, 1);
            apiCalls += 1;

            const workerMap2 = new Map();
            for (const w of workers2 || []) {
              const tw = tail(w.worker_name);
              if (tailToIds.has(tw)) {
                workerMap2.set(tw, w);
              }
            }

            const matchedSample2 = [];
            for (const t of offlineCandidateTails) {
              const w = workerMap2.get(t);
              if (w) {
                matchedSample2.push({
                  tail: t,
                  worker_name: w.worker_name,
                  worker_status: w.worker_status,
                  hashrate_10min: w.hashrate_10min,
                  onlineFrom: isOnlineFrom(w),
                });
              } else {
                matchedSample2.push({
                  tail: t,
                  worker_name: null,
                  worker_status: null,
                  hashrate_10min: null,
                  onlineFrom: null,
                });
              }
            }
            dlog("GROUP MATCH 2nd READ", {
              coin,
              apiKey: mask(apiKey),
              secondReadWorkers: matchedSample2.slice(0, 10),
            });

            for (const t of Array.from(offlineCandidateTails)) {
              const ids = tailToIds.get(t) || [];
              const w2 = workerMap2.get(t);
              if (!w2) {
                // 2ª leitura sem worker → dúvida → não mexemos
                continue;
              }
              const online2 = isOnlineFrom(w2);
              if (online2) {
                // afinal está online na 2ª leitura
                for (const id of ids) {
                  if (!onlineIdsSet.has(id)) {
                    onlineIdsSet.add(id);
                    onlineIdsForHoursRaw.push(id);
                  }
                }
              } else {
                // worker presente em ambas e nunca online → agora sim, offline
                for (const id of ids) offlineIdsRaw.push(id);
              }
            }
          } catch (e2) {
            console.error(
              "[uptime:viabtc] 2ª verificação falhou, a preservar estado",
              { group: k, err: String(e2?.message || e2) }
            );
            // Falha na 2ª leitura → não marcamos offline para ninguém deste grupo
          }
        }

        const onlineIdsRaw = Array.from(onlineIdsSet);

        dlog("GROUP FINAL CLASSIFICATION", {
          coin,
          apiKey: mask(apiKey),
          onlineIdsRaw,
          offlineIdsRaw,
          tailsWanted,
        });

        // 1) Horas online (dedupe por slot) — NÃO contar se em manutenção
        const ids = dedupe(onlineIdsForHoursRaw);
        if (ids.length) {
          const r = await sql/*sql*/`
            UPDATE miners
            SET total_horas_online = COALESCE(total_horas_online,0) + 0.25
            WHERE id = ANY(${ids})
              AND lower(COALESCE(status, '')) <> 'maintenance'
            RETURNING id
          `;
          const inc = Array.isArray(r) ? r.length : r?.count || 0;
          updated += inc;
          dlog("HOURS UPDATED", { coin, apiKey: mask(apiKey), count: inc });
        }

        // 2) Status (IGNORAR manutenção; só altera quando diverge)
        if (onlineIdsRaw.length) {
          const r1 = await sql/*sql*/`
            UPDATE miners
            SET status = 'online'
            WHERE id = ANY(${onlineIdsRaw})
              AND status IS DISTINCT FROM 'online'
              AND lower(COALESCE(status, '')) <> 'maintenance'
            RETURNING id
          `;
          const c1 = Array.isArray(r1) ? r1.length : r1?.count || 0;
          statusToOnline += c1;
          dlog("STATUS -> ONLINE", {
            coin,
            apiKey: mask(apiKey),
            count: c1,
          });
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
          const c2 = Array.isArray(r2) ? r2.length : r2?.count || 0;
          statusToOffline += c2;
          dlog("STATUS -> OFFLINE", {
            coin,
            apiKey: mask(apiKey),
            count: c2,
          });
        }
      } catch (err) {
        groupErrors += 1;
        console.error(
          `[uptime:viabtc] group error ${k}:`,
          err?.message || err
        );
      }
    };

    // Pool de concorrência
    for (const entry of groupEntries) {
      const task = (async () => await processGroup(entry))();
      allTasks.push(task);
      inflight.add(task);
      task.finally(() => inflight.delete(task));

      if (inflight.size >= CONCURRENCY) {
        await Promise.race(inflight).catch(() => {});
      }
    }

    await Promise.allSettled(allTasks);

    console.log(
      `[uptime:viabtc] ${sISO} groups=${totalGroups} miners=${totalMiners} api=${apiCalls} workers=${workersRelevant} extra=${workersExtra} online(+hrs)=${updated} statusOn=${statusToOnline} statusOff=${statusToOffline} errs=${groupErrors} dur=${Date.now() - t0}ms`
    );
    return {
      ok: true,
      updated,
      statusChanged: statusToOnline + statusToOffline,
      statusToOnline,
      statusToOffline,
      groups: totalGroups,
      miners: totalMiners,
      api: apiCalls,
      workers_relevant: workersRelevant,
      workers_extra: workersExtra,
      errs: groupErrors,
    };
  } catch (e) {
    console.error(
      `[uptime:viabtc] ${sISO} ERROR: ${e?.message || e}`
    );
    return { ok: false, error: String(e?.message || e) };
  }
}

/* =============================== */
/* Scheduler                       */
/* =============================== */
export function startUptimeViaBTC() {
  cron.schedule(
    "*/15 * * * *",
    async () => {
      try {
        await runUptimeViaBTCOnce();
      } catch (e) {
        console.error(
          "[uptime:viabtc] tick error:",
          e?.message || e
        );
      }
    },
    { timezone: "Europe/Lisbon" }
  );
  console.log("[jobs] ViaBTC (*/15) agendado.");
}
