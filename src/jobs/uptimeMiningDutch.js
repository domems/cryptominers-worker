// src/jobs/uptimeMiningDutch.js
import cron from "node-cron";
import fetchOrig from "node-fetch";
import { sql } from "../config/db.js";
import { redis } from "../config/upstash.js";

/* =============================== */
/* Logger + config                 */
/* =============================== */
const TAG = "[uptime:miningdutch]";
const DEBUG =
  (process.env.DEBUG_UPTIME_MININGDUTCH ?? "false").toLowerCase() === "true";

// tolerância para considerar que continua online (erros / sem info)
const GRACE_MINUTES = 60;
// quantos slots de 15 min seguidos offline até assumir OFFLINE
const OFFLINE_TOLERANCE_SLOTS = 4;

// usa fetch global se existir, senão node-fetch
const fetch = globalThis.fetch || fetchOrig;

/* =============================== */
/* time slot (15 min, UTC)         */
/* =============================== */
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

/* =============================== */
/* helpers                         */
/* =============================== */
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

function minutesDiff(aISO, bISO) {
  const a = new Date(aISO).getTime();
  const b = new Date(bISO).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return Infinity;
  return Math.abs(b - a) / (60 * 1000);
}

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
  // Mining-Dutch: alive=1/0, hashrate number, status string
  const alive = Number(w?.alive ?? w?.online ?? 0);
  if (!Number.isNaN(alive) && alive > 0) return true;

  const hr = Number(w?.hashrate ?? w?.hash ?? 0);
  if (!Number.isNaN(hr) && hr > 0) return true;

  const st = low(w?.status);
  if (
    st &&
    ["alive", "online", "active", "up", "working", "connected"].includes(st)
  ) {
    return true;
  }

  return false;
}

/* =============================== */
/* redis keys helpers              */
/* =============================== */
const redisKeyLastOnline = (id) => `uptime:lastOnline:miningdutch:${id}`;
const redisKeyOfflineStreak = (id) =>
  `uptime:offlineStreak:miningdutch:${id}`;

/* =============================== */
/* parser robusto                  */
/* =============================== */
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

/* =============================== */
/* fetch com logs                  */
/* =============================== */
async function fetchMiningDutchWorkers({ coin, account_id, api_key }) {
  const urls = buildCandidateUrls({ coin, account_id, api_key });
  let lastErr;

  for (const url of urls) {
    const started = Date.now();
    try {
      if (DEBUG) {
        console.log(TAG, "FETCH_BEGIN", { url, coin, account_id });
      }

      const res = await fetch(url, { timeout: 12_000 });
      const ms = Date.now() - started;

      if (!res.ok) {
        const text = await res.text().catch(() => "<no-body>");
        console.warn(TAG, "HTTP_ERROR", {
          url,
          status: res.status,
          ms,
          body: text.slice(0, 300),
        });
        lastErr = new Error(`HTTP ${res.status}`);
        continue;
      }

      const data = await res.json().catch(() => null);
      const list = parseWorkersPayload(data);

      if (!list) {
        console.warn(
          TAG,
          "SCHEMA_UNEXPECTED",
          url,
          JSON.stringify(data)?.slice(0, 300)
        );
        continue;
      }

      if (DEBUG) {
        console.log(TAG, "FETCH_OK", {
          url,
          ms,
          count: list.length,
          sample: list[0]
            ? { name: list[0].name, status: list[0].status }
            : null,
        });
      }

      // normalização final
      return list.map((w) => ({
        name: norm(w.name || w.username),
        hashrate: Number(w.hashrate || 0),
        status: norm(w.status || (Number(w.alive) > 0 ? "alive" : "")),
        alive: Number(w.alive || 0),
        raw: w.raw || {},
      }));
    } catch (e) {
      const ms = Date.now() - started;
      lastErr = e;
      console.warn(TAG, "FETCH_FAIL", {
        url,
        ms,
        error: e?.message || String(e),
      });
    }
  }

  throw lastErr ?? new Error("All MiningDutch endpoints failed");
}

/* =============================== */
/* GRACE em caso de erro / noInfo */
/* =============================== */
/**
 * Para ERRO de API / worker não devolvido:
 * - Se esteve online nos últimos GRACE_MINUTES → conta horas (fallbackOnlineIds)
 * - Não mexe em status.
 */
async function classifyErrorWithGrace(m, slot, fallbackOnlineIds) {
  const key = redisKeyLastOnline(m.id);

  try {
    const last = await redis.get(key);
    if (last && minutesDiff(last, slot) <= GRACE_MINUTES) {
      // conta horas como online neste slot
      fallbackOnlineIds.push(m.id);
    }
  } catch (e) {
    console.warn(TAG, "redis.get lastOnline (error) failed", {
      id: m.id,
      error: e?.message || String(e),
    });
  }
}

/* =============================== */
/* offline streak para "online"   */
/* =============================== */
/**
 * Worker estava ONLINE na BD e a API confirmou OFFLINE (após 2ª chamada).
 * Só marcamos offline depois de OFFLINE_TOLERANCE_SLOTS slots seguidos.
 * Até lá: conta horas (fallbackOnlineIds) e mantém status.
 */
async function handleConfirmedApiOfflineForOnlineMiner(
  m,
  slot,
  fallbackOnlineIds,
  offlineIdsConfirmed
) {
  const streakKey = redisKeyOfflineStreak(m.id);
  let streak = 1;

  try {
    const val = await redis.incr(streakKey);
    streak = Number.isFinite(Number(val)) ? Number(val) : 1;
  } catch (e) {
    console.warn(TAG, "redis.incr offlineStreak failed", {
      id: m.id,
      error: e?.message || String(e),
    });
    streak = 1;
  }

  if (streak < OFFLINE_TOLERANCE_SLOTS) {
    // ainda em tolerância → tratamos como online para faturação
    fallbackOnlineIds.push(m.id);
    if (DEBUG) {
      console.log(TAG, "OFFLINE_TOLERANCE", {
        id: m.id,
        slot,
        streak,
        needed: OFFLINE_TOLERANCE_SLOTS,
      });
    }
    return;
  }

  // atingiu o limite → agora confirmamos OFFLINE
  offlineIdsConfirmed.push(m.id);

  try {
    await redis.del(redisKeyLastOnline(m.id));
    await redis.del(streakKey);
  } catch (e) {
    console.warn(TAG, "redis.del streak/lastOnline (confirmed offline) failed", {
      id: m.id,
      error: e?.message || String(e),
    });
  }

  if (DEBUG) {
    console.log(TAG, "OFFLINE_CONFIRMED", {
      id: m.id,
      slot,
      streak,
    });
  }
}

/**
 * Quando API confirma ONLINE:
 * - adiciona a onlineIdsConfirmed
 * - grava lastOnline
 * - limpa offlineStreak
 */
async function markOnlineAndResetStreak(m, slot, onlineIdsConfirmed) {
  onlineIdsConfirmed.push(m.id);

  try {
    await redis.set(redisKeyLastOnline(m.id), slot, {
      ex: 7 * 24 * 60 * 60,
    });
  } catch (e) {
    console.warn(TAG, "redis.set lastOnline failed", {
      id: m.id,
      error: e?.message || String(e),
    });
  }

  try {
    await redis.del(redisKeyOfflineStreak(m.id));
  } catch (e) {
    console.warn(TAG, "redis.del offlineStreak failed", {
      id: m.id,
      error: e?.message || String(e),
    });
  }
}

/* =============================== */
/* controle de slot                */
/* =============================== */
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

/* =============================== */
/* job principal                   */
/* =============================== */
export async function runUptimeMiningDutchOnce() {
  const sISO = slotISO();
  beginSlot(sISO);

  if (DEBUG) {
    console.log(TAG, "RUN_BEGIN", { slot: sISO });
  }

  const lockKey = `uptime:${sISO}:miningdutch`;
  const gotLock = await redis.set(lockKey, "1", { nx: true, ex: 20 * 60 });
  if (!gotLock) {
    if (DEBUG) {
      console.log(TAG, "LOCK_SKIP", { slot: sISO });
    }
    return { ok: true, skipped: true };
  }

  let hoursUpdated = 0;
  let statusToOnline = 0;
  let statusToOffline = 0;

  try {
    const miners = await sql/*sql*/`
      SELECT id, worker_name, api_key, coin, status
      FROM miners
      WHERE pool = 'MiningDutch'
        AND api_key IS NOT NULL AND api_key <> ''
        AND worker_name IS NOT NULL AND worker_name <> ''
    `;

    if (!miners.length) {
      if (DEBUG) {
        console.log(TAG, "NO_MINERS");
      }
      return { ok: true, updated: 0, statusChanged: 0 };
    }

    // group by api_key + account_id (head(worker_name)) + coin
    const groups = new Map();
    for (const m of miners) {
      const account_id = head(m.worker_name);
      const key = `${m.api_key}::${account_id}::${m.coin || ""}`;
      if (!groups.has(key)) {
        groups.set(key, {
          account_id,
          api_key: m.api_key,
          coin: m.coin,
          list: [],
        });
      }
      groups.get(key).list.push(m);
    }

    for (const [, grp] of groups) {
      const { account_id, api_key, coin, list } = grp;

      if (DEBUG) {
        console.log(TAG, "GROUP_BEGIN", {
          account_id,
          coin,
          miners: list.length,
        });
      }

      const onlineIdsConfirmed = [];   // API confirmou online neste slot
      const offlineIdsConfirmed = [];  // API confirmou offline (após tolerância)
      const fallbackOnlineIds = [];    // online por GRACE / tolerância

      /* ---------- 1) Fetch com retry de grupo ---------- */
      let workers = null;
      let groupFullyFailed = false;

      try {
        try {
          workers = await fetchMiningDutchWorkers({
            coin,
            account_id,
            api_key,
          });
        } catch (e1) {
          console.error(
            TAG,
            "GROUP_FETCH_ERROR_FIRST",
            { account_id, coin },
            e1?.message || String(e1)
          );

          // retry total do grupo
          try {
            workers = await fetchMiningDutchWorkers({
              coin,
              account_id,
              api_key,
            });
            console.warn(TAG, "GROUP_FETCH_RECOVERED", {
              account_id,
              coin,
            });
          } catch (e2) {
            console.error(
              TAG,
              "GROUP_FETCH_ERROR_SECOND",
              { account_id, coin },
              e2?.message || String(e2)
            );
            groupFullyFailed = true;
          }
        }

        if (groupFullyFailed || !workers) {
          // API deste grupo está mesmo em erro neste slot:
          // → NUNCA marcamos offline
          // → Só somamos horas se GRACE disser que esteve online há pouco
          for (const m of list) {
            await classifyErrorWithGrace(m, sISO, fallbackOnlineIds);
          }
        } else {
          /* ---------- 2) 1ª classificação com base no 1º fetch ---------- */
          const byTail = new Map();
          for (const w of workers) {
            const keyTail = low(tail(w.name) || w.name);
            if (keyTail) byTail.set(keyTail, w);
          }

          const suspicious = [];        // API offline mas BD dizia online
          const offlineCandidates = []; // API offline com BD ≠ online
          const noInfo = [];            // worker não apareceu na resposta → tratar como erro/inconclusivo

          for (const m of list) {
            const t = low(tail(m.worker_name));
            const info = t ? byTail.get(t) : null;

            if (info) {
              const apiOnline = isOnlineFromWorker(info);

              if (apiOnline) {
                // API garantiu online → aceitamos e reset streak
                await markOnlineAndResetStreak(m, sISO, onlineIdsConfirmed);
              } else {
                const statusLower = low(m.status);
                if (statusLower === "online") {
                  // EXACTAMENTE o caso: API diz offline mas BD está online
                  suspicious.push(m);
                } else {
                  // API disse offline e BD já não está online → mantém offline
                  offlineCandidates.push(m);
                }
              }
            } else {
              // Worker não veio na resposta → tratamos como erro/inconclusivo,
              // não marcamos offline por isto, apenas usamos GRACE para horas.
              noInfo.push(m);
            }
          }

          /* ---------- 3) 2ª chamada só para os "suspeitos" ---------- */
          if (suspicious.length) {
            let workersRetry = null;
            try {
              workersRetry = await fetchMiningDutchWorkers({
                coin,
                account_id,
                api_key,
              });
              console.warn(TAG, "SUSPICIOUS_RETRY", {
                account_id,
                coin,
                count: suspicious.length,
              });
            } catch (eRetry) {
              console.error(
                TAG,
                "SUSPICIOUS_RETRY_FAIL",
                { account_id, coin },
                eRetry?.message || String(eRetry)
              );
            }

            let byTailRetry = null;
            if (workersRetry && Array.isArray(workersRetry)) {
              byTailRetry = new Map();
              for (const w of workersRetry) {
                const keyTail = low(tail(w.name) || w.name);
                if (keyTail) byTailRetry.set(keyTail, w);
              }
            }

            for (const m of suspicious) {
              if (byTailRetry) {
                const t = low(tail(m.worker_name));
                const info2 = t ? byTailRetry.get(t) : null;
                const apiOnline2 = !!(info2 && isOnlineFromWorker(info2));

                if (apiOnline2) {
                  // 2ª chamada corrigiu → tratamos como online (confirmado)
                  await markOnlineAndResetStreak(m, sISO, onlineIdsConfirmed);
                  continue; // próximo suspeito
                }
              }

              // === 2ª chamada continuou a dar offline ===
              // Worker estava ONLINE na BD e API insistiu offline:
              // aplica regra de 4 slots de tolerância
              await handleConfirmedApiOfflineForOnlineMiner(
                m,
                sISO,
                fallbackOnlineIds,
                offlineIdsConfirmed
              );
            }
          }

          /* ---------- 4) restantes offlineCandidates ---------- */
          // Aqui entram workers que já não estavam "online" na BD.
          // Mantemos offline; não damos horas extra.
          for (const m of offlineCandidates) {
            offlineIdsConfirmed.push(m.id);
          }

          /* ---------- 5) "Erro"/sem info: só GRACE para horas ---------- */
          for (const m of noInfo) {
            // Nunca marca offline por não aparecer na resposta
            await classifyErrorWithGrace(m, sISO, fallbackOnlineIds);
          }
        }
      } catch (eGroup) {
        console.error(
          TAG,
          "GROUP_FATAL",
          { account_id, coin },
          eGroup?.message || String(eGroup)
        );
      }

      /* ---------- 6) Aplicar horas + estados para este grupo ---------- */
      // Horas online (confirmed + fallback, dedupe por slot)
      const onlineIdsForHours = dedupeForHours([
        ...onlineIdsConfirmed,
        ...fallbackOnlineIds,
      ]);
      if (onlineIdsForHours.length) {
        await sql/*sql*/`
          UPDATE miners
          SET total_horas_online = COALESCE(total_horas_online, 0) + 0.25
          WHERE id = ANY(${onlineIdsForHours})
            AND lower(COALESCE(status, '')) <> 'maintenance'
        `;
        hoursUpdated += onlineIdsForHours.length;
      }

      // Status – só mexemos com base em informação "forte"
      if (onlineIdsConfirmed.length) {
        const r1 = await sql/*sql*/`
          UPDATE miners
          SET status = 'online'
          WHERE id = ANY(${onlineIdsConfirmed})
            AND status IS DISTINCT FROM 'online'
            AND lower(COALESCE(status, '')) <> 'maintenance'
          RETURNING id
        `;
        statusToOnline += Array.isArray(r1) ? r1.length : r1?.count || 0;
      }

      if (offlineIdsConfirmed.length) {
        const r2 = await sql/*sql*/`
          UPDATE miners
          SET status = 'offline'
          WHERE id = ANY(${offlineIdsConfirmed})
            AND status IS DISTINCT FROM 'offline'
            AND lower(COALESCE(status, '')) <> 'maintenance'
          RETURNING id
        `;
        statusToOffline += Array.isArray(r2) ? r2.length : r2?.count || 0;
      }

      if (DEBUG) {
        console.log(TAG, "GROUP_DONE", {
          account_id,
          coin,
          workers: list.length,
          onlineConfirmed: onlineIdsConfirmed.length,
          offlineConfirmed: offlineIdsConfirmed.length,
          fallbackOnline: fallbackOnlineIds.length,
        });
      }
    }

    const statusChanged = statusToOnline + statusToOffline;
    console.log(
      TAG,
      "RUN_DONE",
      sISO,
      "horas+:",
      hoursUpdated,
      "->online:",
      statusToOnline,
      "->offline:",
      statusToOffline
    );
    return { ok: true, updated: hoursUpdated, statusChanged };
  } catch (e) {
    console.error("⛔ uptime:miningdutch", e);
    return { ok: false, error: String(e?.message || e) };
  }
}

/* =============================== */
/* cron                            */
/* =============================== */
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
    {
      timezone: "Europe/Lisbon",
    }
  );
  console.log("[jobs] MiningDutch (*/15) agendado.");
}
