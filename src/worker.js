// src/worker.js
import dotenv from "dotenv";
dotenv.config();

import express from "express";

import { startUptimeViaBTC } from "./jobs/uptimeViaBTC.js";
import { startUptimeLTCPool } from "./jobs/uptimeLiteCoinPool.js";
import { startUptimeMiningDutch } from "./jobs/uptimeMiningDutch.js";
import { startUptimeF2Pool } from "./jobs/uptimeF2Pool.js";
import { startUptimeBinance } from "./jobs/uptimeBinance.js";

import {
  getMinerStatus,
  getMinersStatusMany,
} from "./status-service.js";

/* =============================== */
/* Logger básico                   */
/* =============================== */

const TAG = "[cryptominers-worker]";

function log(...args) {
  console.log(TAG, ...args);
}

function logError(...args) {
  console.error(TAG, ...args);
}

/* =============================== */
/* Global error handlers           */
/* =============================== */

process.on("unhandledRejection", (reason, promise) => {
  logError("unhandledRejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
  });
});

process.on("uncaughtException", (err) => {
  logError("uncaughtException", {
    message: err?.message,
    stack: err?.stack,
  });
  // Hard fail para PM2 reiniciar em vez de ficar em estado zombie
  process.exit(1);
});

/* =============================== */
/* Cron jobs                       */
/* =============================== */

log("boot – starting cron jobs");

try {
  startUptimeViaBTC();
  startUptimeLTCPool();
  startUptimeMiningDutch();
  startUptimeF2Pool();
  startUptimeBinance();

  log("cron jobs scheduled (ViaBTC, LiteCoinPool, MiningDutch, F2Pool, Binance)");
} catch (err) {
  logError("failed to start cron jobs", {
    message: err?.message,
    stack: err?.stack,
  });
  process.exit(1);
}

/* =============================== */
/* Mini-API de status              */
/* =============================== */

const app = express();

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "cryptominers-worker",
    cron: true,
  });
});

// 1 miner: GET /status/:id?refresh=1
app.get("/status/:id", getMinerStatus);

// vários miners: GET /status?ids=1,2,3
app.get("/status", getMinersStatusMany);

// ===============================
// Arranque HTTP
// ===============================

const PORT_RAW = process.env.STATUS_PORT || "4000";
const PORT = Number.parseInt(PORT_RAW, 10);

if (!Number.isFinite(PORT) || PORT <= 0) {
  logError("Invalid STATUS_PORT", { STATUS_PORT: PORT_RAW });
  process.exit(1);
}

const server = app.listen(PORT, () => {
  log(
    `status API listening on port ${PORT} (GET /status/:id, GET /status?ids=...)`
  );
});

// ===============================
// Shutdown limpo
// ===============================

function shutdown(signal) {
  log(`${signal} received – shutting down HTTP server...`);
  server.close((err) => {
    if (err) {
      logError("Error while closing HTTP server", {
        message: err?.message,
        stack: err?.stack,
      });
      process.exit(1);
    }
    log("HTTP server closed. Exiting.");
    process.exit(0);
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
