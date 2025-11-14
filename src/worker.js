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
/* Cron jobs                       */
/* =============================== */

console.log("[cryptominers-worker] boot – starting cron jobs");

startUptimeViaBTC();
startUptimeLTCPool();
startUptimeMiningDutch();
startUptimeF2Pool();
startUptimeBinance();

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

const PORT = Number(process.env.STATUS_PORT || 4000);

app.listen(PORT, () => {
  console.log(
    `[cryptominers-worker] status API listening on port ${PORT} (GET /status/:id, GET /status?ids=...)`
  );
});
