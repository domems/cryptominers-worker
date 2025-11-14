// src/status-server.js
import dotenv from "dotenv";
dotenv.config();

import express from "express";
import { getMinerStatus, getMinersStatusMany } from "./status-service.js";

const app = express();

app.get("/health", (req, res) => {
  res.json({ ok: true, service: "cryptominers-worker-status" });
});

// um só miner
app.get("/status/:id", getMinerStatus);

// vários miners: GET /status?ids=1,2,3
app.get("/status", getMinersStatusMany);

const PORT = Number(process.env.STATUS_PORT || 4000);

app.listen(PORT, () => {
  console.log(
    `[status-server] listening on port ${PORT} (GET /status/:id, GET /status?ids=...)`
  );
});
