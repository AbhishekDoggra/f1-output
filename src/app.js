import express from "express";
import cors from "cors";
import { config } from "./config/index.js";
import { getLatestState } from "./websocket/server.js";

const app = express();

app.use(
  cors({
    origin: config.frontendUrl || "*",
    methods: ["GET"],
  })
);
app.use(express.json());

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "F1 Pipeline Output Service",
    timestamp: new Date().toISOString(),
  });
});

// REST endpoint to get latest snapshot
app.get("/api/latest", (req, res) => {
  const latest = getLatestState();
  res.json({
    success: true,
    data: latest || null,
  });
});

export default app;
