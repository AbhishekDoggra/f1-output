import { WebSocketServer, WebSocket } from "ws";

let wss = null;
let latestLeaderboardState = null;

export const initWebSocketServer = (httpServer) => {
  wss = new WebSocketServer({ server: httpServer });

  wss.on("connection", (ws, req) => {
    const ip = req.socket.remoteAddress;
    console.log(`🔌 [WebSocket] New client connected from ${ip} (Total clients: ${wss.clients.size})`);

    // Immediately send the latest state upon connection if available
    if (latestLeaderboardState) {
      ws.send(
        JSON.stringify({
          type: "INITIAL_STATE",
          payload: latestLeaderboardState,
        })
      );
    } else {
      ws.send(
        JSON.stringify({
          type: "WAITING_FOR_DATA",
          message: "Connected to F1 Output Service. Waiting for race data stream...",
        })
      );
    }

    ws.on("close", () => {
      console.log(`🔌 [WebSocket] Client disconnected (Remaining clients: ${wss.clients.size})`);
    });

    ws.on("error", (err) => {
      console.error(`⚠️ [WebSocket] Client socket error:`, err.message);
    });
  });

  console.log("⚡ [WebSocket] Server initialized and listening for client connections.");
  return wss;
};

/**
 * Broadcast race leaderboard update to all connected clients
 */
export const broadcastLeaderboard = (data) => {
  latestLeaderboardState = data;

  if (!wss || wss.clients.size === 0) return;

  const payload = JSON.stringify({
    type: "LEADERBOARD_UPDATE",
    payload: data,
  });

  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
};

export const getLatestState = () => latestLeaderboardState;
