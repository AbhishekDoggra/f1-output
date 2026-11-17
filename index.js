import http from "http";
import app from "./src/app.js";
import { config } from "./src/config/index.js";
import { initWebSocketServer } from "./src/websocket/server.js";
import {
  startAnalysisConsumer,
  disconnectConsumer,
} from "./src/kafka/consumer.js";

const server = http.createServer(app);

// Initialize WebSocket server attached to HTTP server
initWebSocketServer(server);

// Start listening on configured port
server.listen(config.port, () => {
  console.log(`\n🚀 F1 Output Service running on http://localhost:${config.port}`);
  console.log(`⚡ WebSocket endpoint available at ws://localhost:${config.port}`);
});

// Start Kafka Consumer
const startConsumer = async () => {
  try {
    console.log(`🏎️  Starting Kafka Consumer for topic: "${config.kafka.inputTopic}"...`);
    await startAnalysisConsumer();
  } catch (error) {
    console.error(`⚠️  [Kafka Consumer Warning] Could not connect to Kafka: ${error.message}`);
    console.error(`   Output service will keep running. Check credentials and restart when ready.\n`);
  }
};

startConsumer();

// Graceful shutdown
const shutdown = async (signal) => {
  console.log(`\nReceived ${signal}. Gracefully shutting down...`);
  try {
    await disconnectConsumer();
  } catch (err) {
    console.error("Error during Kafka disconnect:", err.message);
  }

  server.close(() => {
    console.log("Server closed.");
    process.exit(0);
  });

  setTimeout(() => {
    console.error("Forcefully shutting down after timeout.");
    process.exit(1);
  }, 10000);
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
