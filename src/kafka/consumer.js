import { Kafka, logLevel } from "kafkajs";
import { config } from "../config/index.js";
import { broadcastLeaderboard } from "../websocket/server.js";

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  ssl: config.kafka.ssl,
  sasl: config.kafka.sasl,
  logLevel: logLevel.WARN,
});

export const consumer = kafka.consumer({
  groupId: config.kafka.groupId,
});

let isConsumerConnected = false;

export const connectConsumer = async () => {
  if (isConsumerConnected) return;
  try {
    console.log(`[Kafka Consumer] Connecting to brokers: ${config.kafka.brokers.join(", ")}...`);
    await consumer.connect();
    isConsumerConnected = true;
    console.log("[Kafka Consumer] Connected successfully.");
  } catch (error) {
    console.error("[Kafka Consumer] Connection failed:", error.message);
    throw error;
  }
};

export const disconnectConsumer = async () => {
  if (!isConsumerConnected) return;
  try {
    await consumer.disconnect();
    isConsumerConnected = false;
    console.log("[Kafka Consumer] Disconnected.");
  } catch (error) {
    console.error("[Kafka Consumer] Error disconnecting:", error.message);
  }
};

/**
 * Subscribe and listen to f1_analysis messages to broadcast via WebSocket
 */
export const startAnalysisConsumer = async () => {
  await connectConsumer();

  await consumer.subscribe({
    topic: config.kafka.inputTopic,
    fromBeginning: false,
  });

  console.log(`[Kafka Consumer] Subscribed to topic: "${config.kafka.inputTopic}"`);
  console.log(`[Kafka Consumer] Streaming leaderboard messages to WebSocket clients...\n`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const rawValue = message.value?.toString();
        if (!rawValue) return;

        const data = JSON.parse(rawValue);
        const race = data.race || message.key?.toString() || "Unknown Race";

        console.log(
          `📥 [Kafka] Received analyzed leaderboard for "${race}" (Round ${data.round}/${data.totalRounds || 24})`
        );

        // Broadcast to all connected React clients via WebSockets
        broadcastLeaderboard(data);
      } catch (err) {
        console.error(`❌ [Kafka Consumer] Error processing analysis message:`, err.message);
      }
    },
  });
};
