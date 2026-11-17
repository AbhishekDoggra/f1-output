import dotenv from "dotenv";

dotenv.config();

export const config = {
  port: process.env.PORT || 3002,
  nodeEnv: process.env.NODE_ENV || "development",
  frontendUrl: process.env.FRONTEND_URL || "http://localhost:5173",
  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || "f1-output-service",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    inputTopic: process.env.KAFKA_INPUT_TOPIC || process.env.KAFKA_TOPIC || "f1_analysis",
    groupId: process.env.KAFKA_GROUP_ID || "f1-output-consumer-group",
    ssl: process.env.KAFKA_SSL === "true",
    sasl: process.env.KAFKA_SASL_USERNAME
      ? {
          mechanism: (process.env.KAFKA_SASL_MECHANISM || "scram-sha-512").toLowerCase(),
          username: process.env.KAFKA_SASL_USERNAME,
          password: process.env.KAFKA_SASL_PASSWORD,
        }
      : undefined,
  },
};
