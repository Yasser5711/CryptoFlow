import { Kafka, logLevel, type Producer, type LogEntry } from "kafkajs";
import { env } from "./env";
import logger from "./logger";
let producer: Producer | null = null;
let isConnecting = false;

export async function initProducer() {
  if (isConnecting) {
    throw new Error("Producer connection already in progress");
  }
  if (producer) {
    console.log("[kafka] Producer already initialized");
    return producer;
  }

  isConnecting = true;
  try {
    const brokers = env.KAFKA_BROKERS;
    if (!brokers || brokers.length === 0) {
      throw new Error("KAFKA_BROKERS is not configured");
    }

    logger.info("[kafka] Initializing producer with brokers:", brokers);

    const kafka = new Kafka({
      clientId: "prices-scraper",
      brokers,

      logCreator: () => (entry: LogEntry) => {
        const { level, log } = entry;
        const msg = `[KafkaJS] ${log.message}`;

        switch (level) {
          case logLevel.ERROR:
            logger.error(msg, log);
            break;
          default:
            break;
        }
      },
      retry: {
        initialRetryTime: 300,
        retries: 15,
        multiplier: 2,
        maxRetryTime: 30000,
      },
      connectionTimeout: 10000,
      requestTimeout: 30000,
      ssl: false,
    });

    producer = kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      retry: {
        initialRetryTime: 300,
        retries: 8,
        multiplier: 2,
      },
    });

    await producer.connect();
    logger.kafka("connect", "[kafka] Producer connected successfully");
    return producer;
  } finally {
    isConnecting = false;
  }
}

export function getProducer(): Producer {
  if (!producer) throw new Error("Kafka producer not initialized");
  return producer;
}

export async function shutdownProducer() {
  if (!producer) return;
  try {
    await producer.disconnect();
    logger.kafka("connect", "[kafka] Producer disconnected successfully");
  } catch (e) {
    logger.error("[kafka] Error disconnecting producer:", e);
  } finally {
    producer = null;
  }
}
