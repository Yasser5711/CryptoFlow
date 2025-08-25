import {
  Kafka,
  logLevel,
  type Consumer,
  type Producer,
  type LogEntry,
} from "kafkajs";
import { env } from "./env";
import logger from "./logger";
let producer: Producer | null = null;
let consumer: Consumer | null = null;
let isConnecting = false;

export async function initKafka() {
  if (isConnecting) {
    throw new Error("Kafka connection already in progress");
  }
  if (producer && consumer) {
    logger.info("[kafka] Kafka already initialized");
    return { producer, consumer };
  }

  isConnecting = true;
  try {
    const brokers = env.KAFKA_BROKERS;
    if (!brokers || brokers.length === 0) {
      throw new Error("KAFKA_BROKERS is not configured");
    }

    logger.info("[kafka] Initializing producer with brokers:", brokers);

    const kafka = new Kafka({
      clientId: "analytics-prices",
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

    consumer = kafka.consumer({
      groupId: "analytics-prices-g1",
      sessionTimeout: 20000,
      rebalanceTimeout: 60000,
    });

    await consumer.connect();
    logger.kafka("connect", "[kafka] Consumer connected successfully");

    return { producer, consumer };
  } finally {
    isConnecting = false;
  }
}

export function getProducer(): Producer {
  if (!producer) throw new Error("Producer not ready");
  return producer;
}

export function getConsumer(): Consumer {
  if (!consumer) throw new Error("Consumer not ready");
  return consumer;
}

export async function shutdownKafka() {
  try {
    if (consumer) {
      await consumer.disconnect();
      logger.kafka("connect", "[kafka] Consumer disconnected successfully");
    }
  } catch (e) {
    logger.error("[kafka] Error disconnecting consumer:", e);
  }

  try {
    if (producer) {
      await producer.disconnect();
      logger.kafka("connect", "[kafka] Producer disconnected successfully");
    }
  } catch (e) {
    logger.error("[kafka] Error disconnecting producer:", e);
  }

  producer = null;
  consumer = null;
}
