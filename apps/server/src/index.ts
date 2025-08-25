import "dotenv/config";
import Fastify from "fastify";
import fastifyCors from "@fastify/cors";
import {
  registerRealTimeServer,
  initializeKafkaListener,
} from "./lib/websocketServer.js";

import {
  fastifyTRPCPlugin,
  type FastifyTRPCPluginOptions,
} from "@trpc/server/adapters/fastify";
import { createContext } from "./lib/context";
import { appRouter, type AppRouter } from "./routers/index";

import { startKafka, stopKafka, isKafkaReady } from "./services/kafka";
import { closeInflux } from "./services/influx";

import ScalarApiReference from "@scalar/fastify-api-reference";
import {
  fastifyTRPCOpenApiPlugin,
  generateOpenApiDocument,
} from "trpc-to-openapi";
import pkg from "../package.json" assert { type: "json" };
import { printBanner } from "@cryptoflow/banner";
import { env } from "./env";
import logger from "./lib/logger";

const baseCorsConfig = {
  origin: env.CORS_ORIGIN.split(",").map((o) => o.trim()),
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
  credentials: true,
  maxAge: 86400,
};

const fastify = Fastify({
  logger: false,
});

fastify.register(fastifyCors, baseCorsConfig);

await registerRealTimeServer(fastify);

fastify.register(fastifyTRPCPlugin, {
  prefix: "/trpc",
  trpcOptions: {
    router: appRouter,
    createContext,
    onError({ path, error }) {
      logger.error(`tRPC error on '${path || "unknown"}'`, {
        message: error.message,
        stack: error.stack,
      });
    },
  } satisfies FastifyTRPCPluginOptions<AppRouter>["trpcOptions"],
});

fastify.get("/", async () => "OK");

fastify.get("/readyz", async (_req, reply) => {
  const kafkaOk = await isKafkaReady().catch(() => false);
  if (kafkaOk) return { ok: true, kafkaOk };
  return reply.code(503).send({ ok: false, kafkaOk });
});

await fastify.register(fastifyTRPCOpenApiPlugin, {
  basePath: "/api",
  router: appRouter,
  createContext,
});

fastify.get("/openapi.json", async (_req, reply) => {
  const trpcDoc = generateOpenApiDocument(appRouter, {
    title: "Corevia tRPC API",
    version: pkg.version,
    baseUrl: "http://localhost:3000/api",
  });
  reply.header("Content-Type", "application/json").send(trpcDoc);
});

await fastify.register(ScalarApiReference, {
  routePrefix: "/reference",
  configuration: {
    url: "/openapi.json",
    title: `${pkg.name} tRPC API`,
    layout: "modern",
    theme: "purple",
    darkMode: true,
  },
});

async function start() {
  logger.start("🚀 Starting server…");

  logger.kafka("connect", "Connecting Kafka (metrics.events → Influx) …");
  await startKafka();
  logger.success("✓ Kafka connected (metrics.events)");

  logger.kafka("connect", "Initializing Kafka listener for live prices …");
  try {
    await initializeKafkaListener();
    logger.success("✓ Kafka prices listener ready");
  } catch (e: any) {
    logger.warn("SSE will run without live Kafka prices", {
      error: e?.message,
    });
  }

  logger.start("Starting Fastify on :3000 …");
  await fastify.listen({ port: 3000, host: "0.0.0.0" });

  printBanner("SERVER", "Server listening at http://0.0.0.0:3000");

  logger.success("✅ ALL SYSTEMS GO!");
  logger.box("📊 ENDPOINTS", [
    "SSE Stream:        http://localhost:3000/api/stream/prices/:coin",
    "Live Candles:      http://localhost:3000/api/live-candles/:coin",
    "Stream Status:     http://localhost:3000/api/stream/status",
    "Health Check:      http://localhost:3000/readyz",
    "OpenAPI (JSON):    http://localhost:3000/openapi.json",
    "API Docs (Scalar): http://localhost:3000/reference",
    "tRPC:              http://localhost:3000/trpc/*",
  ]);
}

fastify.addHook("onClose", async () => {
  logger.stop("Shutting down server…");
  try {
    await Promise.allSettled([stopKafka(), closeInflux()]);
    logger.success("✓ Server shutdown complete");
  } catch (err: any) {
    logger.error("Error during shutdown", { error: err?.message });
  }
});

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    logger.warn(`Signal received: ${sig} — closing Fastify …`);
    try {
      await fastify.close();
      process.exit(0);
    } catch (err: any) {
      logger.error("Error during fastify.close()", { error: err?.message });
      process.exit(1);
    }
  });
}

process.on("unhandledRejection", (reason) => {
  logger.error("Unhandled Rejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
    stack: reason instanceof Error ? reason.stack : undefined,
  });
});

process.on("uncaughtException", (error) => {
  logger.fatal("Uncaught Exception", {
    error: error.message,
    stack: error.stack,
  });
  process.exit(1);
});

await start().catch(async (e) => {
  logger.fatal("Failed to start server", {
    error: e?.message,
    stack: e?.stack,
  });
  await stopKafka().catch(() => {});
  process.exit(1);
});
