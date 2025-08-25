import Fastify from "fastify";
import { getProducer } from "./kafka";

export function createHealthServer(state: {
  lastTickIso: string | null;
  rateLimitBackoffMs?: number;
}) {
  const app = Fastify({ logger: false });

  app.get("/readyz", async () => {
    getProducer();
    return { ok: true };
  });

  app.get("/healthz", async () => {
    return {
      ok: true,
      lastTickIso: state.lastTickIso,
      rateLimitBackoffMs: state.rateLimitBackoffMs ?? 0,
    };
  });

  return app;
}
