import Fastify from "fastify";

export function createHealthServer(state: {
  lastTickIso: string | null;
  lastFlushIso: string | null;
}) {
  const app = Fastify({ logger: false });

  app.get("/readyz", async () => ({ ok: true }));
  app.get("/healthz", async () => ({
    ok: true,
    lastTickIso: state.lastTickIso,
    lastFlushIso: state.lastFlushIso,
  }));

  return app;
}
