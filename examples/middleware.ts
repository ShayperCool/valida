import type { MiddlewareHandler } from "hono";

export const requestTiming: MiddlewareHandler = async (context, next) => {
  const started = performance.now();
  await next();
  context.header("server-timing", `app;dur=${(performance.now() - started).toFixed(1)}`);
};
