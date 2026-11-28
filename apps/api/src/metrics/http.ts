import { Counter, Gauge, Histogram, latencyBuckets, observe, type Registry } from "@cloudpilot/metrics";
import type { Request, RequestHandler } from "express";

const routePrefixes = new WeakMap<Request, string>();
const instrumented = new WeakMap<Registry, RequestHandler>();
const methods = new Set(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]);

export function metricsRoutePrefix(prefix: string): RequestHandler {
  // This prefix comes from a static router declaration, never from the URL.
  return (request, _response, next) => { if (routePrefixes.has(request)) routePrefixes.set(request, prefix); next(); };
}

export function httpMetrics(registry: Registry): RequestHandler {
  const existing = instrumented.get(registry);
  if (existing) return existing;
  const requests = new Counter({
    name: "cloudpilot_http_requests_total", help: "Terminal API HTTP requests",
    labelNames: ["method", "route", "status"], registers: [registry],
  });
  const durations = new Histogram({
    name: "cloudpilot_http_request_duration_seconds", help: "API HTTP duration in seconds",
    labelNames: ["method", "route", "status"], buckets: latencyBuckets, registers: [registry],
  });
  const inFlight = new Gauge({
    name: "cloudpilot_http_requests_in_flight", help: "In-flight API HTTP requests", registers: [registry],
  });
  const handler: RequestHandler = (request, response, next) => {
    if (request.path === "/metrics") { next(); return; }
    routePrefixes.set(request, "");
    const start = performance.now();
    observe(() => inFlight.inc());
    let recorded = false;
    function finish() {
      if (recorded) return;
      recorded = true;
      observe(() => inFlight.dec());
      const path: unknown = request.route?.path;
      const prefix = routePrefixes.get(request) ?? "";
      const route = typeof path === "string" && path.startsWith("/") ? prefix + path : "unmatched";
      const status = response.writableFinished ? String(response.statusCode) : "aborted";
      const method = methods.has(request.method) ? request.method : "OTHER";
      observe(() => requests.inc({ method, route, status }));
      observe(() => durations.observe({ method, route, status }, (performance.now() - start) / 1000));
    }
    response.once("finish", finish);
    response.once("close", finish);
    next();
  };
  instrumented.set(registry, handler);
  return handler;
}
