import { request } from "node:http";
import { healthDefaults, type HealthCheckFailure } from "./health-state.js";

const maximumBodyBytes = 4 * 1024;
export const defaultHealthPath = "/health";

export function isHealthPath(value: string): boolean {
  return value.length <= 256 && /^\/(?!\/)[A-Za-z0-9._~/-]*$/.test(value) &&
    !value.split("/").some((part) => part === "." || part === "..");
}

export function isHealthBinding(host: string | null, port: number | null): boolean {
  return (host === "127.0.0.1" || host === "::1") &&
    port !== null && Number.isInteger(port) && port >= 1 && port <= 65535;
}

export interface HttpHealthResult {
  status: number | null;
  latencyMs: number;
  failureReason: HealthCheckFailure | null;
}

export async function probeHttpHealth(
  host: string, port: number, path = defaultHealthPath, signal?: AbortSignal,
  timeoutMs: number = healthDefaults.httpTimeoutMs,
): Promise<HttpHealthResult | null> {
  if (!isHealthBinding(host, port) || !isHealthPath(path) ||
    !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60_000) {
    throw new RangeError("Invalid local health probe configuration");
  }
  if (signal?.aborted) return null;

  const timeout = AbortSignal.timeout(timeoutMs);
  const requestSignal = signal ? AbortSignal.any([signal, timeout]) : timeout;
  const started = performance.now();
  return new Promise((resolve) => {
    let status: number | null = null;
    const finish = (failureReason: HealthCheckFailure | null) => {
      resolve(signal?.aborted ? null : {
        status, failureReason, latencyMs: Math.min(60_000, Math.round(performance.now() - started)),
      });
    };
    const interrupted = () => timeout.aborted ? "HTTP_TIMEOUT" : "HTTP_INVALID_RESPONSE";
    const req = request({
      hostname: host, port, path, method: "GET", agent: false, signal: requestSignal,
      headers: { Accept: "*/*", Connection: "close" },
    }, (response) => {
      status = response.statusCode ?? null;
      if (status === null || status < 100 || status > 599) {
        finish("HTTP_INVALID_RESPONSE");
        response.destroy();
        return;
      }
      if (status >= 300 && status < 400) {
        finish("HTTP_REDIRECT");
        response.destroy();
        return;
      }
      let bytes = 0;
      response.on("data", (chunk: Buffer) => {
        bytes += chunk.length;
        if (bytes > maximumBodyBytes) {
          finish("HTTP_INVALID_RESPONSE");
          response.destroy();
        }
      });
      response.once("error", () => finish(interrupted()));
      response.once("close", () => { if (!response.complete) finish(interrupted()); });
      response.once("end", () => finish(status !== null && status >= 200 && status < 300 ? null : "HTTP_STATUS"));
    });
    req.once("upgrade", (_response, socket) => {
      finish("HTTP_INVALID_RESPONSE");
      socket.destroy();
    });
    req.once("error", (error: NodeJS.ErrnoException) => {
      if (requestSignal.aborted) finish(interrupted());
      else if (error.code === "ECONNREFUSED") finish("HTTP_CONNECTION_REFUSED");
      else if (error.code?.startsWith("HPE_")) finish("HTTP_INVALID_RESPONSE");
      else finish("HTTP_UNREACHABLE");
    });
    req.end();
  });
}
