/**
 * CloudPilot — Authenticated Socket.IO Log Gateway
 *
 * Phase 5.6 — Realtime Deployment Logs
 *
 * PostgreSQL is the authoritative log store.
 * Redis notifications are reconciliation hints.
 *
 * Security:
 * - Explicit WebSocket origin validation.
 * - Session cookie authentication.
 * - Current session and workspace authorization.
 * - Authorization before individual batch delivery.
 * - Bounded connections and subscriptions.
 * - Serialized per-socket operations.
 * - Bounded replay and periodic reconciliation.
 *
 * Redis Pub/Sub is not a durable delivery mechanism.
 * Clients must tolerate duplicates and reconnects.
 */

import type { Server as HttpServer } from "node:http";

import type { Redis } from "ioredis";

import { Server } from "socket.io";
import type { Socket } from "socket.io";

import { prisma } from "@cloudpilot/database";

import {
  logSubscribePayloadSchema,
  logUnsubscribePayloadSchema,
  socketEventNames,
  type DeploymentLogEntryDto,
} from "@cloudpilot/queue";

import { env } from "../config/env.js";
import { logger } from "../config/logger.js";

import {
  hashSecret,
  SESSION_COOKIE,
} from "../services/session.service.js";

import {
  getEntriesInRange,
  getLogHistory,
  type LogScope,
} from "../services/deployment-logs.service.js";

import {
  isValidLogChannelPrefix,
  isLogNotificationForChannel,
  matchesLogNotificationScope,
  parseLogNotification,
} from "../services/log-channel.js";

// ==================================================
// Constants
// ==================================================

const MAX_CONNECTIONS = 500;

const MAX_CONNECTIONS_PER_USER = 10;

const MAX_PENDING_HANDSHAKES = 32;

const MAX_SUBSCRIPTIONS = 20;

const MAX_TOTAL_SUBSCRIPTIONS = 2_000;

const MAX_COOKIE_HEADER_BYTES = 8_192;

const MAX_SESSION_TOKEN_LENGTH = 256;

const MAX_HTTP_BUFFER_BYTES = 16 * 1024;

const MAX_PENDING_OPERATIONS = 16;

const MAX_EVENTS_PER_WINDOW = 30;

const EVENT_WINDOW_MS = 10_000;

const MAX_CURSOR =
  9_223_372_036_854_775_807n;

const MAX_HISTORY_REPLAY = 1_000;

const LIVE_BATCH_LIMIT = 1_000;

const MAX_REPLAY_PAGES = 8;

const MAX_REPLAY_DURATION_MS = 5_000;

const MAX_BATCH_MESSAGE_BYTES =
  512 * 1024;


const MAX_RECONCILE_SOCKETS = 32;

const RECONCILE_INTERVAL_MS = 5_000;

const SHUTDOWN_TIMEOUT_MS = 5_000;

const MAX_NOTIFICATION_BYTES = 4096;

const CURSOR_PATTERN =
  /^(?:0|[1-9][0-9]{0,18})$/;

// ==================================================
// Internal contracts
// ==================================================

interface Subscription {
  scope: LogScope;

  lastSent: bigint;

  dirty: boolean;

  active: boolean;
}

interface SocketData {
  userId: string;

  sessionId: string;

  activeWorkspaceId: string | null;

  subscriptions: Map<string, Subscription>;

  operationTail: Promise<void>;

  pendingOperations: number;

  eventWindowStartedAt: number;

  eventCount: number;

  closed: boolean;

  reconcileQueued: boolean;

  lastRevalidatedAt: number;
}

interface SessionIdentity {
  id: string;

  userId: string;

  activeWorkspaceId: string | null;

  expiresAt: Date;

  revokedAt: Date | null;
}

type Ack = (
  response: unknown,
) => void;

// ==================================================
// Cookie parsing
// ==================================================

function readCookie(
  header: string | undefined,
  name: string,
): string | null {
  if (
    typeof header !== "string" ||
    header.length === 0 ||
    header.length >
      MAX_COOKIE_HEADER_BYTES ||
    Buffer.byteLength(header, "utf8") >
      MAX_COOKIE_HEADER_BYTES
  ) {
    return null;
  }

  let result: string | null = null;

  for (const part of header.split(";")) {
    const index = part.indexOf("=");

    if (index < 0) {
      continue;
    }

    const key =
      part.slice(0, index).trim();

    if (key !== name) {
      continue;
    }

    // Reject ambiguous duplicate session cookies.
    if (result !== null) {
      return null;
    }

    try {
      const value = decodeURIComponent(
        part.slice(index + 1).trim(),
      );

      if (
        value.length === 0 ||
        value.length >
          MAX_SESSION_TOKEN_LENGTH ||
        /[\u0000-\u0020\u007f]/u.test(value)
      ) {
        return null;
      }

      result = value;
    } catch {
      return null;
    }
  }

  return result;
}

// ==================================================
// Cursor helpers
// ==================================================

function parseCursor(
  value: unknown,
): bigint | null {
  if (
    typeof value !== "string" ||
    value.length > 19 ||
    !CURSOR_PATTERN.test(value)
  ) {
    return null;
  }

  try {
    const cursor = BigInt(value);

    return (
      cursor >= 0n &&
      cursor <= MAX_CURSOR
    )
      ? cursor
      : null;
  } catch {
    return null;
  }
}

function subscriptionKey(
  projectId: string,
  deploymentId: string,
): string {
  return (
    `${projectId.toLowerCase()}:` +
    deploymentId.toLowerCase()
  );
}

// ==================================================
// Origin validation
// ==================================================

function isAllowedOrigin(
  origin: unknown,
): boolean {
  if (
    typeof origin !== "string" ||
    origin.length === 0 ||
    origin.length > 512 ||
    origin === "null"
  ) {
    return false;
  }

  try {
    const url = new URL(origin);

    if (
      url.protocol !== "http:" &&
      url.protocol !== "https:"
    ) {
      return false;
    }

    if (url.origin !== origin) {
      return false;
    }

    return env.corsOrigins.includes(
      url.origin,
    );
  } catch {
    return false;
  }
}

// ==================================================
// Timeout helper
// ==================================================

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timer:
    | ReturnType<typeof setTimeout>
    | undefined;

  try {
    return await Promise.race([
      operation,

      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(
            new Error(
              "Log gateway operation timed out.",
            ),
          );
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

// ==================================================
// Gateway
// ==================================================

export class LogGateway {
  private readonly io: Server;

  private readonly subscriber: Redis;

  private readonly prefix: string;

  private readonly pattern: string;

  private readonly maxSubscriptions: number;

  private readonly socketData =
    new Map<string, SocketData>();

  private reconcileTimer:
    | ReturnType<typeof setTimeout>
    | undefined;

  private reconcileTask:
    | Promise<void>
    | undefined;

  private closeTask:
    | Promise<void>
    | undefined;

  private pendingHandshakes = 0;

  private totalSubscriptions = 0;

  private reconcileOffset = 0;

  private closed = false;

  constructor(
    httpServer: HttpServer,
    subscriber: Redis,
  ) {
    if (
      !isValidLogChannelPrefix(
        env.LOG_REDIS_PREFIX,
      )
    ) {
      throw new Error(
        "Invalid log gateway Redis prefix.",
      );
    }

    this.prefix = env.LOG_REDIS_PREFIX;

    this.pattern =
      `${this.prefix}:deployment:*`;

    this.maxSubscriptions = Math.min(
      env.LOG_SOCKET_MAX_SUBSCRIPTIONS,
      MAX_SUBSCRIPTIONS,
    );

    this.subscriber = subscriber;

    this.io = new Server(httpServer, {
      cors: {
        origin: [...env.corsOrigins],

        credentials: true,
      },

      allowRequest: (request, callback) => {
        const allowed =
          !this.closed &&
          isAllowedOrigin(
            request.headers.origin,
          );

        callback(null, allowed);
      },

      transports: ["websocket"],

      maxHttpBufferSize:
        MAX_HTTP_BUFFER_BYTES,

      perMessageDeflate: false,

      connectTimeout: 10_000,

      pingInterval: 25_000,

      pingTimeout: 20_000,
    });

    this.io.use((socket, next) => {
      void this.authenticate(
        socket,
        next,
      );
    });

    this.registerEvents();

    this.subscriber.on(
      "pmessage",
      this.handleRedisMessage,
    );

    this.subscriber.on(
      "ready",
      this.handleRedisReady,
    );

    this.subscribeRedis();

    this.scheduleReconcile(
      RECONCILE_INTERVAL_MS,
    );
  }

  // ==================================================
  // Redis subscription
  // ==================================================

  private subscribeRedis(): void {
    if (this.closed) {
      return;
    }

    void this.subscriber
      .psubscribe(this.pattern)
      .catch(() => {
        if (!this.closed) {
          logger.debug(
            {
              code:
                "LOG_REDIS_SUBSCRIBE_FAILED",
            },
            "Log notification subscription unavailable",
          );
        }
      });
  }

  private readonly handleRedisReady =
    (): void => {
      if (this.closed) {
        return;
      }

      this.subscribeRedis();

      // Redis may have missed notifications
      // while disconnected.

      for (const data of
        this.socketData.values()) {
        for (const subscription of
          data.subscriptions.values()) {
          subscription.dirty = true;
        }
      }

      this.requestReconcile();
    };

  private readonly handleRedisMessage =
    (
      pattern: string,
      channel: string,
      raw: string,
    ): void => {
      if (
        this.closed ||
        pattern !== this.pattern ||
        typeof raw !== "string" ||
        raw.length >
          MAX_NOTIFICATION_BYTES
      ) {
        return;
      }

      this.onNotification(
        channel,
        raw,
      );
    };

  // ==================================================
  // Handshake authentication
  // ==================================================

  private async authenticate(
    socket: Socket,
    next: (error?: Error) => void,
  ): Promise<void> {
    if (
      this.closed ||
      this.pendingHandshakes >=
        MAX_PENDING_HANDSHAKES ||
      this.socketData.size +
        this.pendingHandshakes >=
        MAX_CONNECTIONS
    ) {
      next(new Error("CONNECTION_LIMIT"));

      return;
    }

    if (
      !isAllowedOrigin(
        socket.handshake.headers.origin,
      )
    ) {
      next(new Error("FORBIDDEN"));

      return;
    }

    this.pendingHandshakes += 1;

    try {
      const token = readCookie(
        socket.handshake.headers.cookie,
        SESSION_COOKIE,
      );

      if (!token) {
        next(new Error("UNAUTHORIZED"));

        return;
      }

      const session =
        await prisma.session.findUnique({
          where: {
            tokenHash: hashSecret(token),
          },

          select: {
            id: true,
            userId: true,
            activeWorkspaceId: true,
            expiresAt: true,
            revokedAt: true,
          },
        });

      if (
        this.closed ||
        !this.isValidSession(session)
      ) {
        next(new Error("UNAUTHORIZED"));

        return;
      }

      const data: SocketData = {
        userId: session.userId,

        sessionId: session.id,

        activeWorkspaceId:
          session.activeWorkspaceId,

        subscriptions: new Map(),

        operationTail:
          Promise.resolve(),

        pendingOperations: 0,

        eventWindowStartedAt:
          performance.now(),

        eventCount: 0,

        closed: false,

        reconcileQueued: false,

        lastRevalidatedAt:
          performance.now(),
      };

      socket.data = data;

      next();
    } catch {
      next(
        new Error(
          "DEPENDENCY_NOT_READY",
        ),
      );
    } finally {
      this.pendingHandshakes =
        Math.max(
          0,
          this.pendingHandshakes - 1,
        );
    }
  }

  private isValidSession(
    session: SessionIdentity | null,
  ): session is SessionIdentity {
    return (
      session !== null &&
      session.revokedAt === null &&
      session.expiresAt instanceof Date &&
      Number.isFinite(
        session.expiresAt.getTime(),
      ) &&
      session.expiresAt.getTime() >
        Date.now()
    );
  }

  // ==================================================
  // Event registration
  // ==================================================

  private registerEvents(): void {
    this.io.on(
      "connection",
      (socket) => {
        const data =
          socket.data as SocketData;

        if (
          this.closed ||
          !data ||
          !data.userId ||
          !(data.subscriptions instanceof Map) ||
          this.socketData.size >=
            MAX_CONNECTIONS
        ) {
          socket.disconnect(true);

          return;
        }

        let userConnections = 0;

        for (const existing of
          this.socketData.values()) {
          if (
            existing.userId === data.userId
          ) {
            userConnections += 1;
          }
        }

        if (
          userConnections >=
          MAX_CONNECTIONS_PER_USER
        ) {
          socket.disconnect(true);

          return;
        }

        this.socketData.set(
          socket.id,
          data,
        );

        socket.on(
          socketEventNames.subscribe,
          (
            payload: unknown,
            ack: unknown,
          ) => {
            if (
              !this.allowClientEvent(
                socket,
                data,
              )
            ) {
              return;
            }

            const accepted =
              this.enqueueOperation(
                socket,
                data,
                async () => {
                  await this.handleSubscribe(
                    socket,
                    data,
                    payload,
                    ack,
                  );
                },
              );

            if (!accepted) {
              this.safeAck(ack, {
                ok: false,
              });
            }
          },
        );

        socket.on(
          socketEventNames.unsubscribe,
          (payload: unknown) => {
            if (
              !this.allowClientEvent(
                socket,
                data,
              )
            ) {
              return;
            }

            // Invalidate synchronously so an
            // in-flight replay cannot continue
            // delivering after unsubscribe.

            this.handleUnsubscribe(
              data,
              payload,
            );
          },
        );

        socket.on(
          "disconnect",
          () => {
            this.cleanupSocket(
              socket,
            );
          },
        );
      },
    );
  }

  // ==================================================
  // Socket lifecycle checks
  // ==================================================

  private isSocketActive(
    socket: Socket,
    data: SocketData,
  ): boolean {
    return (
      !this.closed &&
      !data.closed &&
      socket.connected &&
      this.socketData.get(
        socket.id,
      ) === data
    );
  }

  private isSubscriptionActive(
    socket: Socket,
    data: SocketData,
    key: string,
    subscription: Subscription,
  ): boolean {
    return (
      this.isSocketActive(
        socket,
        data,
      ) &&
      subscription.active &&
      data.subscriptions.get(key) ===
        subscription
    );
  }

  // ==================================================
  // Client event rate limiting
  // ==================================================

  private allowClientEvent(
    socket: Socket,
    data: SocketData,
  ): boolean {
    if (
      !this.isSocketActive(
        socket,
        data,
      )
    ) {
      return false;
    }

    const now = performance.now();

    if (
      now -
        data.eventWindowStartedAt >=
      EVENT_WINDOW_MS
    ) {
      data.eventWindowStartedAt = now;

      data.eventCount = 0;
    }

    data.eventCount += 1;

    if (
      data.eventCount >
      MAX_EVENTS_PER_WINDOW
    ) {
      this.emitError(
        socket,
        null,
        "RATE_LIMITED",
        "Too many log subscription requests.",
      );

      socket.disconnect(true);

      return false;
    }

    return true;
  }

  // ==================================================
  // Serialized socket operations
  // ==================================================

  private enqueueOperation(
    socket: Socket,
    data: SocketData,
    operation: () => Promise<void>,
  ): boolean {
    if (
      !this.isSocketActive(
        socket,
        data,
      )
    ) {
      return false;
    }

    if (
      data.pendingOperations >=
      MAX_PENDING_OPERATIONS
    ) {
      this.emitError(
        socket,
        null,
        "OPERATION_LIMIT",
        "Too many pending log operations.",
      );

      socket.disconnect(true);

      return false;
    }

    data.pendingOperations += 1;

    data.operationTail =
      data.operationTail
        .then(async () => {
          if (
            !this.isSocketActive(
              socket,
              data,
            )
          ) {
            return;
          }

          await operation();
        })
        .catch(() => {
          if (
            this.isSocketActive(
              socket,
              data,
            )
          ) {
            this.emitError(
              socket,
              null,
              "DEPENDENCY_NOT_READY",
              "Log access could not be verified.",
            );

            // Fail closed when authorization
            // or database state is uncertain.

            socket.disconnect(true);
          }
        })
        .finally(() => {
          data.pendingOperations =
            Math.max(
              0,
              data.pendingOperations - 1,
            );
        });

    return true;
  }

  // ==================================================
  // Current session validation
  // ==================================================

  private async validateSession(
    socket: Socket,
    data: SocketData,
  ): Promise<boolean> {
    if (
      !this.isSocketActive(
        socket,
        data,
      )
    ) {
      return false;
    }

    const session =
      await prisma.session.findUnique({
        where: {
          id: data.sessionId,
        },

        select: {
          id: true,
          userId: true,
          activeWorkspaceId: true,
          expiresAt: true,
          revokedAt: true,
        },
      });

    if (
      !this.isSocketActive(
        socket,
        data,
      )
    ) {
      return false;
    }

    if (
      !this.isValidSession(session) ||
      session.userId !== data.userId
    ) {
      this.emitError(
        socket,
        null,
        "UNAUTHORIZED",
        "Your session is no longer valid.",
      );

      socket.disconnect(true);

      return false;
    }

    if (
      session.activeWorkspaceId !==
      data.activeWorkspaceId
    ) {
      this.emitError(
        socket,
        null,
        "ACCESS_REVOKED",
        "Your active workspace has changed.",
      );

      socket.disconnect(true);

      return false;
    }

    data.lastRevalidatedAt =
      performance.now();

    return true;
  }

  // ==================================================
  // Tenant authorization
  // ==================================================

  private async authorize(
    socket: Socket,
    data: SocketData,
    projectId: string,
    deploymentId: string,
  ): Promise<LogScope | null> {
    if (
      !(await this.validateSession(
        socket,
        data,
      ))
    ) {
      return null;
    }

    const workspaceId =
      data.activeWorkspaceId;

    if (!workspaceId) {
      return null;
    }

    const membership =
      await prisma.workspaceMember.findFirst({
        where: {
          userId: data.userId,

          workspaceId,
        },

        select: {
          workspaceId: true,
        },
      });

    if (!membership) {
      return null;
    }

    const project =
      await prisma.project.findFirst({
        where: {
          id: projectId,

          workspaceId,
        },

        select: {
          id: true,
        },
      });

    if (!project) {
      return null;
    }

    const deployment =
      await prisma.deployment.findFirst({
        where: {
          id: deploymentId,

          workspaceId,

          projectId,
        },

        select: {
          id: true,
        },
      });

    if (
      !deployment ||
      !this.isSocketActive(
        socket,
        data,
      )
    ) {
      return null;
    }

    return {
      workspaceId,

      projectId,

      deploymentId,
    };
  }

  // ==================================================
  // Subscription management
  // ==================================================

  private async handleSubscribe(
    socket: Socket,
    data: SocketData,
    rawPayload: unknown,
    ack: unknown,
  ): Promise<void> {
    const parsed =
      logSubscribePayloadSchema.safeParse(
        rawPayload,
      );

    if (!parsed.success) {
      this.emitError(
        socket,
        null,
        "INVALID_SUBSCRIPTION",
        "The subscription request is invalid.",
      );

      this.safeAck(ack, {
        ok: false,
      });

      return;
    }

    const projectId =
      parsed.data.projectId.toLowerCase();

    const deploymentId =
      parsed.data.deploymentId.toLowerCase();

    const key = subscriptionKey(
      projectId,
      deploymentId,
    );

    const existing =
      data.subscriptions.get(key);

    if (
      !existing &&
      (
        data.subscriptions.size >=
          this.maxSubscriptions ||
        this.totalSubscriptions >=
          MAX_TOTAL_SUBSCRIPTIONS
      )
    ) {
      this.emitError(
        socket,
        deploymentId,
        "SUBSCRIPTION_LIMIT",
        "Too many active log subscriptions.",
      );

      this.safeAck(ack, {
        ok: false,
      });

      return;
    }

    const cursor =
      parsed.data.lastCursor === undefined
        ? 0n
        : parseCursor(
            parsed.data.lastCursor,
          );

    if (cursor === null) {
      this.emitError(
        socket,
        deploymentId,
        "INVALID_CURSOR",
        "The requested log cursor is invalid.",
      );

      this.safeAck(ack, {
        ok: false,
      });

      return;
    }

    // Register a cancellable reservation before
    // asynchronous authorization.

    if (existing) {
      this.removeSubscription(
        data,
        key,
      );
    }

    const subscription: Subscription = {
      scope: {
        workspaceId:
          data.activeWorkspaceId ?? "",

        projectId,

        deploymentId,
      },

      lastSent: cursor,

      dirty: true,

      active: true,
    };

    data.subscriptions.set(
      key,
      subscription,
    );

    this.totalSubscriptions += 1;

    const scope = await this.authorize(
      socket,
      data,
      projectId,
      deploymentId,
    );

    if (
      !this.isSubscriptionActive(
        socket,
        data,
        key,
        subscription,
      )
    ) {
      this.safeAck(ack, {
        ok: false,
      });

      return;
    }

    if (!scope) {
      this.revokeSubscription(
        socket,
        data,
        key,
        subscription,
      );

      this.safeAck(ack, {
        ok: false,
      });

      return;
    }

    subscription.scope = scope;

    // The subscription exists before replay.
    // Redis hints received during replay set dirty.
    // Delivery always resumes from lastSent.

    await this.replaySubscription(
      socket,
      data,
      key,
      subscription,
      true,
    );

    const active =
      this.isSubscriptionActive(
        socket,
        data,
        key,
        subscription,
      );

    this.safeAck(ack, {
      ok: active,

      ...(active
        ? {
            highWatermark:
              subscription.lastSent.toString(10),
          }
        : {}),
    });
  }

  private handleUnsubscribe(
    data: SocketData,
    rawPayload: unknown,
  ): void {
    const parsed =
      logUnsubscribePayloadSchema.safeParse(
        rawPayload,
      );

    if (!parsed.success) {
      return;
    }

    const key = subscriptionKey(
      parsed.data.projectId,
      parsed.data.deploymentId,
    );

    this.removeSubscription(
      data,
      key,
    );
  }

  private removeSubscription(
    data: SocketData,
    key: string,
  ): void {
    const subscription =
      data.subscriptions.get(key);

    if (!subscription) {
      return;
    }

    subscription.active = false;

    subscription.dirty = false;

    data.subscriptions.delete(key);

    this.totalSubscriptions =
      Math.max(
        0,
        this.totalSubscriptions - 1,
      );
  }

  private revokeSubscription(
    socket: Socket,
    data: SocketData,
    key: string,
    subscription: Subscription,
  ): void {
    if (
      data.subscriptions.get(key) !==
      subscription
    ) {
      return;
    }

    this.removeSubscription(
      data,
      key,
    );

    this.emitError(
      socket,
      subscription.scope.deploymentId,
      "ACCESS_REVOKED",
      "You are not authorized to view these logs.",
    );
  }

  // ==================================================
  // Outbound delivery protection
  // ==================================================

  private canDeliver(
    socket: Socket,
    data: SocketData,
  ): boolean {
    if (
      !this.isSocketActive(
        socket,
        data,
      )
    ) {
      return false;
    }

    // maxHttpBufferSize only limits inbound data.
    //
    // Also inspect Engine.IO transport readiness
    // and its pending packet buffer before sending.

    if (
      !socket.conn.transport.writable
    ) {
      return false;
    }

    return true;
  }

  // ==================================================
  // Authorized batch delivery
  // ==================================================

  private async deliverBatch(
    socket: Socket,
    data: SocketData,
    key: string,
    subscription: Subscription,
    entries: DeploymentLogEntryDto[],
    gapDetected: boolean,
  ): Promise<boolean> {
    if (
      !this.isSubscriptionActive(
        socket,
        data,
        key,
        subscription,
      )
    ) {
      return false;
    }

    const scope = await this.authorize(
      socket,
      data,
      subscription.scope.projectId,
      subscription.scope.deploymentId,
    );

    if (
      !this.isSubscriptionActive(
        socket,
        data,
        key,
        subscription,
      )
    ) {
      return false;
    }

    if (
      !scope ||
      scope.workspaceId !==
        subscription.scope.workspaceId
    ) {
      this.revokeSubscription(
        socket,
        data,
        key,
        subscription,
      );

      return false;
    }

    if (
      !this.canDeliver(
        socket,
        data,
      )
    ) {
      subscription.dirty = true;

      return false;
    }

    const accepted:
      DeploymentLogEntryDto[] = [];

    let bytes = 0;

    let cursor =
      subscription.lastSent;

    for (const entry of entries) {
      const sequence =
        parseCursor(entry.sequence);

      if (
        sequence === null ||
        sequence === 0n ||
        entry.workspaceId !==
          scope.workspaceId ||
        entry.projectId !==
          scope.projectId ||
        entry.deploymentId !==
          scope.deploymentId
      ) {
        throw new Error(
          "Invalid log delivery scope.",
        );
      }

      if (sequence <= cursor) {
        continue;
      }

      const entryBytes =
        Buffer.byteLength(
          entry.message,
          "utf8",
        );

      if (
        accepted.length >=
          LIVE_BATCH_LIMIT ||
        bytes + entryBytes >
          MAX_BATCH_MESSAGE_BYTES
      ) {
        break;
      }

      if (sequence > cursor + 1n) {
        gapDetected = true;
      }

      accepted.push(entry);

      bytes += entryBytes;

      cursor = sequence;
    }

    socket.emit(
      socketEventNames.batch,
      {
        deploymentId:
          scope.deploymentId,

        entries: accepted,

        highWatermark:
          cursor.toString(10),

        gapDetected,
      },
    );

    // This is an emitted cursor, not a client
    // acknowledgement of durable receipt.
    //
    // Reconnecting clients supply their own
    // last successfully received cursor.

    subscription.lastSent = cursor;

    return true;
  }

  // ==================================================
  // History replay and reconciliation
  // ==================================================

  private async replaySubscription(
    socket: Socket,
    data: SocketData,
    key: string,
    subscription: Subscription,
    initial = false,
  ): Promise<void> {
    const startedAt =
      performance.now();

    subscription.dirty = false;

    const scope = await this.authorize(
      socket,
      data,
      subscription.scope.projectId,
      subscription.scope.deploymentId,
    );

    if (
      !this.isSubscriptionActive(
        socket,
        data,
        key,
        subscription,
      )
    ) {
      return;
    }

    if (
      !scope ||
      scope.workspaceId !==
        subscription.scope.workspaceId
    ) {
      this.revokeSubscription(
        socket,
        data,
        key,
        subscription,
      );

      return;
    }

    const history = await getLogHistory(
      scope,
      {
        after:
          subscription.lastSent.toString(10),

        limit: MAX_HISTORY_REPLAY,
      },
    );

    if (
      !this.isSubscriptionActive(
        socket,
        data,
        key,
        subscription,
      )
    ) {
      return;
    }

    const newest =
      history.newestSequence === null
        ? null
        : parseCursor(
            history.newestSequence,
          );

    if (
      history.newestSequence !== null &&
      newest === null
    ) {
      throw new Error(
        "Invalid log history watermark.",
      );
    }

    if (
      newest !== null &&
      subscription.lastSent > newest
    ) {
      this.emitResync(
        socket,
        subscription,
        "CURSOR_AHEAD",
        newest,
      );

      this.removeSubscription(
        data,
        key,
      );

      return;
    }

    // When every retained row has been deleted,
    // there is no retained cursor to advance to.

    if (newest === null) {
      if (
        initial ||
        history.retentionTruncated
      ) {
        await this.deliverBatch(
          socket,
          data,
          key,
          subscription,
          [],
          history.retentionTruncated,
        );
      }

      return;
    }

    if (
      history.items.length > 0 ||
      initial
    ) {
      const delivered =
        await this.deliverBatch(
          socket,
          data,
          key,
          subscription,
          history.items,
          history.retentionTruncated,
        );

      if (!delivered) {
        subscription.dirty = true;

        return;
      }
    }

    // Capture a fixed upper bound for this replay.
    // Newer commits are processed by reconciliation.

    for (
      let page = 0;
      page < MAX_REPLAY_PAGES;
      page += 1
    ) {
      if (
        !this.isSubscriptionActive(
          socket,
          data,
          key,
          subscription,
        )
      ) {
        return;
      }

      if (
        subscription.lastSent >= newest
      ) {
        return;
      }

      if (
        performance.now() - startedAt >=
        MAX_REPLAY_DURATION_MS
      ) {
        subscription.dirty = true;

        return;
      }

      if (
        !this.canDeliver(
          socket,
          data,
        )
      ) {
        subscription.dirty = true;

        return;
      }

      const currentScope =
        await this.authorize(
          socket,
          data,
          scope.projectId,
          scope.deploymentId,
        );

      if (
        !this.isSubscriptionActive(
          socket,
          data,
          key,
          subscription,
        )
      ) {
        return;
      }

      if (
        !currentScope ||
        currentScope.workspaceId !==
          scope.workspaceId
      ) {
        this.revokeSubscription(
          socket,
          data,
          key,
          subscription,
        );

        return;
      }

      const previousCursor =
        subscription.lastSent;

      const entries =
        await getEntriesInRange(
          scope,
          previousCursor + 1n,
          newest,
          LIVE_BATCH_LIMIT,
        );

      if (
        !this.isSubscriptionActive(
          socket,
          data,
          key,
          subscription,
        )
      ) {
        return;
      }

      if (entries.length === 0) {
        // The range may have been removed
        // by retention after the first snapshot.

        this.emitResync(
          socket,
          subscription,
          "HISTORY_CHANGED",
          newest,
        );

        subscription.dirty = true;

        return;
      }

      const delivered =
        await this.deliverBatch(
          socket,
          data,
          key,
          subscription,
          entries,
          false,
        );

      if (!delivered) {
        subscription.dirty = true;

        return;
      }

      if (
        subscription.lastSent <=
        previousCursor
      ) {
        subscription.dirty = true;

        return;
      }
    }

    if (
      subscription.lastSent < newest
    ) {
      subscription.dirty = true;
    }
  }

  // ==================================================
  // Redis notification handling
  // ==================================================

  /**
   * Redis payloads never authorize delivery.
   *
   * Notifications only mark matching local
   * subscriptions as needing reconciliation.
   *
   * This coalesces repeated notifications
   * instead of launching a database query
   * for every incoming Redis message.
   */
  private onNotification(
    channel: string,
    raw: string,
  ): void {
    const notification =
      parseLogNotification(raw);

    if (!notification) {
      return;
    }

    if (
      !isLogNotificationForChannel(
        this.prefix,
        channel,
        notification,
      )
    ) {
      return;
    }

    const key = subscriptionKey(
      notification.projectId,
      notification.deploymentId,
    );

    const to =
      parseCursor(
        notification.toSequence,
      );

    if (to === null) {
      return;
    }

    let changed = false;

    for (const data of
      this.socketData.values()) {
      const subscription =
        data.subscriptions.get(key);

      if (
        !subscription ||
        !subscription.active
      ) {
        continue;
      }

      if (
        !matchesLogNotificationScope(
          notification,
          subscription.scope,
        )
      ) {
        continue;
      }

      if (to > subscription.lastSent) {
        subscription.dirty = true;

        changed = true;
      }
    }

    if (changed) {
      this.requestReconcile();
    }
  }

  // ==================================================
  // Serialized reconciliation scheduler
  // ==================================================

  private scheduleReconcile(
    delayMs: number,
  ): void {
    if (
      this.closed ||
      this.reconcileTimer !== undefined ||
      this.reconcileTask !== undefined
    ) {
      return;
    }

    this.reconcileTimer =
      setTimeout(() => {
        this.reconcileTimer = undefined;

        this.runReconcileTask();
      }, delayMs);

    this.reconcileTimer.unref?.();
  }

  private requestReconcile(): void {
    if (
      this.closed ||
      this.reconcileTask !== undefined
    ) {
      return;
    }

    if (
      this.reconcileTimer !== undefined
    ) {
      clearTimeout(
        this.reconcileTimer,
      );

      this.reconcileTimer = undefined;
    }

    // Coalesce notification bursts.
    this.scheduleReconcile(100);
  }

  private runReconcileTask(): void {
    if (
      this.closed ||
      this.reconcileTask !== undefined
    ) {
      return;
    }

    const task = this.revalidateAll()
      .catch(() => {
        logger.warn(
          {
            code:
              "LOG_GATEWAY_RECONCILE_FAILED",
          },
          "Log gateway reconciliation failed",
        );
      })
      .finally(() => {
        if (
          this.reconcileTask === task
        ) {
          this.reconcileTask = undefined;
        }

        if (!this.closed) {
          this.scheduleReconcile(
            RECONCILE_INTERVAL_MS,
          );
        }
      });

    this.reconcileTask = task;
  }

  // ==================================================
  // Permission revalidation and missed-log recovery
  // ==================================================

  private async revalidateAll():
    Promise<void> {
    if (this.closed) {
      return;
    }

    const sockets = Array.from(
      this.io.sockets.sockets.values(),
    );

    if (sockets.length === 0) {
      this.reconcileOffset = 0;

      return;
    }

    const count = Math.min(
      sockets.length,
      MAX_RECONCILE_SOCKETS,
    );

    const start =
      this.reconcileOffset %
      sockets.length;

    for (
      let index = 0;
      index < count;
      index += 1
    ) {
      if (this.closed) {
        return;
      }

      const socket =
        sockets[
          (start + index) %
            sockets.length
        ];

      if (!socket) {
        continue;
      }

      const data =
        this.socketData.get(
          socket.id,
        );

      if (
        !data ||
        data.closed ||
        data.reconcileQueued
      ) {
        continue;
      }

      data.reconcileQueued = true;

      const accepted =
        this.enqueueOperation(
          socket,
          data,
          async () => {
            try {
              if (
                !(await this.validateSession(
                  socket,
                  data,
                ))
              ) {
                return;
              }

              const subscriptions =
                Array.from(
                  data.subscriptions.entries(),
                );

              for (const [
                key,
                subscription,
              ] of subscriptions) {
                if (
                  !this.isSubscriptionActive(
                    socket,
                    data,
                    key,
                    subscription,
                  )
                ) {
                  continue;
                }

                // Reconcile even without a Redis hint.
                // This recovers missed Pub/Sub messages.

                await this.replaySubscription(
                  socket,
                  data,
                  key,
                  subscription,
                );
              }
            } finally {
              data.reconcileQueued = false;
            }
          },
        );

      if (!accepted) {
        data.reconcileQueued = false;
      }
    }

    this.reconcileOffset =
      (start + count) %
      sockets.length;
  }

  // ==================================================
  // Event helpers
  // ==================================================

  private safeAck(
    ack: unknown,
    response: unknown,
  ): void {
    if (typeof ack !== "function") {
      return;
    }

    try {
      (ack as Ack)(response);
    } catch {
      // Client acknowledgement failure
      // must not crash the gateway.
    }
  }

  private emitError(
    socket: Socket,
    deploymentId: string | null,
    code: string,
    message: string,
  ): void {
    if (!socket.connected) {
      return;
    }

    socket.emit(
      socketEventNames.error,
      {
        deploymentId,

        code,

        message,
      },
    );
  }

  private emitResync(
    socket: Socket,
    subscription: Subscription,
    reason: string,
    highWatermark: bigint | null,
  ): void {
    if (!socket.connected) {
      return;
    }

    socket.emit(
      socketEventNames.resync,
      {
        deploymentId:
          subscription.scope.deploymentId,

        reason,

        highWatermark:
          highWatermark?.toString(10) ??
          null,
      },
    );
  }

  // ==================================================
  // Socket cleanup
  // ==================================================

  private cleanupSocket(
    socket: Socket,
  ): void {
    const data =
      this.socketData.get(
        socket.id,
      );

    if (!data) {
      return;
    }

    data.closed = true;

    for (const key of
      data.subscriptions.keys()) {
      this.removeSubscription(
        data,
        key,
      );
    }

    data.subscriptions.clear();

    this.socketData.delete(
      socket.id,
    );
  }

  // ==================================================
  // Gateway shutdown
  // ==================================================

  async close(): Promise<void> {
    if (this.closeTask !== undefined) {
      return this.closeTask;
    }

    this.closed = true;

    this.closeTask =
      this.performClose();

    return this.closeTask;
  }

  private async performClose():
    Promise<void> {
    if (
      this.reconcileTimer !== undefined
    ) {
      clearTimeout(
        this.reconcileTimer,
      );

      this.reconcileTimer = undefined;
    }

    this.subscriber.off(
      "pmessage",
      this.handleRedisMessage,
    );

    this.subscriber.off(
      "ready",
      this.handleRedisReady,
    );

    const activeTasks: Promise<void>[] =
      [];

    if (
      this.reconcileTask !== undefined
    ) {
      activeTasks.push(
        this.reconcileTask,
      );
    }

    for (const data of
      this.socketData.values()) {
      activeTasks.push(
        data.operationTail,
      );
    }

    for (const socket of
      this.io.sockets.sockets.values()) {
      this.cleanupSocket(socket);

      socket.disconnect(true);
    }

    try {
      await withTimeout(
        Promise.allSettled(
          activeTasks,
        ),
        SHUTDOWN_TIMEOUT_MS,
      );
    } catch {
      logger.warn(
        {
          code:
            "LOG_GATEWAY_DRAIN_TIMEOUT",
        },
        "Log gateway operations did not fully drain",
      );
    }

    try {
      await withTimeout(
        this.subscriber.punsubscribe(
          this.pattern,
        ),
        SHUTDOWN_TIMEOUT_MS,
      );
    } catch {
      logger.debug(
        {
          code:
            "LOG_GATEWAY_UNSUBSCRIBE_FAILED",
        },
        "Log gateway Redis unsubscribe incomplete",
      );
    }

    try {
      await withTimeout(
        this.io.close(),
        SHUTDOWN_TIMEOUT_MS,
      );
    } catch {
      logger.warn(
        {
          code:
            "LOG_GATEWAY_CLOSE_TIMEOUT",
        },
        "Log gateway shutdown incomplete",
      );
    }

    this.io.removeAllListeners();

    this.socketData.clear();

    this.totalSubscriptions = 0;
  }
}