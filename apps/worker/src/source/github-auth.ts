/**
 * CloudPilot — Worker GitHub App Authentication
 *
 * Provides GitHub App initialization, installation
 * verification, scoped access tokens, token revocation,
 * and sanitized GitHub API error classification.
 *
 * Private keys and installation tokens must never
 * appear in logs, queue payloads, or deployment records.
 */

import { createPrivateKey } from "node:crypto";

import { open } from "node:fs/promises";

import { resolve } from "node:path";

import { App } from "@octokit/app";

import { Octokit } from "@octokit/rest";

import {
  SourcePreparationError,
} from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const GITHUB_API_VERSION = "2022-11-28";

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

const MAX_PRIVATE_KEY_BYTES = 64 * 1024;

const MIN_TOKEN_VALIDITY_MS = 30_000;

const APP_ID_PATTERN = /^[1-9][0-9]*$/u;

const REPOSITORY_PATTERN =
  /^[A-Za-z0-9-]{1,39}\/[A-Za-z0-9_.-]{1,100}$/u;

// ==================================================
// Interfaces
// ==================================================

export interface WorkerGitHubConfig {
  appId: string;

  privateKeyPath: string;

  requestTimeoutMs?: number;
}

export interface InstallationSnapshot {
  id: number;

  suspendedAt: string | null;

  repositorySelection: "all" | "selected";
}

export interface MintedInstallationToken {
  token: string;

  expiresAt: string;
}

export interface InstallationTokenScope {
  repositoryFullName: string;

  repositoryId?: bigint;

  signal?: AbortSignal;
}

type GitHubApp = App<{
  Octokit: typeof Octokit;
}>;

// ==================================================
// Validation utilities
// ==================================================

function installationNumber(
  value: bigint,
): number {
  const numeric = Number(value);

  if (
    typeof value !== "bigint" ||
    !Number.isSafeInteger(numeric) ||
    numeric <= 0
  ) {
    throw new SourcePreparationError(
      "INSTALLATION_UNAVAILABLE",
      "Invalid installation identifier.",
    );
  }

  return numeric;
}

function repositoryNumber(
  value: bigint,
): number {
  const numeric = Number(value);

  if (
    typeof value !== "bigint" ||
    !Number.isSafeInteger(numeric) ||
    numeric <= 0
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Invalid repository identifier.",
    );
  }

  return numeric;
}

function parseRepositoryFullName(
  fullName: string,
): {
  owner: string;
  repo: string;
} {
  if (
    typeof fullName !== "string" ||
    !REPOSITORY_PATTERN.test(fullName)
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Invalid repository identity.",
    );
  }

  const [owner, repo] = fullName.split("/");

  if (
    !owner ||
    !repo ||
    owner.startsWith("-") ||
    owner.endsWith("-") ||
    repo === "." ||
    repo === ".."
  ) {
    throw new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "Invalid repository identity.",
    );
  }

  return {
    owner,
    repo,
  };
}

function assertNotCancelled(
  signal?: AbortSignal,
): void {
  if (signal?.aborted) {
    throw new SourcePreparationError(
      "SOURCE_PREPARATION_CANCELLED",
      "GitHub authentication was cancelled.",
    );
  }
}

function isRecord(
  value: unknown,
): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null
  );
}

// ==================================================
// GitHub rate-limit classification
// ==================================================

function getResponseHeader(
  error: unknown,
  name: string,
): string | undefined {
  if (!isRecord(error)) {
    return undefined;
  }

  const response = error.response;

  if (!isRecord(response)) {
    return undefined;
  }

  const headers = response.headers;

  if (!isRecord(headers)) {
    return undefined;
  }

  for (const [key, value] of Object.entries(headers)) {
    if (
      key.toLowerCase() === name.toLowerCase() &&
      (
        typeof value === "string" ||
        typeof value === "number"
      )
    ) {
      return String(value);
    }
  }

  return undefined;
}

function isGitHubRateLimited(
  error: unknown,
  status: number,
): boolean {
  if (status === 429) {
    return true;
  }

  if (status !== 403) {
    return false;
  }

  const remaining = getResponseHeader(
    error,
    "x-ratelimit-remaining",
  );

  const retryAfter = getResponseHeader(
    error,
    "retry-after",
  );

  if (
    remaining === "0" ||
    retryAfter !== undefined
  ) {
    return true;
  }

  // Secondary rate limits may not provide a
  // Retry-After header.

  if (isRecord(error)) {
    const response = error.response;

    if (isRecord(response)) {
      const data = response.data;

      if (
        isRecord(data) &&
        typeof data.message === "string" &&
        /rate limit|abuse detection/iu.test(data.message)
      ) {
        return true;
      }
    }

    if (
      typeof error.message === "string" &&
      /rate limit|abuse detection/iu.test(error.message)
    ) {
      return true;
    }
  }

  return false;
}

// ==================================================
// Sanitized GitHub error mapping
// ==================================================

export function mapGitHubAuthError(
  error: unknown,
  signal?: AbortSignal,
): SourcePreparationError {
  if (error instanceof SourcePreparationError) {
    return error;
  }

  if (signal?.aborted) {
    return new SourcePreparationError(
      "SOURCE_PREPARATION_CANCELLED",
      "GitHub authentication was cancelled.",
    );
  }

  const status =
    isRecord(error) &&
    typeof error.status === "number"
      ? error.status
      : undefined;

  if (
    status !== undefined &&
    isGitHubRateLimited(error, status)
  ) {
    return new SourcePreparationError(
      "GITHUB_RATE_LIMITED",
      "GitHub API rate limit exceeded.",
    );
  }

  if (status === 401) {
    return new SourcePreparationError(
      "INSTALLATION_UNAVAILABLE",
      "GitHub App authentication failed.",
    );
  }

  if (status === 403) {
    return new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "GitHub denied repository access.",
    );
  }

  if (status === 404) {
    return new SourcePreparationError(
      "INSTALLATION_UNAVAILABLE",
      "GitHub installation is unavailable.",
    );
  }

  if (status === 422) {
    return new SourcePreparationError(
      "REPOSITORY_ACCESS_DENIED",
      "GitHub rejected the requested token scope.",
    );
  }

  if (
    status === 408 ||
    status === 0 ||
    (
      status !== undefined &&
      status >= 500
    )
  ) {
    return new SourcePreparationError(
      "GITHUB_UNAVAILABLE",
      "GitHub is temporarily unavailable.",
    );
  }

  if (
    error instanceof Error &&
    (
      error.name === "AbortError" ||
      error.name === "TimeoutError" ||
      error instanceof TypeError
    )
  ) {
    return new SourcePreparationError(
      "GITHUB_UNAVAILABLE",
      "GitHub authentication request was interrupted.",
    );
  }

  if (isRecord(error)) {
    const code = error.code;

    if (
      typeof code === "string" &&
      [
        "ECONNRESET",
        "ECONNREFUSED",
        "ETIMEDOUT",
        "EAI_AGAIN",
        "ENETUNREACH",
        "EHOSTUNREACH",
        "UND_ERR_CONNECT_TIMEOUT",
      ].includes(code)
    ) {
      return new SourcePreparationError(
        "GITHUB_UNAVAILABLE",
        "GitHub network connection is unavailable.",
      );
    }
  }

  // Unknown errors must not automatically become
  // retryable infrastructure failures.

  return new SourcePreparationError(
    "UNEXPECTED_FAILURE",
    "GitHub authentication operation failed.",
  );
}

// ==================================================
// Bounded private key loading
// ==================================================

async function loadPrivateKey(
  privateKeyPath: string,
): Promise<string> {
  const keyPath = resolve(privateKeyPath);

  const handle = await open(keyPath, "r");

  try {
    const info = await handle.stat();

    if (
      !info.isFile() ||
      info.size <= 0 ||
      info.size > MAX_PRIVATE_KEY_BYTES
    ) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",
        "GitHub App private key file is invalid.",
      );
    }

    const buffer = Buffer.alloc(
      MAX_PRIVATE_KEY_BYTES + 1,
    );

    let total = 0;

    try {
      while (total < buffer.length) {
        const { bytesRead } = await handle.read(
          buffer,
          total,
          buffer.length - total,
          total,
        );

        if (bytesRead === 0) {
          break;
        }

        total += bytesRead;
      }

      if (
        total === 0 ||
        total > MAX_PRIVATE_KEY_BYTES
      ) {
        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub App private key size is invalid.",
        );
      }

      const privateKey = buffer
        .subarray(0, total)
        .toString("utf8");

      const parsed = createPrivateKey(privateKey);

      if (
        parsed.type !== "private" ||
        parsed.asymmetricKeyType !== "rsa"
      ) {
        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub App private key is invalid.",
        );
      }

      const modulusLength =
        parsed.asymmetricKeyDetails?.modulusLength;

      if (
        modulusLength !== undefined &&
        modulusLength < 2048
      ) {
        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub App private key strength is insufficient.",
        );
      }

      return privateKey;
    } finally {
      buffer.fill(0);
    }
  } finally {
    await handle.close();
  }
}

// ==================================================
// GitHub authentication service
// ==================================================

export class WorkerGitHubAuth {
  private appPromise:
    | Promise<GitHubApp>
    | null = null;

  private readonly requestTimeoutMs: number;

  constructor(
    private readonly config: WorkerGitHubConfig,
  ) {
    if (
      typeof config.appId !== "string" ||
      !APP_ID_PATTERN.test(config.appId)
    ) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",
        "GitHub App identifier is invalid.",
      );
    }

    if (
      typeof config.privateKeyPath !== "string" ||
      config.privateKeyPath.trim().length === 0
    ) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",
        "GitHub App private key is not configured.",
      );
    }

    this.requestTimeoutMs =
      config.requestTimeoutMs ??
      DEFAULT_REQUEST_TIMEOUT_MS;

    if (
      !Number.isSafeInteger(this.requestTimeoutMs) ||
      this.requestTimeoutMs <= 0 ||
      this.requestTimeoutMs > 300_000
    ) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",
        "GitHub authentication timeout is invalid.",
      );
    }
  }

  // ================================================
  // Request cancellation and timeout
  // ================================================

  private requestSignal(
    signal?: AbortSignal,
  ): AbortSignal {
    const timeout = AbortSignal.timeout(
      this.requestTimeoutMs,
    );

    return signal
      ? AbortSignal.any([signal, timeout])
      : timeout;
  }

  private requestHeaders() {
    return {
      accept: "application/vnd.github+json",

      "x-github-api-version":
        GITHUB_API_VERSION,
    };
  }

  // ================================================
  // Lazy GitHub App initialization
  // ================================================

  private async getApp(): Promise<GitHubApp> {
    if (!this.appPromise) {
      this.appPromise = (
        async () => {
          const privateKey = await loadPrivateKey(
            this.config.privateKeyPath,
          );

          return new App<{
            Octokit: typeof Octokit;
          }>({
            appId: this.config.appId,

            privateKey,

            Octokit,
          });
        }
      )().catch((error: unknown) => {
        // Allow a later initialization attempt after
        // a configuration or filesystem failure.

        this.appPromise = null;

        if (error instanceof SourcePreparationError) {
          throw error;
        }

        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub App could not be initialized.",
        );
      });
    }

    return this.appPromise;
  }

  // ================================================
  // Installation verification
  // ================================================

  async verifyInstallation(
    installationId: bigint,
    signal?: AbortSignal,
  ): Promise<InstallationSnapshot> {
    assertNotCancelled(signal);

    const id = installationNumber(
      installationId,
    );

    try {
      const app = await this.getApp();

      assertNotCancelled(signal);

      const response = await app.octokit.request(
        "GET /app/installations/{installation_id}",
        {
          installation_id: id,

          headers: this.requestHeaders(),

          request: {
            signal: this.requestSignal(signal),
          },
        },
      );

      assertNotCancelled(signal);

      const installation = response.data;

      if (installation.id !== id) {
        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub installation identity mismatch.",
        );
      }

      if (installation.suspended_at) {
        throw new SourcePreparationError(
          "INSTALLATION_SUSPENDED",
          "GitHub installation is suspended.",
        );
      }

      if (
        installation.repository_selection !== "all" &&
        installation.repository_selection !== "selected"
      ) {
        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub installation repository selection is invalid.",
        );
      }

      return {
        id: installation.id,

        suspendedAt: null,

        repositorySelection:
          installation.repository_selection,
      };
    } catch (error) {
      throw mapGitHubAuthError(
        error,
        signal,
      );
    }
  }

  // ================================================
  // Repository installation verification
  // ================================================

  async verifyRepositoryInstallation(
    installationId: bigint,
    repositoryFullName: string,
    signal?: AbortSignal,
  ): Promise<void> {
    assertNotCancelled(signal);

    const id = installationNumber(
      installationId,
    );

    const { owner, repo } =
      parseRepositoryFullName(
        repositoryFullName,
      );

    try {
      const app = await this.getApp();

      assertNotCancelled(signal);

      const response = await app.octokit.request(
        "GET /repos/{owner}/{repo}/installation",
        {
          owner,

          repo,

          headers: this.requestHeaders(),

          request: {
            signal: this.requestSignal(signal),
          },
        },
      );

      assertNotCancelled(signal);

      if (response.data.id !== id) {
        throw new SourcePreparationError(
          "REPOSITORY_ACCESS_DENIED",
          "Repository does not belong to the authorized installation.",
        );
      }

      if (response.data.suspended_at) {
        throw new SourcePreparationError(
          "INSTALLATION_SUSPENDED",
          "GitHub installation is suspended.",
        );
      }
    } catch (error) {
      if (
        isRecord(error) &&
        error.status === 404
      ) {
        throw new SourcePreparationError(
          "REPOSITORY_ACCESS_DENIED",
          "Repository installation access is unavailable.",
        );
      }

      throw mapGitHubAuthError(
        error,
        signal,
      );
    }
  }

  // ================================================
  // Installation token generation
  // ================================================

  async mintInstallationToken(
    installationId: bigint,
    scope?: InstallationTokenScope,
  ): Promise<MintedInstallationToken> {
    const signal = scope?.signal;

    assertNotCancelled(signal);

    const id = installationNumber(
      installationId,
    );

    let mintedToken: string | undefined;

    try {
      const app = await this.getApp();

      let repositoryScope:
        | { repositories: string[] }
        | { repository_ids: number[] }
        | Record<string, never> = {};

      if (scope) {
        const { repo } =
          parseRepositoryFullName(
            scope.repositoryFullName,
          );

        // Prevent a repository associated with another
        // installation from being selected.

        await this.verifyRepositoryInstallation(
          installationId,
          scope.repositoryFullName,
          signal,
        );

        repositoryScope =
          scope.repositoryId !== undefined
            ? {
                repository_ids: [
                  repositoryNumber(
                    scope.repositoryId,
                  ),
                ],
              }
            : {
                repositories: [repo],
              };
      }

      assertNotCancelled(signal);

      const response = await app.octokit.request(
        "POST /app/installations/{installation_id}/access_tokens",
        {
          installation_id: id,

          ...repositoryScope,

          permissions: {
            contents: "read",
            metadata: "read",
          },

          headers: this.requestHeaders(),

          request: {
            signal: this.requestSignal(signal),
          },
        },
      );

      const token = response.data.token;

      const expiresAt = response.data.expires_at;

      if (
        typeof token !== "string" ||
        token.length === 0 ||
        /\s/u.test(token)
      ) {
        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub returned an invalid installation token.",
        );
      }

      mintedToken = token;

      assertNotCancelled(signal);

      const expiration = Date.parse(expiresAt);

      if (
        !Number.isFinite(expiration) ||
        expiration <=
          Date.now() + MIN_TOKEN_VALIDITY_MS
      ) {
        throw new SourcePreparationError(
          "INSTALLATION_UNAVAILABLE",
          "GitHub installation token expiration is invalid.",
        );
      }

      if (
        response.data.permissions?.contents !== "read"
      ) {
        throw new SourcePreparationError(
          "REPOSITORY_ACCESS_DENIED",
          "GitHub token contents permission is invalid.",
        );
      }

      const hasWritePermission = Object.values(
        response.data.permissions,
      ).some((permission) => permission === "write" || permission === "admin");

      if (hasWritePermission) {
        throw new SourcePreparationError(
          "REPOSITORY_ACCESS_DENIED",
          "GitHub token has unexpected write permissions.",
        );
      }

      // Verify the repository scope returned by GitHub.
      // Missing scope evidence fails closed.

      if (scope) {
        const repositories =
          response.data.repositories;

        if (
          !Array.isArray(repositories) ||
          repositories.length !== 1 ||
          repositories[0]?.full_name.toLowerCase() !==
            scope.repositoryFullName.toLowerCase()
        ) {
          throw new SourcePreparationError(
            "REPOSITORY_ACCESS_DENIED",
            "GitHub token repository scope mismatch.",
          );
        }

        if (
          scope.repositoryId !== undefined &&
          repositories[0]?.id !==
            repositoryNumber(scope.repositoryId)
        ) {
          throw new SourcePreparationError(
            "REPOSITORY_ACCESS_DENIED",
            "GitHub token repository identity mismatch.",
          );
        }
      }

      return {
        token,

        expiresAt,
      };
    } catch (error) {
      // If a token was issued but failed validation,
      // attempt revocation without exposing its value.

      if (mintedToken) {
        await this.revokeInstallationToken(
          mintedToken,
        ).catch(() => undefined);
      }

      throw mapGitHubAuthError(
        error,
        signal,
      );
    }
  }

  // ================================================
  // Token revocation
  // ================================================

  async revokeInstallationToken(
    token: string,
  ): Promise<void> {
    if (
      typeof token !== "string" ||
      token.length === 0 ||
      /\s/u.test(token)
    ) {
      throw new SourcePreparationError(
        "INSTALLATION_UNAVAILABLE",
        "Invalid installation token.",
      );
    }

    try {
      // Use an independent timeout so cancellation
      // of source preparation does not prevent cleanup.

      const client = new Octokit({
        auth: token,
      });

      await client.request(
        "DELETE /installation/token",
        {
          headers: this.requestHeaders(),

          request: {
            signal: this.requestSignal(),
          },
        },
      );
    } catch (error) {
      // An unauthorized token may already be invalid,
      // expired, or revoked.

      if (
        isRecord(error) &&
        error.status === 401
      ) {
        return;
      }

      throw mapGitHubAuthError(error);
    }
  }
}