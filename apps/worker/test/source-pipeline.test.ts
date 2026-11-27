import assert from "node:assert/strict";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  lstat,
  rm,
  writeFile,
} from "node:fs/promises";

import { tmpdir } from "node:os";
import { join, relative, isAbsolute } from "node:path";

import {
  SourcePreparationError,
  type SourceFailureCode,
} from "@cloudpilot/queue";

import {
  downloadGitHubArchive,
} from "../src/source/download.ts";

import {
  WorkspaceManager,
} from "../src/source/workspace.ts";

import {
  extractTarGzSafely,
} from "../src/source/extract.ts";

// --------------------------------------------------
// Shared test configuration
// --------------------------------------------------

const sha = "a".repeat(40);

const BLOCK_SIZE = 512;

const downloadDefaults = {
  token: "test-token",
  owner: "o",
  repo: "r",
  sha,
  maxArchiveBytes: 1024,
  timeoutMs: 5000,
  maxRetries: 0,
  sleep: async () => undefined,
};

// --------------------------------------------------
// Test helpers
// --------------------------------------------------

function response(
  body: Buffer,
  init: {
    status?: number;
    headers?: Record<string, string>;
  } = {},
): Response {
  return new Response(
    new Uint8Array(body),
    {
      status: init.status ?? 200,
      headers: init.headers,
    },
  );
}

async function withTempDir(
  prefix: string,
  fn: (root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(
    join(tmpdir(), prefix),
  );

  try {
    await fn(root);
  } finally {
    await rm(root, {
      recursive: true,
      force: true,
      maxRetries: 3,
      retryDelay: 100,
    });
  }
}

function expectSourceFailure(
  expectedCode: SourceFailureCode,
): (error: unknown) => boolean {
  return (error: unknown): boolean => {
    assert.ok(
      error instanceof SourcePreparationError,
      "Expected a structured SourcePreparationError",
    );

    assert.equal(
      error.code,
      expectedCode,
    );

    return true;
  };
}

async function assertMissing(
  path: string,
): Promise<void> {
  await assert.rejects(
    lstat(path),
    { code: "ENOENT" },
  );
}

function assertContained(
  root: string,
  target: string,
): void {
  const path = relative(root, target);

  assert.ok(path.length > 0);

  assert.notEqual(path, "..");

  assert.ok(
    !path.startsWith("../") &&
      !path.startsWith("..\\"),
  );

  assert.equal(isAbsolute(path), false);
}

// --------------------------------------------------
// Test 1: GitHub archive download and integrity
// --------------------------------------------------

test(
  "downloads an archive from the initial GitHub URL",
  async () => {
    await withTempDir("cp-dl-", async (root) => {
      const body = Buffer.from("archive-bytes");

      const destinationPath = join(
        root,
        "archive.tar.gz",
      );

      const calls: Array<{
        url: string;
        authorization: string | null;
      }> = [];

      const fetchImpl: typeof fetch = async (
        url,
        init,
      ) => {
        calls.push({
          url: String(url),
          authorization: new Headers(
            init?.headers,
          ).get("authorization"),
        });

        return response(body);
      };

      const result = await downloadGitHubArchive({
        ...downloadDefaults,
        destinationPath,
        fetchImpl,
      });

      assert.equal(calls.length, 1);

      assert.equal(
        calls[0]?.url,
        `https://api.github.com/repos/o/r/tarball/${sha}`,
      );

      assert.ok(
        calls[0]?.authorization?.includes(
          downloadDefaults.token,
        ),
      );

      assert.equal(
        result.downloadedBytes,
        body.length,
      );

      const downloaded = await readFile(
        destinationPath,
      );

      assert.deepEqual(downloaded, body);
    });
  },
);

// --------------------------------------------------
// Test 2: SSRF protection
// --------------------------------------------------

test(
  "rejects redirects to unapproved hosts (SSRF protection)",
  async () => {
    const unsafeTargets = [
      "https://evil.example.com/x",
      "https://api.github.com.evil.example.com/x",
      "http://127.0.0.1/archive",
      "http://169.254.169.254/archive",
      "http://codeload.github.com/archive",
      "https://codeload.github.com:8443/archive",
      "https://user:password@codeload.github.com/archive",
    ];

    for (const target of unsafeTargets) {
      await withTempDir(
        "cp-dl-ssrf-",
        async (root) => {
          const destinationPath = join(
            root,
            "archive.tar.gz",
          );

          let calls = 0;

          const fetchImpl: typeof fetch =
            async () => {
              calls += 1;

              return response(Buffer.alloc(0), {
                status: 302,
                headers: {
                  location: target,
                },
              });
            };

          await assert.rejects(
            downloadGitHubArchive({
              ...downloadDefaults,
              destinationPath,
              fetchImpl,
            }),
            (error: unknown) => {
              assert.ok(
                error instanceof SourcePreparationError,
              );

              assert.equal(error.code, "UNSAFE_ARCHIVE");

              return true;
            },
          );

          // The unsafe redirect must never be fetched.
          assert.equal(calls, 1);

          await assertMissing(destinationPath);
        },
      );
    }
  },
);

// --------------------------------------------------
// Test 3: Redirect credential protection
// --------------------------------------------------

test(
  "follows approved codeload redirect without forwarding credentials",
  async () => {
    await withTempDir(
      "cp-dl-redirect-",
      async (root) => {
        const body = Buffer.from("tarball");

        const token = "test-secret-token";

        const destinationPath = join(
          root,
          "archive.tar.gz",
        );

        const redirectUrl =
          `https://codeload.github.com/o/r/tar.gz/${sha}`;

        const seen: Array<{
          url: string;
          authorization: string | null;
          cookie: string | null;
          redirect: RequestRedirect | undefined;
        }> = [];

        const fetchImpl: typeof fetch = async (
          url,
          init,
        ) => {
          const headers = new Headers(
            init?.headers,
          );

          const currentUrl = String(url);

          seen.push({
            url: currentUrl,
            authorization:
              headers.get("authorization"),
            cookie: headers.get("cookie"),
            redirect: init?.redirect,
          });

          if (
            new URL(currentUrl).hostname ===
            "api.github.com"
          ) {
            return response(Buffer.alloc(0), {
              status: 302,
              headers: {
                location: redirectUrl,
              },
            });
          }

          return response(body);
        };

        const result = await downloadGitHubArchive({
          ...downloadDefaults,
          token,
          destinationPath,
          fetchImpl,
        });

        assert.equal(seen.length, 2);

        assert.equal(
          result.downloadedBytes,
          body.length,
        );

        assert.ok(
          seen[0]?.authorization?.includes(token),
        );

        assert.equal(
          seen[1]?.url,
          redirectUrl,
        );

        assert.equal(
          seen[1]?.authorization,
          null,
        );

        assert.equal(
          seen[1]?.cookie,
          null,
        );

        assert.equal(
          seen[0]?.redirect,
          "manual",
        );

        assert.equal(
          seen[1]?.redirect,
          "manual",
        );

        assert.deepEqual(
          await readFile(destinationPath),
          body,
        );
      },
    );
  },
);

// --------------------------------------------------
// Test 4: Compressed archive size protection
// --------------------------------------------------

test(
  "enforces maximum compressed archive size",
  async () => {
    await withTempDir(
      "cp-dl-size-",
      async (root) => {
        const cases = [
          {
            body: Buffer.alloc(1000),
            headers: {},
          },
          {
            body: Buffer.alloc(1000),
            headers: {
              "content-length": "1000",
            },
          },
        ];

        for (const [index, fixture] of cases.entries()) {
          const destinationPath = join(
            root,
            `archive-${index}.tar.gz`,
          );

          const fetchImpl: typeof fetch =
            async () =>
              response(fixture.body, {
                headers: fixture.headers,
              });

          await assert.rejects(
            downloadGitHubArchive({
              ...downloadDefaults,
              destinationPath,
              maxArchiveBytes: 10,
              fetchImpl,
            }),
            expectSourceFailure(
              "ARCHIVE_SIZE_EXCEEDED",
            ),
          );

          // Partial archives must be removed.
          await assertMissing(destinationPath);
        }
      },
    );
  },
);

// --------------------------------------------------
// Test 5: Permanent GitHub errors must not retry
// --------------------------------------------------

test(
  "maps 404 to COMMIT_NOT_FOUND and does not retry",
  async () => {
    await withTempDir(
      "cp-dl-404-",
      async (root) => {
        let calls = 0;
        let sleeps = 0;

        const destinationPath = join(
          root,
          "archive.tar.gz",
        );

        const fetchImpl: typeof fetch =
          async () => {
            calls += 1;

            return response(Buffer.alloc(0), {
              status: 404,
            });
          };

        await assert.rejects(
          downloadGitHubArchive({
            ...downloadDefaults,
            destinationPath,
            maxRetries: 3,
            fetchImpl,
            sleep: async () => {
              sleeps += 1;
            },
          }),
          expectSourceFailure(
            "COMMIT_NOT_FOUND",
          ),
        );

        assert.equal(calls, 1);
        assert.equal(sleeps, 0);

        await assertMissing(destinationPath);
      },
    );
  },
);

// --------------------------------------------------
// Test 6: Workspace isolation and active protection
// --------------------------------------------------

test("prepared source resolves only a matching GitHub archive wrapper", async () => {
  await withTempDir("cp-source-root-", async (root) => {
    const manager = new WorkspaceManager({ root, ttlSeconds: 3600, minFreeBytes: 0 });
    const deploymentId = "11111111-1111-1111-1111-111111111111";
    const attempt = await manager.prepareAttempt(deploymentId);
    const wrapped = join(attempt.sourceDir, "owner-demo-app-aaaaaaa");
    await mkdir(wrapped);
    await writeFile(join(wrapped, "Dockerfile"), "FROM scratch\n");

    assert.equal(await manager.resolvePreparedSourceDir(deploymentId, attempt.attemptId, "Owner/demo-app", sha), wrapped);
    assert.equal(await manager.resolvePreparedSourceDir(deploymentId, attempt.attemptId, "Owner/demo-app", "b".repeat(40)), attempt.sourceDir);
    await writeFile(join(attempt.sourceDir, "README.md"), "Flat source");
    assert.equal(await manager.resolvePreparedSourceDir(deploymentId, attempt.attemptId, "Owner/demo-app", sha), attempt.sourceDir);
  });
});

test(
  "workspace manager isolates attempts and refuses to delete active workspaces",
  async () => {
    await withTempDir(
      "cp-ws-",
      async (root) => {
        let sequence = 0;

        const manager = new WorkspaceManager({
          root,
          ttlSeconds: 3600,
          minFreeBytes: 0,
          random: () => `attempt-${++sequence}`,
        });

        const deploymentId =
          "11111111-1111-1111-1111-111111111111";

        const first = await manager.prepareAttempt(
          deploymentId,
        );

        const second = await manager.prepareAttempt(
          deploymentId,
        );

        try {
          assert.notEqual(
            first.attemptId,
            second.attemptId,
          );

          assert.notEqual(
            first.sourceDir,
            second.sourceDir,
          );

          assertContained(
            manager.rootPath,
            first.sourceDir,
          );

          assertContained(
            manager.rootPath,
            second.sourceDir,
          );

          assert.equal(
            manager.isActive(first.attemptId),
            true,
          );

          assert.equal(
            manager.isActive(second.attemptId),
            true,
          );

          const firstFile = join(
            first.sourceDir,
            "first.txt",
          );

          const secondFile = join(
            second.sourceDir,
            "second.txt",
          );

          await writeFile(firstFile, "first");
          await writeFile(secondFile, "second");

          await assertMissing(
            join(first.sourceDir, "second.txt"),
          );

          await assertMissing(
            join(second.sourceDir, "first.txt"),
          );

          await assert.rejects(
            manager.cleanupAttempt(
              first.deploymentId,
              first.attemptId,
            ),
            /active workspace/u,
          );

          assert.equal(
            await readFile(firstFile, "utf8"),
            "first",
          );

          manager.release(first.attemptId);

          await manager.cleanupAttempt(
            first.deploymentId,
            first.attemptId,
          );

          assert.equal(
            manager.isActive(first.attemptId),
            false,
          );

          await assertMissing(first.sourceDir);

          // Cleaning one attempt must not affect another.
          assert.equal(
            manager.isActive(second.attemptId),
            true,
          );

          assert.equal(
            await readFile(secondFile, "utf8"),
            "second",
          );
        } finally {
          manager.release(first.attemptId);
          manager.release(second.attemptId);
        }
      },
    );
  },
);

// --------------------------------------------------
// Test 7: Atomic metadata persistence
// --------------------------------------------------

test(
  "workspace manager writes metadata atomically",
  async () => {
    await withTempDir(
      "cp-ws-meta-",
      async (root) => {
        const manager = new WorkspaceManager({
          root,
          ttlSeconds: 3600,
          minFreeBytes: 0,
        });

        const prepared =
          await manager.prepareAttempt(
            "22222222-2222-2222-2222-222222222222",
          );

        try {
          const metadata = {
            deploymentId: prepared.deploymentId,
            attemptId: prepared.attemptId,
            repositoryFullName: "o/r",
            requestedBranch: "main",
            resolvedCommitSha: sha,
            status: "READY" as const,
            downloadedBytes: 10,
            extractedBytes: 20,
            fileCount: 2,
            failureCode: null,
            createdAt: prepared.createdAt,
            completedAt: new Date().toISOString(),
          };

          await manager.writeMetadata(metadata);

          const raw: unknown = JSON.parse(
            await readFile(
              prepared.metadataPath,
              "utf8",
            ),
          );

          assert.deepEqual(raw, metadata);

          const info = await lstat(
            prepared.metadataPath,
          );

          assert.equal(info.isFile(), true);
          assert.equal(info.isSymbolicLink(), false);

          assert.ok(info.size > 0);

          // Verify a complete subsequent metadata update.
          const updatedMetadata = {
            ...metadata,
            downloadedBytes: 15,
            extractedBytes: 30,
            fileCount: 3,
            completedAt: new Date().toISOString(),
          };

          await manager.writeMetadata(
            updatedMetadata,
          );

          const updated: unknown = JSON.parse(
            await readFile(
              prepared.metadataPath,
              "utf8",
            ),
          );

          assert.deepEqual(
            updated,
            updatedMetadata,
          );
        } finally {
          manager.release(prepared.attemptId);
        }
      },
    );
  },
);

// --------------------------------------------------
// TAR fixture for decompression limit testing
// --------------------------------------------------

function buildLargeTar(
  body: Buffer,
): Buffer {
  const header = Buffer.alloc(BLOCK_SIZE);

  header.write("big.bin", 0, 100, "utf8");

  header.write("0000644\0", 100, 8, "ascii");
  header.write("0000000\0", 108, 8, "ascii");
  header.write("0000000\0", 116, 8, "ascii");

  header.write(
    body.length.toString(8).padStart(11, "0") +
      "\0",
    124,
    12,
    "ascii",
  );

  header.write(
    "00000000000\0",
    136,
    12,
    "ascii",
  );

  header.fill(32, 148, 156);

  header.write("0", 156, 1, "ascii");

  header.write("ustar\0", 257, 6, "ascii");
  header.write("00", 263, 2, "ascii");

  let checksum = 0;

  for (
    let index = 0;
    index < BLOCK_SIZE;
    index += 1
  ) {
    checksum += header[index] ?? 0;
  }

  header.write(
    checksum.toString(8).padStart(6, "0") +
      "\0 ",
    148,
    8,
    "ascii",
  );

  const padding =
    (BLOCK_SIZE - (body.length % BLOCK_SIZE)) %
    BLOCK_SIZE;

  return Buffer.concat([
    header,
    body,
    Buffer.alloc(padding),
    Buffer.alloc(BLOCK_SIZE * 2),
  ]);
}

// --------------------------------------------------
// Test 8: Gzip decompression bomb protection
// --------------------------------------------------

test(
  "gzip decompression bomb is bounded by extraction limits",
  async () => {
    await withTempDir(
      "cp-bomb-",
      async (root) => {
        const archivePath = join(
          root,
          "bomb.tar.gz",
        );

        const destinationDir = join(
          root,
          "out",
        );

        // Required by the hardened extractor.
        await mkdir(destinationDir, {
          mode: 0o700,
        });

        // Controlled, highly compressible test fixture.
        const body = Buffer.alloc(
          2 * 1024 * 1024,
          1,
        );

        const tar = buildLargeTar(body);

        const compressed = gzipSync(tar);

        assert.ok(
          compressed.length < body.length / 100,
          "Expected a highly compressible fixture",
        );

        await writeFile(
          archivePath,
          compressed,
        );

        await assert.rejects(
          extractTarGzSafely({
            archivePath,
            destinationDir,
            limits: {
              maxExtractedBytes: 1000,
              maxFileBytes: 1000,
              maxFileCount: 10,
            },
          }),
          (error: unknown) => {
            assert.ok(
              error instanceof SourcePreparationError,
            );

            assert.ok(
              error.code === "FILE_SIZE_EXCEEDED" ||
                error.code ===
                  "EXTRACTION_SIZE_EXCEEDED",
              `Unexpected failure code: ${error.code}`,
            );

            return true;
          },
        );

        // Oversized content must not reach the filesystem.
        await assertMissing(
          join(destinationDir, "big.bin"),
        );

        assert.deepEqual(
          await readdir(destinationDir),
          [],
        );
      },
    );
  },
);
