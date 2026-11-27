import assert from "node:assert/strict";
import { test } from "node:test";

import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { BuildError } from "@cloudpilot/queue";

import {
  generateImageTag,
  assertValidImageReference,
} from "../src/build/image-artifact.ts";

import {
  validateBuildPolicy,
  defaultBuildPolicy,
} from "../src/build/build-policy.ts";

import {
  sanitizeBuildLogLine,
  BuildLogCollector,
} from "../src/build/build-logs.ts";

import {
  prepareBuildContext,
} from "../src/build/build-context.ts";

import {
  validateDockerfile,
} from "../src/build/dockerfile-validator.ts";

import { runBuild } from "../src/build/build-engine.ts";
import { assertManagedImageTag } from "../src/build/buildkit-client.ts";

import type {
  BuilderCapabilities,
  BuildExecutor,
  BuildExecutorRequest,
  BuildExecutorResult,
  ImageInspection,
} from "../src/build/build-executor.ts";

// --------------------------------------------------
// Helpers
// --------------------------------------------------

const sha = "a".repeat(40);
const deploymentId = "22222222-2222-2222-2222-222222222222";

const projectId =
  "11111111-1111-1111-1111-111111111111";

async function withTempDir(
  fn: (root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "cp-build-"));

  try {
    await fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function expectBuildError(
  code: string,
): (error: unknown) => boolean {
  return (error: unknown): boolean => {
    assert.ok(error instanceof BuildError);
    assert.equal(error.code, code);
    return true;
  };
}

// --------------------------------------------------
// Mock executor
// --------------------------------------------------

class MockExecutor implements BuildExecutor {
  private built = false;

  constructor(
    private readonly options: {
      available?: boolean;
      buildError?: BuildError;
      imageExists?: boolean;
      onBuild?: (request: BuildExecutorRequest) => void;
    } = {},
  ) {}

  probe(): Promise<BuilderCapabilities> {
    return Promise.resolve({
      available: this.options.available ?? true,
      builderType: "mock",
      driver: "mock",
      supportsResourceLimits: false,
      supportsNetworkIsolation: false,
      supportsStrongIsolation: false,
    });
  }

  build(
    request: BuildExecutorRequest,
  ): Promise<BuildExecutorResult> {
    this.options.onBuild?.(request);

    if (this.options.buildError) {
      return Promise.reject(this.options.buildError);
    }

    this.built = true;
    request.onLog("stdout", "Step 1/1 : FROM scratch\n");

    return Promise.resolve({
      artifactType: "DOCKER_IMAGE_STORE",
      imageId: ("sha256:" + "b".repeat(64)),
      imageDigest: null,
      imageSizeBytes: 1024,
      platform: request.platform,
    });
  }

  inspectImage(_imageTag: string): Promise<ImageInspection> {
    return Promise.resolve({
      exists: this.built && (this.options.imageExists ?? true),
      imageId: ("sha256:" + "b".repeat(64)),
      imageSizeBytes: 1024,
      digest: null,
    });
  }

  removeImage(_imageTag: string): Promise<void> {
    return Promise.resolve();
  }
}

// --------------------------------------------------
// Image tag tests
// --------------------------------------------------

test("image tag is generated from trusted identifiers", () => {
  const tag = generateImageTag({
    projectId,
        deploymentId,
    resolvedCommitSha: sha,
    buildAttempt: 1,
  });

  assert.equal(tag, `cloudpilot/project-${projectId}:${sha}-${deploymentId}-1`);
  assert.doesNotThrow(() => assertValidImageReference(tag));
});

test("image tag rejects invalid identifiers", () => {
  assert.throws(
    () =>
      generateImageTag({
        projectId: "not-a-uuid",
        deploymentId,
        resolvedCommitSha: sha,
        buildAttempt: 1,
      }),
    expectBuildError("UNEXPECTED_BUILD_FAILURE"),
  );

  assert.throws(
    () => assertValidImageReference("UPPER/Case:Tag"),
    expectBuildError("UNEXPECTED_BUILD_FAILURE"),
  );
});

test("BuildKit accepts only canonical deployment-scoped image tags", () => {
  const project = "abcdefab-abcd-abcd-abcd-abcdefabcdef";
  const deployment = "abcdefab-abcd-abcd-abcd-123456abcdef";
  const tag = generateImageTag({
    projectId: project.toUpperCase(),
    deploymentId: deployment.toUpperCase(),
    resolvedCommitSha: sha,
    buildAttempt: 12,
  });
  assert.equal(tag, `cloudpilot/project-${project}:${sha}-${deployment}-12`);

  assert.doesNotThrow(() => assertManagedImageTag(tag));

  for (const invalid of [
    `cloudpilot/project-${project}:${sha}-12`,
    tag.replace(project, project.toUpperCase()),
    tag.replace(deployment, deployment.toUpperCase()),
    tag.replace(sha, sha.toUpperCase()),
    tag.replace(/12$/, "0"),
    tag.replace(/12$/, "9007199254740992"),
    `registry.example/${tag}`,
    "alpine:latest",
  ]) {
    assert.throws(() => assertManagedImageTag(invalid), expectBuildError("BUILD_POLICY_VIOLATION"));
  }
});

// --------------------------------------------------
// Policy tests
// --------------------------------------------------

test("build policy rejects privileged and host networking", () => {
  assert.throws(
    () =>
      validateBuildPolicy(
        "FROM scratch\nRUN --privileged echo hi",
        defaultBuildPolicy,
      ),
    expectBuildError("BUILD_POLICY_VIOLATION"),
  );

  assert.throws(
    () =>
      validateBuildPolicy(
        "FROM scratch\nRUN --network=host echo hi",
        defaultBuildPolicy,
      ),
    expectBuildError("BUILD_POLICY_VIOLATION"),
  );
});

test("build policy rejects ssh/secret mounts and remote add", () => {
  assert.throws(
    () =>
      validateBuildPolicy(
        "FROM scratch\nRUN --mount=type=ssh echo hi",
        defaultBuildPolicy,
      ),
    expectBuildError("BUILD_POLICY_VIOLATION"),
  );

  assert.throws(
    () =>
      validateBuildPolicy(
        "FROM scratch\nADD https://example.com/x /x",
        defaultBuildPolicy,
      ),
    expectBuildError("BUILD_POLICY_VIOLATION"),
  );

  assert.throws(
    () =>
      validateBuildPolicy(
        "# syntax=docker/dockerfile:1\nFROM scratch",
        defaultBuildPolicy,
      ),
    expectBuildError("BUILD_POLICY_VIOLATION"),
  );
});

test("build policy accepts a clean multi-stage Dockerfile", () => {
  assert.doesNotThrow(() =>
    validateBuildPolicy(
      "FROM node:20 AS build\nRUN npm ci\nFROM node:20\nCOPY --from=build /app /app",
      defaultBuildPolicy,
    ),
  );
});

// --------------------------------------------------
// Log sanitization tests
// --------------------------------------------------

test("log sanitization strips ANSI and redacts secrets", () => {
  const line = "\u001b[31mtoken=ghp_abcdefghijklmnopqrstuvwxyz\u001b[0m";

  const sanitized = sanitizeBuildLogLine(line);

  assert.ok(!sanitized.includes("\u001b"));
  assert.ok(!sanitized.includes("ghp_"));
  assert.ok(sanitized.includes("[REDACTED]"));
});

test("log collector enforces a byte bound", () => {
  const collector = new BuildLogCollector({ maxLogBytes: 32 });

  collector.append("stdout", "x".repeat(100));
  collector.finalize();

  assert.equal(collector.isTruncated, true);
  assert.ok(collector.sizeBytes <= 32);
});

// --------------------------------------------------
// Dockerfile validator tests
// --------------------------------------------------

test("dockerfile validator rejects a missing dockerfile", async () => {
  await withTempDir(async (root) => {
    await assert.rejects(
      validateDockerfile(join(root, "Dockerfile")),
      expectBuildError("DOCKERFILE_NOT_FOUND"),
    );
  });
});

test("dockerfile validator rejects a dockerfile without FROM", async () => {
  await withTempDir(async (root) => {
    const path = join(root, "Dockerfile");

    await writeFile(path, "RUN echo hi\n");

    await assert.rejects(
      validateDockerfile(path),
      expectBuildError("DOCKERFILE_INVALID"),
    );
  });
});

// --------------------------------------------------
// Build context tests
// --------------------------------------------------

test("build context rejects traversal and missing dockerfile", async () => {
  await withTempDir(async (root) => {
    const source = join(root, "source");

    await mkdir(source);

    await assert.rejects(
      prepareBuildContext({
        sourceDir: source,
        dockerfilePath: "../Dockerfile",
        buildContextPath: null,
        maxContextBytes: 1024,
        maxFileCount: 10,
      }),
      expectBuildError("BUILD_CONTEXT_INVALID"),
    );

    await assert.rejects(
      prepareBuildContext({
        sourceDir: source,
        dockerfilePath: null,
        buildContextPath: null,
        maxContextBytes: 1024,
        maxFileCount: 10,
      }),
      expectBuildError("DOCKERFILE_NOT_FOUND"),
    );
  });
});

test("build context enforces a size limit", async () => {
  await withTempDir(async (root) => {
    const source = join(root, "source");

    await mkdir(source);

    await writeFile(join(source, "Dockerfile"), "FROM scratch\n");

    await writeFile(join(source, "big.txt"), "x".repeat(4096));

    await assert.rejects(
      prepareBuildContext({
        sourceDir: source,
        dockerfilePath: null,
        buildContextPath: null,
        maxContextBytes: 1024,
        maxFileCount: 100,
      }),
      expectBuildError("BUILD_RESOURCE_LIMIT_EXCEEDED"),
    );
  });
});

// --------------------------------------------------
// Build engine tests
// --------------------------------------------------

test("build engine returns a verified artifact on success", async () => {
  await withTempDir(async (root) => {
    await writeFile(join(root, "Dockerfile"), "FROM scratch\n");

    const result = await runBuild({
      trustedRepository: true,
        sourceDir: root,
      projectId,
      deploymentId: "22222222-2222-2222-2222-222222222222",
      resolvedCommitSha: sha,
      buildAttempt: 1,
      dockerfilePath: null,
      buildContextPath: null,
      buildArgs: {},
      target: null,
      platform: "linux/amd64",
      limits: {
        timeoutMs: 60_000,
        maxContextBytes: 1024 * 1024,
        maxFileCount: 100,
        maxLogBytes: 1024 * 1024,
      },
      executor: new MockExecutor(),
      signal: new AbortController().signal,
    });

    assert.equal(result.imageTag, `cloudpilot/project-${projectId}:${sha}-${deploymentId}-1`);
    assert.equal(result.imageId, ("sha256:" + "b".repeat(64)));
    assert.ok(result.logEntries.length > 0);
  });
});

test("build engine fails closed when the builder is unavailable", async () => {
  await withTempDir(async (root) => {
    await writeFile(join(root, "Dockerfile"), "FROM scratch\n");

    await assert.rejects(
      runBuild({
        trustedRepository: true,
        sourceDir: root,
        projectId,
        deploymentId: "22222222-2222-2222-2222-222222222222",
        resolvedCommitSha: sha,
        buildAttempt: 1,
        dockerfilePath: null,
        buildContextPath: null,
        buildArgs: {},
        target: null,
        platform: "linux/amd64",
        limits: {
          timeoutMs: 60_000,
          maxContextBytes: 1024 * 1024,
          maxFileCount: 100,
          maxLogBytes: 1024 * 1024,
        },
        executor: new MockExecutor({ available: false }),
        signal: new AbortController().signal,
      }),
      expectBuildError("BUILDER_UNAVAILABLE"),
    );
  });
});

test("build engine surfaces policy violations", async () => {
  await withTempDir(async (root) => {
    await writeFile(
      join(root, "Dockerfile"),
      "FROM scratch\nRUN --privileged echo hi",
    );

    await assert.rejects(
      runBuild({
        trustedRepository: true,
        sourceDir: root,
        projectId,
        deploymentId: "22222222-2222-2222-2222-222222222222",
        resolvedCommitSha: sha,
        buildAttempt: 1,
        dockerfilePath: null,
        buildContextPath: null,
        buildArgs: {},
        target: null,
        platform: "linux/amd64",
        limits: {
          timeoutMs: 60_000,
          maxContextBytes: 1024 * 1024,
          maxFileCount: 100,
          maxLogBytes: 1024 * 1024,
        },
        executor: new MockExecutor(),
        signal: new AbortController().signal,
      }),
      expectBuildError("BUILD_POLICY_VIOLATION"),
    );
  });
});

test("build engine fails when the artifact is missing", async () => {
  await withTempDir(async (root) => {
    await writeFile(join(root, "Dockerfile"), "FROM scratch\n");

    await assert.rejects(
      runBuild({
        trustedRepository: true,
        sourceDir: root,
        projectId,
        deploymentId: "22222222-2222-2222-2222-222222222222",
        resolvedCommitSha: sha,
        buildAttempt: 1,
        dockerfilePath: null,
        buildContextPath: null,
        buildArgs: {},
        target: null,
        platform: "linux/amd64",
        limits: {
          timeoutMs: 60_000,
          maxContextBytes: 1024 * 1024,
          maxFileCount: 100,
          maxLogBytes: 1024 * 1024,
        },
        executor: new MockExecutor({ imageExists: false }),
        signal: new AbortController().signal,
      }),
      expectBuildError("IMAGE_VERIFICATION_FAILED"),
    );
  });
});

test("build engine cancels cooperatively", async () => {
  await withTempDir(async (root) => {
    await writeFile(join(root, "Dockerfile"), "FROM scratch\n");

    const controller = new AbortController();

    controller.abort();

    await assert.rejects(
      runBuild({
        trustedRepository: true,
        sourceDir: root,
        projectId,
        deploymentId: "22222222-2222-2222-2222-222222222222",
        resolvedCommitSha: sha,
        buildAttempt: 1,
        dockerfilePath: null,
        buildContextPath: null,
        buildArgs: {},
        target: null,
        platform: "linux/amd64",
        limits: {
          timeoutMs: 60_000,
          maxContextBytes: 1024 * 1024,
          maxFileCount: 100,
          maxLogBytes: 1024 * 1024,
        },
        executor: new MockExecutor(),
        signal: controller.signal,
      }),
      expectBuildError("BUILD_CANCELLED"),
    );
  });
});
