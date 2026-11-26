/**
 * CloudPilot — Build Executor Interface
 *
 * Abstracts the image build backend so orchestration is not
 * coupled to the Docker CLI or Engine internals. Implementations
 * may target local development, dedicated remote builders, or
 * future cloud-based execution.
 */

import type {
  BuildArtifactType,
} from "@cloudpilot/queue";

import type { BuildLogStream } from "./build-logs.js";

// ==================================================
// Builder capabilities
// ==================================================

export interface BuilderCapabilities {
  available: boolean;

  builderType: string;

  driver: string;

  /**
   * Whether the builder can enforce per-build CPU/memory/disk
   * limits.
   */
  supportsResourceLimits: boolean;

  /**
   * Whether the builder can enforce network egress restrictions
   * for build steps.
   */
  supportsNetworkIsolation: boolean;

  /**
   * Whether the builder provides strong multi-tenant isolation
   * suitable for untrusted repository-controlled builds.
   */
  supportsStrongIsolation: boolean;
}

// ==================================================
// Build request
// ==================================================

export interface BuildExecutorRequest {
  contextDir: string;

  dockerfileRelativePath: string;

  imageTag: string;

  platform: string;

  buildArgs: Readonly<Record<string, string>>;

  target: string | null;

  timeoutMs: number;

  signal: AbortSignal;

  onLog: (stream: BuildLogStream, chunk: string) => void;
}

// ==================================================
// Build result
// ==================================================

export interface BuildExecutorResult {
  artifactType: BuildArtifactType;

  imageId: string | null;

  imageDigest: string | null;

  imageSizeBytes: number | null;

  platform: string;
}

// ==================================================
// Image inspection
// ==================================================

export interface ImageInspection {
  exists: boolean;

  imageId: string | null;

  imageSizeBytes: number | null;

  digest: string | null;
}

// ==================================================
// Executor contract
// ==================================================

export interface BuildExecutor {
  /**
   * Detect builder availability and isolation capabilities.
   */
  probe(): Promise<BuilderCapabilities>;

  /**
   * Execute a build and return verified artifact metadata.
   */
  build(
    request: BuildExecutorRequest,
  ): Promise<BuildExecutorResult>;

  /**
   * Inspect an image artifact in the builder image store.
   */
  inspectImage(
    imageTag: string,
  ): Promise<ImageInspection>;

  /**
   * Remove an attempt-owned image artifact. Best effort and
   * scoped to a single tag. Never runs global cleanup.
   */
  removeImage(imageTag: string): Promise<void>;
}