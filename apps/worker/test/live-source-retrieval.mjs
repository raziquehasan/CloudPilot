/**
 * CloudPilot — Live GitHub Source Retrieval Smoke Test
 *
 * Verifies authorized GitHub source retrieval, immutable
 * commit binding, archive extraction, content integrity,
 * workspace isolation, and cleanup.
 *
 * No repository-controlled code is executed.
 */

import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";

import {
  mkdtemp,
  readFile,
  readdir,
  realpath,
  lstat,
  rm,
} from "node:fs/promises";

import { tmpdir } from "node:os";

import {
  join,
  relative,
  isAbsolute,
  sep,
} from "node:path";

import { performance } from "node:perf_hooks";

import {
  prisma,
} from "../../../packages/database/dist/index.js";

import {
  workerEnv,
} from "../dist/config/env.js";

import {
  WorkerGitHubAuth,
} from "../dist/source/github-auth.js";

import {
  WorkspaceManager,
} from "../dist/source/workspace.js";

import {
  prepareSource,
} from "../dist/source/prepare.js";

// ==================================================
// Test configuration
// ==================================================

const EXPECTED_REPOSITORY =
  process.env.LIVE_SMOKE_REPOSITORY ??
  "raziquehasan/CloudPilot";

const EXPECTED_BRANCH =
  process.env.LIVE_SMOKE_BRANCH ?? "main";

const EXPECTED_PACKAGE_NAME =
  process.env.LIVE_SMOKE_PACKAGE_NAME ?? "cloudpilot";

const EXPECTED_WORKSPACE_ID =
  process.env.LIVE_SMOKE_WORKSPACE_ID;

const GITHUB_API = "https://api.github.com";

const API_TIMEOUT_MS = 30_000;

const MAX_VERIFICATION_BYTES = 1024 * 1024;

const COMMIT_SHA_PATTERN = /^[0-9a-f]{40}$/u;

const REPOSITORY_PATTERN =
  /^[A-Za-z0-9-]+\/[A-Za-z0-9_.-]+$/u;

const deploymentId = randomUUID();

const startedAt = performance.now();

let root;
let passed = false;
let stage = "INITIALIZATION";

// ==================================================
// Utilities
// ==================================================

function sha256(content) {
  return createHash("sha256")
    .update(content)
    .digest("hex");
}

function assertContainedPath(parent, child) {
  const difference = relative(parent, child);

  assert(
    difference !== "" &&
      difference !== ".." &&
      !difference.startsWith(`..${sep}`) &&
      !isAbsolute(difference),
    "Source path must remain inside the test workspace",
  );
}

function validateRepository(fullName) {
  assert.equal(typeof fullName, "string");

  assert.match(
    fullName,
    REPOSITORY_PATTERN,
    "Invalid repository identifier",
  );

  const [owner, repo] = fullName.split("/");

  assert(owner && repo);

  assert(
    repo !== "." && repo !== "..",
    "Invalid repository name",
  );

  return {
    owner,
    repo,
  };
}

function validateBranch(branch) {
  assert.equal(typeof branch, "string");

  assert(
    branch.length > 0 &&
      branch.length <= 255 &&
      branch.trim() === branch &&
      !/[\u0000-\u001f\u007f]/u.test(branch),
    "Invalid branch reference",
  );

  return branch;
}

// ==================================================
// Authenticated GitHub API request
// ==================================================

async function githubRequest(
  pathname,
  token,
  accept = "application/vnd.github+json",
) {
  const url = new URL(pathname, GITHUB_API);

  assert.equal(
    url.origin,
    GITHUB_API,
    "Unexpected GitHub API origin",
  );

  assert(
    !url.username && !url.password,
    "Embedded URL credentials are not allowed",
  );

  const response = await fetch(url, {
    method: "GET",

    redirect: "error",

    signal: AbortSignal.timeout(API_TIMEOUT_MS),

    headers: {
      Accept: accept,
      Authorization: `Bearer ${token}`,
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "CloudPilot-LiveSmoke",
    },
  });

  if (!response.ok) {
    await response.body?.cancel().catch(() => {});

    throw new Error(
      `GitHub verification request failed: HTTP ${response.status}`,
    );
  }

  return response;
}

// ==================================================
// Bounded response reader
// ==================================================

async function readBoundedResponse(
  response,
  maxBytes,
) {
  assert(
    response.body,
    "Expected a GitHub response body",
  );

  const reader = response.body.getReader();

  const chunks = [];

  let totalBytes = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        break;
      }

      totalBytes += value.byteLength;

      assert(
        totalBytes <= maxBytes,
        "GitHub verification response exceeded size limit",
      );

      chunks.push(Buffer.from(value));
    }

    return Buffer.concat(chunks, totalBytes);
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

// ==================================================
// Locate extracted package.json
// ==================================================

async function locatePackageJson(sourceDir) {
  const directPath = join(
    sourceDir,
    "package.json",
  );

  try {
    const stats = await lstat(directPath);

    assert(
      stats.isFile() && !stats.isSymbolicLink(),
      "Expected a regular package.json file",
    );

    return directPath;
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }

  // GitHub archives may retain a single root directory.
  const entries = await readdir(sourceDir, {
    withFileTypes: true,
  });

  assert.equal(
    entries.length,
    1,
    "Unexpected archive root structure",
  );

  const [entry] = entries;

  assert(
    entry.isDirectory() &&
      !entry.isSymbolicLink(),
    "Expected a regular archive root directory",
  );

  const nestedPath = join(
    sourceDir,
    entry.name,
    "package.json",
  );

  const stats = await lstat(nestedPath);

  assert(
    stats.isFile() && !stats.isSymbolicLink(),
    "Expected a regular package.json file",
  );

  return nestedPath;
}

// ==================================================
// Main smoke test
// ==================================================

try {
  stage = "CONFIGURATION_VALIDATION";

  assert(
    workerEnv.GITHUB_APP_ID &&
      workerEnv.GITHUB_APP_PRIVATE_KEY_PATH,
    "GitHub App must be configured",
  );

  validateRepository(EXPECTED_REPOSITORY);
  validateBranch(EXPECTED_BRANCH);

  // ------------------------------------------------
  // Select the explicitly authorized repository
  // ------------------------------------------------

  stage = "REPOSITORY_LOOKUP";

  const repositories = await prisma.repository.findMany({
    where: {
      provider: "GITHUB",
      fullName: EXPECTED_REPOSITORY,

      ...(EXPECTED_WORKSPACE_ID
        ? { workspaceId: EXPECTED_WORKSPACE_ID }
        : {}),
    },

    select: {
      fullName: true,
      selectedBranch: true,
      defaultBranch: true,

      installation: {
        select: {
          installationId: true,
          status: true,
        },
      },
    },

    take: 2,
  });

  assert.equal(
    repositories.length,
    1,
    "Expected one matching linked repository; specify LIVE_SMOKE_WORKSPACE_ID if ambiguous",
  );

  const [linked] = repositories;

  assert(
    linked.fullName && linked.installation,
    "Expected an authorized linked repository",
  );

  assert.equal(
    linked.fullName,
    EXPECTED_REPOSITORY,
    "Unexpected repository selected",
  );

  assert.equal(
    linked.installation.status,
    "ACTIVE",
    "GitHub installation must be active",
  );

  const { owner, repo } =
    validateRepository(linked.fullName);

  const branch = validateBranch(EXPECTED_BRANCH);

  const repositoryPath =
    `/repos/${encodeURIComponent(owner)}` +
    `/${encodeURIComponent(repo)}`;

  // ------------------------------------------------
  // Verify GitHub installation
  // ------------------------------------------------

  stage = "INSTALLATION_VERIFICATION";

  const auth = new WorkerGitHubAuth({
    appId: workerEnv.GITHUB_APP_ID,

    privateKeyPath:
      workerEnv.GITHUB_APP_PRIVATE_KEY_PATH,
  });

  await auth.verifyInstallation(
    linked.installation.installationId,
  );

  // ------------------------------------------------
  // Mint temporary installation token
  // ------------------------------------------------

  stage = "TOKEN_GENERATION";

  const minted = await auth.mintInstallationToken(
    linked.installation.installationId,
  );

  assert(
    typeof minted.token === "string" &&
      minted.token.length > 0,
    "Expected an installation access token",
  );

  // ------------------------------------------------
  // Verify repository identity
  // ------------------------------------------------

  stage = "REPOSITORY_VERIFICATION";

  const repositoryResponse = await githubRequest(
    repositoryPath,
    minted.token,
  );

  const repository = JSON.parse(
    (
      await readBoundedResponse(
        repositoryResponse,
        MAX_VERIFICATION_BYTES,
      )
    ).toString("utf8"),
  );

  assert.equal(
    repository.full_name?.toLowerCase(),
    EXPECTED_REPOSITORY.toLowerCase(),
    "GitHub repository identity mismatch",
  );

  // ------------------------------------------------
  // Resolve immutable commit SHA
  // ------------------------------------------------

  stage = "COMMIT_RESOLUTION";

  const commitResponse = await githubRequest(
    `${repositoryPath}/commits/${encodeURIComponent(branch)}`,
    minted.token,
  );

  const commit = JSON.parse(
    (
      await readBoundedResponse(
        commitResponse,
        MAX_VERIFICATION_BYTES,
      )
    ).toString("utf8"),
  );

  const sha = commit.sha;

  assert.equal(typeof sha, "string");

  assert.match(
    sha,
    COMMIT_SHA_PATTERN,
    "Resolved commit must be a full SHA",
  );

  // ------------------------------------------------
  // Create isolated test workspace
  // ------------------------------------------------

  stage = "WORKSPACE_CREATION";

  root = await mkdtemp(
    join(tmpdir(), "cp-live-"),
  );

  const workspace = new WorkspaceManager({
    root,
    ttlSeconds: 3600,
    minFreeBytes: workerEnv.SOURCE_MIN_FREE_BYTES,
  });

  // ------------------------------------------------
  // Retrieve and extract exact repository source
  // ------------------------------------------------

  stage = "SOURCE_PREPARATION";

  const result = await prepareSource({
    deploymentId,

    repositoryFullName: linked.fullName,

    requestedBranch: branch,

    resolvedCommitSha: sha,

    installationId:
      linked.installation.installationId,

    auth,

    workspace,

    limits: {
      maxArchiveBytes:
        workerEnv.SOURCE_MAX_ARCHIVE_BYTES,

      maxExtractedBytes:
        workerEnv.SOURCE_MAX_EXTRACTED_BYTES,

      maxFileBytes:
        workerEnv.SOURCE_MAX_FILE_BYTES,

      maxFileCount:
        workerEnv.SOURCE_MAX_FILE_COUNT,

      timeoutMs:
        workerEnv.SOURCE_DOWNLOAD_TIMEOUT_MS,

      maxRetries:
        workerEnv.SOURCE_MAX_RETRIES,
    },

    signal: AbortSignal.timeout(300_000),

    shouldCancel: () => false,
  });

  // ------------------------------------------------
  // Verify retrieval metrics
  // ------------------------------------------------

  stage = "METRICS_VERIFICATION";

  assert(
    Number.isSafeInteger(result.fileCount) &&
      result.fileCount > 0,
    "Expected extracted source files",
  );

  assert(
    result.downloadedBytes > 0 &&
      result.extractedBytes > 0,
    "Expected positive retrieval byte counts",
  );

  assert(
    result.downloadedBytes <=
      workerEnv.SOURCE_MAX_ARCHIVE_BYTES,
    "Downloaded archive exceeded configured limit",
  );

  assert(
    result.extractedBytes <=
      workerEnv.SOURCE_MAX_EXTRACTED_BYTES,
    "Extracted source exceeded configured limit",
  );

  assert(
    result.fileCount <=
      workerEnv.SOURCE_MAX_FILE_COUNT,
    "Extracted file count exceeded configured limit",
  );

  // ------------------------------------------------
  // Verify filesystem containment
  // ------------------------------------------------

  stage = "WORKSPACE_VERIFICATION";

  const canonicalRoot = await realpath(root);

  const canonicalSource = await realpath(
    result.sourceDir,
  );

  assertContainedPath(
    canonicalRoot,
    canonicalSource,
  );

  const sourceStats = await lstat(
    result.sourceDir,
  );

  assert(
    sourceStats.isDirectory() &&
      !sourceStats.isSymbolicLink(),
    "Expected an isolated source directory",
  );

  // ------------------------------------------------
  // Locate and validate package.json
  // ------------------------------------------------

  stage = "PACKAGE_VERIFICATION";

  const packagePath = await locatePackageJson(
    canonicalSource,
  );

  const canonicalPackage = await realpath(
    packagePath,
  );

  assertContainedPath(
    canonicalSource,
    canonicalPackage,
  );

  const packageStats = await lstat(
    canonicalPackage,
  );

  assert(
    packageStats.size <= MAX_VERIFICATION_BYTES,
    "Local verification file exceeds size limit",
  );

  const localPackage = await readFile(
    canonicalPackage,
  );

  const packageJson = JSON.parse(
    localPackage.toString("utf8"),
  );

  assert.equal(
    packageJson.name,
    EXPECTED_PACKAGE_NAME,
    "Retrieved package identity mismatch",
  );

  // ------------------------------------------------
  // Verify exact content against immutable commit
  // ------------------------------------------------

  stage = "CONTENT_INTEGRITY_VERIFICATION";

  const verificationToken =
    await auth.mintInstallationToken(
      linked.installation.installationId,
    );

  const remotePackageResponse =
    await githubRequest(
      `${repositoryPath}/contents/package.json?ref=${encodeURIComponent(sha)}`,

      verificationToken.token,

      "application/vnd.github.raw+json",
    );

  const remotePackage = await readBoundedResponse(
    remotePackageResponse,
    MAX_VERIFICATION_BYTES,
  );

  const localHash = sha256(localPackage);

  const remoteHash = sha256(remotePackage);

  assert.equal(
    localHash,
    remoteHash,
    "Extracted package.json SHA-256 mismatch",
  );

  assert(
    localPackage.equals(remotePackage),
    "Extracted package.json differs from immutable commit",
  );

  // ------------------------------------------------
  // Verify persisted workspace metadata
  // ------------------------------------------------

  stage = "METADATA_VERIFICATION";

  const metadata = await workspace.readMetadata(
    deploymentId,
    result.attemptId,
  );

  assert(
    metadata,
    "Expected persisted source metadata",
  );

  assert.equal(
    metadata.resolvedCommitSha,
    sha,
    "Metadata immutable commit mismatch",
  );

  assert.equal(
    metadata.status,
    "READY",
    "Source metadata must be READY",
  );

  // ------------------------------------------------
  // Successful verification
  // ------------------------------------------------

  stage = "VERIFICATION_COMPLETE";

  console.log(
    JSON.stringify(
      {
        status: "SOURCE_VERIFIED",

        repository: linked.fullName,

        branch,

        commitSha: sha,

        downloadedBytes:
          result.downloadedBytes,

        extractedBytes:
          result.extractedBytes,

        fileCount: result.fileCount,

        verifiedPackage:
          packageJson.name,

        contentIntegrity: "VERIFIED",

        sha256: localHash,

        workspaceIsolation: "VERIFIED",

        metadataStatus: metadata.status,

        immutableCommitBinding: "VERIFIED",

        repositoryCodeExecuted: false,

        durationMs: Math.round(
          performance.now() - startedAt,
        ),
      },

      null,

      2,
    ),
  );

  passed = true;
} catch (error) {
  // Avoid printing raw GitHub errors, response bodies,
  // authorization headers, or sensitive error causes.

  console.error(
    JSON.stringify(
      {
        status: "FAIL",

        stage,

        errorType:
          error instanceof Error
            ? error.name
            : "UnknownError",

        durationMs: Math.round(
          performance.now() - startedAt,
        ),
      },

      null,

      2,
    ),
  );

  process.exitCode = 1;
} finally {
  // ------------------------------------------------
  // Cleanup must not prevent database disconnection
  // ------------------------------------------------

  try {
    if (root) {
      await rm(root, {
        recursive: true,
        force: true,
        maxRetries: 3,
        retryDelay: 200,
      });

      console.log(
        "[cleanup] Temporary workspace removed",
      );
    }
  } catch {
    passed = false;

    process.exitCode = 1;

    console.error(
      "[cleanup] Temporary workspace cleanup failed",
    );
  }

  try {
    await prisma.$disconnect();

    console.log(
      "[cleanup] Database disconnected",
    );
  } catch {
    passed = false;

    process.exitCode = 1;

    console.error(
      "[cleanup] Database disconnection failed",
    );
  }
}

if (!passed) {
  process.exitCode = 1;
} else {
  console.log(
    "[PASS] Live GitHub source retrieval smoke test",
  );
}