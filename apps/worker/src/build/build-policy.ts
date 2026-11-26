
/**
 * CloudPilot — Build Policy
 *
 * Conservative Dockerfile policy validation.
 *
 * This module provides defense in depth only.
 * It is NOT a security sandbox and must never replace
 * executor-level isolation and entitlement enforcement.
 */

import { BuildError } from "@cloudpilot/queue";

// ==================================================
// Policy definition
// ==================================================

export interface BuildPolicy {
  rejectCustomFrontend: boolean;
  rejectRemoteAdd: boolean;
  rejectInsecureSecurity: boolean;
  rejectPrivileged: boolean;
  rejectHostNetwork: boolean;
  rejectSshForwarding: boolean;
  rejectSecretMounts: boolean;
  rejectHostBindMounts: boolean;
  allowedTargets: readonly string[];
}

export const defaultBuildPolicy: BuildPolicy =
  Object.freeze({
    rejectCustomFrontend: true,
    rejectRemoteAdd: true,
    rejectInsecureSecurity: true,
    rejectPrivileged: true,
    rejectHostNetwork: true,
    rejectSshForwarding: true,
    rejectSecretMounts: true,
    rejectHostBindMounts: true,
    allowedTargets: Object.freeze([]),
  });

// ==================================================
// Constants
// ==================================================

const MAX_DOCKERFILE_BYTES = 1024 * 1024;

const MAX_INSTRUCTIONS = 10_000;

const MAX_INSTRUCTION_LENGTH = 256 * 1024;

const INSTRUCTION_PATTERN =
  /^([A-Za-z]+)[ \t]+(.+)$/u;

const DIRECTIVE_PATTERN =
  /^\s*#\s*([A-Za-z]+)\s*=\s*(.*?)\s*$/u;

const TARGET_PATTERN =
  /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/u;

const UNSAFE_CONTROL_PATTERN =
  /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/u;

// ==================================================
// Error helpers
// ==================================================

function violation(reason: string): BuildError {
  return new BuildError(
    "BUILD_POLICY_VIOLATION",
    `Build policy violation: ${reason}`,
  );
}

function invalidDockerfile(reason: string): BuildError {
  return new BuildError(
    "DOCKERFILE_INVALID",
    `Invalid Dockerfile: ${reason}`,
  );
}

// ==================================================
// Dockerfile instruction contract
// ==================================================

export interface DockerfileInstruction {
  index: number;
  keyword: string;
  rest: string;
}

// ==================================================
// Content validation
// ==================================================

function normalizeContent(content: string): string {
  if (typeof content !== "string") {
    throw invalidDockerfile(
      "content must be a string",
    );
  }

  if (
    Buffer.byteLength(content, "utf8") >
    MAX_DOCKERFILE_BYTES
  ) {
    throw new BuildError(
      "BUILD_RESOURCE_LIMIT_EXCEEDED",
      "Dockerfile exceeds its allowed size",
    );
  }

  const normalized = content
    .replace(/^\uFEFF/u, "")
    .replace(/\r\n/gu, "\n");

  if (
    normalized.includes("\r") ||
    UNSAFE_CONTROL_PATTERN.test(normalized)
  ) {
    throw invalidDockerfile(
      "unsupported control characters",
    );
  }

  return normalized;
}

// ==================================================
// Parser directive validation
// ==================================================

function validateDirectives(
  content: string,
  policy: BuildPolicy,
): void {
  for (const line of content.split("\n")) {
    const match = DIRECTIVE_PATTERN.exec(line);

    if (!match) {
      continue;
    }

    const name = (match[1] ?? "").toLowerCase();

    const value = (match[2] ?? "").trim();

    if (
      name === "syntax" &&
      policy.rejectCustomFrontend
    ) {
      throw violation(
        "custom Dockerfile frontends are not permitted",
      );
    }

    if (
      name === "escape" &&
      value !== "\\"
    ) {
      throw violation(
        "alternative Dockerfile escape characters are not supported",
      );
    }
  }
}

// ==================================================
// Dockerfile instruction parser
// ==================================================

/**
 * Conservative logical instruction parser.
 *
 * Supports standard backslash continuations.
 * Unsupported syntax is rejected rather than ignored.
 */
export function parseDockerfileInstructions(
  content: string,
): DockerfileInstruction[] {
  const normalized = normalizeContent(content);

  const lines = normalized.split("\n");

  const instructions: DockerfileInstruction[] = [];

  let buffer = "";

  let bufferStart = -1;

  let continuing = false;

  for (
    let index = 0;
    index < lines.length;
    index += 1
  ) {
    const raw = lines[index] ?? "";

    const trimmed = raw.trimEnd();

    const leadingTrimmed = trimmed.trimStart();

    // Dockerfile comments are not instructions.
    // Comments inside continuations are also skipped.
    if (
      leadingTrimmed.length === 0 ||
      leadingTrimmed.startsWith("#")
    ) {
      continue;
    }

    if (!continuing) {
      bufferStart = index;
      buffer = "";
    }

    const trailingBackslashes =
      /\\+$/u.exec(trimmed)?.[0].length ?? 0;

    const hasContinuation =
      trailingBackslashes % 2 === 1;

    const segment = hasContinuation
      ? trimmed.slice(0, -1)
      : trimmed;

    buffer += segment.trimStart();

    if (
      buffer.length > MAX_INSTRUCTION_LENGTH
    ) {
      throw invalidDockerfile(
        "instruction exceeds its allowed length",
      );
    }

    if (hasContinuation) {
      continuing = true;
      continue;
    }

    continuing = false;

    const match = INSTRUCTION_PATTERN.exec(
      buffer.trim(),
    );

    if (!match) {
      throw invalidDockerfile(
        "malformed instruction",
      );
    }

    const keyword =
      (match[1] ?? "").toUpperCase();

    const rest = (match[2] ?? "").trim();

    if (!keyword || !rest) {
      throw invalidDockerfile(
        "incomplete instruction",
      );
    }

    /**
     * Heredoc parsing requires a complete Dockerfile
     * grammar. Reject it in the current restricted
     * implementation instead of parsing it incorrectly.
     */
    if (
      (
        keyword === "RUN" ||
        keyword === "COPY" ||
        keyword === "ADD"
      ) &&
      rest.includes("<<")
    ) {
      throw violation(
        "heredoc syntax is not supported",
      );
    }

    instructions.push({
      index: bufferStart,
      keyword,
      rest,
    });

    if (
      instructions.length > MAX_INSTRUCTIONS
    ) {
      throw invalidDockerfile(
        "too many Dockerfile instructions",
      );
    }

    buffer = "";
    bufferStart = -1;
  }

  if (continuing || buffer.length > 0) {
    throw invalidDockerfile(
      "unfinished line continuation",
    );
  }

  return instructions;
}

// ==================================================
// Instruction flag parsing
// ==================================================

interface InstructionFlag {
  name: string;
  value: string | null;
}

interface ParsedInstructionArguments {
  flags: InstructionFlag[];
  arguments: string;
}

/**
 * Parse leading instruction flags only.
 *
 * Complex quoting and escaping in flags are rejected
 * because their interpretation can differ between
 * Dockerfile frontend implementations.
 */
function parseInstructionFlags(
  rest: string,
): ParsedInstructionArguments {
  const flags: InstructionFlag[] = [];

  let remaining = rest.trimStart();

  while (remaining.startsWith("--")) {
    const match =
      /^--([A-Za-z][A-Za-z0-9-]*)(?:=([^\s]+))?(?=\s|$)/u
        .exec(remaining);

    if (!match) {
      throw violation(
        "unsupported instruction flag syntax",
      );
    }

    const name =
      (match[1] ?? "").toLowerCase();

    const value = match[2] ?? null;

    if (
      value !== null &&
      /["'`\\$]/u.test(value)
    ) {
      throw violation(
        "quoted or dynamic instruction flags are not supported",
      );
    }

    flags.push({
      name,
      value,
    });

    remaining = remaining
      .slice(match[0].length)
      .trimStart();
  }

  return {
    flags,
    arguments: remaining,
  };
}

// ==================================================
// Remote ADD validation
// ==================================================

function isRemoteAddSource(
  rest: string,
): boolean {
  const parsed = parseInstructionFlags(rest);

  const args = parsed.arguments.trim();

  let sources: string[];

  if (args.startsWith("[")) {
    let decoded: unknown;

    try {
      decoded = JSON.parse(args);
    } catch {
      throw invalidDockerfile(
        "invalid JSON-form ADD instruction",
      );
    }

    if (
      !Array.isArray(decoded) ||
      decoded.length < 2 ||
      !decoded.every(
        (item) => typeof item === "string",
      )
    ) {
      throw invalidDockerfile(
        "invalid ADD arguments",
      );
    }

    sources = decoded.slice(0, -1);
  } else {
    if (/["'`\\]/u.test(args)) {
      throw violation(
        "complex ADD paths are not supported",
      );
    }

    const tokens = args.split(/\s+/u);

    if (tokens.length < 2) {
      throw invalidDockerfile(
        "ADD requires a source and destination",
      );
    }

    sources = tokens.slice(0, -1);
  }

  for (const source of sources) {
    /**
     * Dynamic ADD sources cannot be reliably
     * evaluated without resolving build arguments.
     */
    if (source.includes("$")) {
      throw violation(
        "dynamic ADD sources are not permitted",
      );
    }

    // URI schemes, protocol-relative URLs and
    // SCP-style Git repository references.
    if (
      /^[A-Za-z][A-Za-z0-9+.-]*:/u.test(source) ||
      source.startsWith("//") ||
      /^[^/\s]+@[^/\s]+:/u.test(source)
    ) {
      return true;
    }
  }

  return false;
}

// ==================================================
// RUN mount validation
// ==================================================

function validateMount(
  specification: string,
  policy: BuildPolicy,
): void {
  if (!specification) {
    throw violation(
      "mount specification is missing",
    );
  }

  const options = new Map<string, string>();

  for (const part of specification.split(",")) {
    const separator = part.indexOf("=");

    const key = (
      separator === -1
        ? part
        : part.slice(0, separator)
    )
      .trim()
      .toLowerCase();

    const value = (
      separator === -1
        ? "true"
        : part.slice(separator + 1)
    ).trim();

    if (
      !/^[a-z][a-z0-9_-]*$/u.test(key) ||
      !value ||
      options.has(key)
    ) {
      throw violation(
        "invalid or duplicate mount option",
      );
    }

    options.set(key, value);
  }

  // BuildKit uses bind as the default mount type.
  const type = (
    options.get("type") ?? "bind"
  ).toLowerCase();

  const supportedTypes = new Set([
    "bind",
    "cache",
    "tmpfs",
    "secret",
    "ssh",
  ]);

  if (!supportedTypes.has(type)) {
    throw violation(
      "unsupported RUN mount type",
    );
  }

  if (
    type === "ssh" &&
    policy.rejectSshForwarding
  ) {
    throw violation(
      "SSH agent forwarding mounts are not permitted",
    );
  }

  if (
    type === "secret" &&
    policy.rejectSecretMounts
  ) {
    throw violation(
      "secret mounts are not permitted",
    );
  }

  if (
    type === "bind" &&
    policy.rejectHostBindMounts
  ) {
    /**
     * A bind mount without from= normally references
     * the build context, not arbitrary host paths.
     *
     * CloudPilot's current conservative policy
     * requires an explicit source for bind mounts.
     */
    const from = options.get("from");

    if (
      !from ||
      !TARGET_PATTERN.test(from)
    ) {
      throw violation(
        "bind mounts require an explicit approved source",
      );
    }
  }
}

// ==================================================
// RUN entitlement validation
// ==================================================

function validateRunEntitlements(
  rest: string,
  policy: BuildPolicy,
): void {
  const parsed = parseInstructionFlags(rest);

  const seen = new Set<string>();

  for (const flag of parsed.flags) {
    const name = flag.name;

    const value =
      flag.value?.toLowerCase() ?? null;

    // Multiple mounts are legitimate.
    if (
      name !== "mount" &&
      seen.has(name)
    ) {
      throw violation(
        "duplicate RUN entitlement",
      );
    }

    seen.add(name);

    switch (name) {
      case "privileged":
        if (policy.rejectPrivileged) {
          throw violation(
            "privileged RUN entitlements are not permitted",
          );
        }
        break;

      case "security":
        if (
          value !== "sandbox" &&
          value !== "insecure"
        ) {
          throw violation(
            "unsupported security entitlement",
          );
        }

        if (
          value === "insecure" &&
          policy.rejectInsecureSecurity
        ) {
          throw violation(
            "insecure security entitlements are not permitted",
          );
        }
        break;

      case "network":
        if (
          value !== "default" &&
          value !== "none" &&
          value !== "host"
        ) {
          throw violation(
            "unsupported network entitlement",
          );
        }

        if (
          value === "host" &&
          policy.rejectHostNetwork
        ) {
          throw violation(
            "host networking is not permitted",
          );
        }
        break;

      case "mount":
        if (!flag.value) {
          throw violation(
            "mount specification is missing",
          );
        }

        validateMount(
          flag.value,
          policy,
        );
        break;

      default:
        throw violation(
          "unsupported RUN entitlement",
        );
    }
  }

  if (!parsed.arguments) {
    throw invalidDockerfile(
      "RUN instruction has no command",
    );
  }
}

// ==================================================
// Build target validation
// ==================================================

function validateConfiguredTargets(
  policy: BuildPolicy,
): void {
  for (const target of policy.allowedTargets) {
    if (!TARGET_PATTERN.test(target)) {
      throw violation(
        "configured build target is invalid",
      );
    }
  }
}

/**
 * Validate an explicitly requested build target.
 *
 * The Dockerfile validator must separately verify
 * that the requested stage actually exists.
 */
export function validateBuildTarget(
  target: string | null | undefined,
  policy: BuildPolicy = defaultBuildPolicy,
): void {
  validateConfiguredTargets(policy);

  if (target == null) {
    if (policy.allowedTargets.length > 0) {
      throw violation(
        "an approved build target must be selected",
      );
    }

    return;
  }

  if (!TARGET_PATTERN.test(target)) {
    throw violation(
      "invalid build target",
    );
  }

  if (
    policy.allowedTargets.length > 0 &&
    !policy.allowedTargets.includes(target)
  ) {
    throw violation(
      "requested build target is not permitted",
    );
  }
}

// ==================================================
// Main build policy validation
// ==================================================

export function validateBuildPolicy(
  content: string,
  policy: BuildPolicy = defaultBuildPolicy,
): void {
  const normalized = normalizeContent(content);

  validateDirectives(
    normalized,
    policy,
  );

  validateConfiguredTargets(policy);

  const instructions =
    parseDockerfileInstructions(normalized);

  for (const instruction of instructions) {
    const { keyword, rest } = instruction;

    if (
      keyword === "ADD" &&
      policy.rejectRemoteAdd &&
      isRemoteAddSource(rest)
    ) {
      throw violation(
        "remote ADD sources are not permitted",
      );
    }

    if (keyword === "RUN") {
      validateRunEntitlements(
        rest,
        policy,
      );
    }

    /**
     * ONBUILD can defer execution of instructions
     * until a downstream image build.
     *
     * Reject it in the current restricted policy.
     */
    if (keyword === "ONBUILD") {
      throw violation(
        "ONBUILD instructions are not supported",
      );
    }
  }
}
