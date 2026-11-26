/**
 * CloudPilot — Runtime Secret Injection
 *
 * Decrypts stored environment variables inside the authorized
 * worker execution boundary only.
 *
 * Security properties:
 * - Plaintext values must never enter queue payloads or logs.
 * - Plaintext values are passed to the runtime out-of-band,
 *   never on a process command line.
 * - Decryption uses AES-256-GCM and the existing associated
 *   data context shared with the API.
 * - Malformed ciphertext and authentication failures fail closed.
 * - Environment variable names and values are validated before
 *   being passed to the runtime.
 * - Individual and aggregate payload sizes are bounded.
 * - Sensitive temporary buffers are cleared where possible.
 *
 * Security limitations:
 * - JavaScript strings cannot be reliably zeroized.
 * - The caller must enforce deployment and tenant ownership.
 * - Docker environment variables remain accessible to users
 *   with sufficient Docker daemon privileges.
 * - The runtime client must securely manage temporary env files.
 */

import {
  createDecipheriv,
} from "node:crypto";

import {
  TextDecoder,
} from "node:util";

import {
  RuntimeDeploymentError,
} from "@cloudpilot/queue";

// ==================================================
// Cryptographic constants
// ==================================================

const CIPHER_VERSION = "v1";

const CIPHER_ALGORITHM = "aes-256-gcm";

const ENCRYPTION_KEY_BYTES = 32;

const ENCRYPTION_KEY_HEX_LENGTH = 64;

const NONCE_BYTES = 12;

const AUTH_TAG_BYTES = 16;

// ==================================================
// Environment variable limits
// ==================================================

const MAX_VARIABLE_NAME_LENGTH = 256;

const MAX_VARIABLE_VALUE_BYTES = 16 * 1024;

const MAX_ENVIRONMENT_VARIABLES = 256;

const MAX_ENVIRONMENT_TOTAL_BYTES = 256 * 1024;

const MAX_CONTEXT_VALUE_LENGTH = 128;

/**
 * Maximum encoded ciphertext length for a plaintext
 * value bounded by MAX_VARIABLE_VALUE_BYTES.
 */
const MAX_CIPHERTEXT_ENCODED_LENGTH =
  Math.ceil(
    (MAX_VARIABLE_VALUE_BYTES * 4) / 3,
  );

/**
 * Includes cipher version, nonce, authentication tag,
 * ciphertext, and format separators.
 */
const MAX_ENCRYPTED_VALUE_LENGTH =
  MAX_CIPHERTEXT_ENCODED_LENGTH + 128;

// ==================================================
// Validation patterns
// ==================================================

const ENCRYPTION_KEY_PATTERN =
  /^[a-f0-9]{64}$/iu;

const VARIABLE_NAME_PATTERN =
  /^[A-Za-z_][A-Za-z0-9_]*$/u;

const INVALID_VARIABLE_NAME_CHARACTER_PATTERN =
  /[^A-Za-z0-9_]/u;

const BASE64URL_PATTERN =
  /^[A-Za-z0-9_-]*$/u;

const INVALID_BASE64URL_CHARACTER_PATTERN =
  /[^A-Za-z0-9_-]/u;

const CONTROL_CHARACTER_PATTERN =
  /[\u0000-\u001f\u007f-\u009f]/u;

/**
 * Docker env-file entries are line-oriented.
 *
 * Reject values that cannot be safely represented
 * as a single KEY=VALUE entry.
 */
const UNSAFE_ENV_VALUE_PATTERN =
  /[\u0000\r\n]/u;

const UUID_PATTERN =
  /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/iu;

// ==================================================
// UTF-8 decoder
// ==================================================

/**
 * Reject invalid UTF-8 instead of silently replacing
 * malformed bytes with replacement characters.
 *
 * Preserve an initial UTF-8 BOM as part of the
 * original environment variable value.
 */
const utf8Decoder = new TextDecoder(
  "utf-8",
  {
    fatal: true,
    ignoreBOM: true,
  },
);

// ==================================================
// Stored variable shape
// ==================================================

export interface StoredEnvironmentVariable {
  key: string;
  encryptedValue: string;
}

// ==================================================
// Internal context types
// ==================================================

interface RuntimeEnvironmentContext {
  workspaceId: string;
  projectId: string;
  environment: string;
  encryptionKey: string;
}

interface EnvironmentAssociatedData {
  workspaceId: string;
  projectId: string;
  environment: string;
  key: string;
}

// ==================================================
// Error handling
// ==================================================

/**
 * Always return sanitized configuration errors.
 *
 * Never include plaintext values, encrypted values,
 * encryption keys, or underlying cryptographic errors.
 */
function configurationError(
  message: string,
): RuntimeDeploymentError {
  return new RuntimeDeploymentError(
    "INVALID_RUNTIME_CONFIGURATION",
    message,
  );
}

// ==================================================
// Context validation
// ==================================================

function isValidUuid(
  value: unknown,
): value is string {
  return (
    typeof value === "string" &&
    value.length === 36 &&
    !CONTROL_CHARACTER_PATTERN.test(value) &&
    UUID_PATTERN.test(value)
  );
}

/**
 * Validate the execution context without modifying it.
 *
 * Associated data must remain byte-for-byte compatible
 * with the context used by the API during encryption.
 *
 * Do not trim, lowercase, or otherwise normalize
 * these values before constructing associated data.
 */
function assertValidContext(
  input: RuntimeEnvironmentContext,
): void {
  if (
    !input ||
    typeof input !== "object" ||
    Array.isArray(input)
  ) {
    throw configurationError(
      "The runtime environment context is invalid.",
    );
  }

  if (
    !isValidUuid(input.workspaceId) ||
    !isValidUuid(input.projectId)
  ) {
    throw configurationError(
      "The runtime environment identity is invalid.",
    );
  }

  if (
    typeof input.environment !== "string" ||
    input.environment.length === 0 ||
    input.environment.length >
      MAX_CONTEXT_VALUE_LENGTH ||
    input.environment.trim() !==
      input.environment ||
    CONTROL_CHARACTER_PATTERN.test(
      input.environment,
    )
  ) {
    throw configurationError(
      "The deployment environment is invalid.",
    );
  }
}

// ==================================================
// Encryption key validation
// ==================================================

function resolveEncryptionKey(
  value: unknown,
): Buffer {
  if (
    typeof value !== "string" ||
    value.length !==
      ENCRYPTION_KEY_HEX_LENGTH ||
    CONTROL_CHARACTER_PATTERN.test(value) ||
    !ENCRYPTION_KEY_PATTERN.test(value)
  ) {
    throw configurationError(
      "The runtime secret key is not configured correctly.",
    );
  }

  const keyBuffer = Buffer.from(
    value,
    "hex",
  );

  if (
    keyBuffer.length !== ENCRYPTION_KEY_BYTES
  ) {
    keyBuffer.fill(0);

    throw configurationError(
      "The runtime secret key is invalid.",
    );
  }

  return keyBuffer;
}

// ==================================================
// Environment variable name validation
// ==================================================

function assertValidVariableName(
  value: unknown,
): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length >
      MAX_VARIABLE_NAME_LENGTH ||
    INVALID_VARIABLE_NAME_CHARACTER_PATTERN.test(
      value,
    ) ||
    !VARIABLE_NAME_PATTERN.test(value)
  ) {
    throw configurationError(
      "A stored environment variable name is invalid.",
    );
  }
}

// ==================================================
// Stored variable validation
// ==================================================

function assertValidStoredVariable(
  variable: StoredEnvironmentVariable,
): void {
  if (
    !variable ||
    typeof variable !== "object" ||
    Array.isArray(variable)
  ) {
    throw configurationError(
      "A stored environment variable is invalid.",
    );
  }

  assertValidVariableName(
    variable.key,
  );

  if (
    typeof variable.encryptedValue !== "string" ||
    variable.encryptedValue.length === 0 ||
    variable.encryptedValue.length >
      MAX_ENCRYPTED_VALUE_LENGTH ||
    CONTROL_CHARACTER_PATTERN.test(
      variable.encryptedValue,
    )
  ) {
    throw configurationError(
      "A stored environment variable has an invalid format.",
    );
  }
}

// ==================================================
// Strict Base64URL decoding
// ==================================================

/**
 * Decode canonical, unpadded Base64URL.
 *
 * Node.js Base64 decoding is intentionally permissive.
 * Explicit validation and round-trip comparison prevent
 * malformed or noncanonical input from being accepted.
 *
 * Empty ciphertext is allowed because AES-GCM can
 * authenticate an empty plaintext value.
 */
function decodeBase64Url(
  value: unknown,
  label: string,
  options: {
    allowEmpty?: boolean;
    maxBytes: number;
  },
): Buffer {
  const maximumEncodedLength =
    Math.ceil(
      (options.maxBytes * 4) / 3,
    );

  if (
    typeof value !== "string" ||
    value.length > maximumEncodedLength ||
    INVALID_BASE64URL_CHARACTER_PATTERN.test(
      value,
    ) ||
    !BASE64URL_PATTERN.test(value) ||
    value.length % 4 === 1
  ) {
    throw configurationError(
      `The stored ${label} is invalid.`,
    );
  }

  if (
    value.length === 0 &&
    !options.allowEmpty
  ) {
    throw configurationError(
      `The stored ${label} is empty.`,
    );
  }

  let buffer: Buffer;

  try {
    buffer = Buffer.from(
      value,
      "base64url",
    );
  } catch {
    throw configurationError(
      `The stored ${label} is invalid.`,
    );
  }

  if (
    buffer.length > options.maxBytes ||
    (
      buffer.length === 0 &&
      !options.allowEmpty
    ) ||
    buffer.toString("base64url") !== value
  ) {
    buffer.fill(0);

    throw configurationError(
      `The stored ${label} is invalid.`,
    );
  }

  return buffer;
}

// ==================================================
// Associated data
// ==================================================

/**
 * Reconstruct the exact associated-data context used
 * by the API when encrypting environment variables.
 *
 * Changing the field order or normalizing any value
 * would invalidate existing authentication tags.
 */
function associatedData(
  input: EnvironmentAssociatedData,
): Buffer {
  return Buffer.from(
    JSON.stringify([
      input.workspaceId,
      input.projectId,
      input.environment,
      input.key,
    ]),
    "utf8",
  );
}

// ==================================================
// Plaintext validation
// ==================================================

/**
 * Validate a decrypted value before it enters the
 * runtime environment map.
 *
 * Empty values are allowed.
 *
 * Newlines, carriage returns, and NUL characters
 * are rejected to prevent unsafe env-file entries.
 *
 * Values are never trimmed or otherwise modified.
 */
function assertValidPlaintext(
  value: string,
): void {
  if (
    Buffer.byteLength(
      value,
      "utf8",
    ) > MAX_VARIABLE_VALUE_BYTES
  ) {
    throw configurationError(
      "A decrypted environment variable exceeds its size limit.",
    );
  }

  if (
    UNSAFE_ENV_VALUE_PATTERN.test(value)
  ) {
    throw configurationError(
      "A decrypted environment variable contains unsupported characters.",
    );
  }
}

// ==================================================
// Authenticated decryption
// ==================================================

/**
 * Decrypt a single environment variable using
 * AES-256-GCM authenticated encryption.
 *
 * Plaintext is not decoded or returned until
 * authentication has completed successfully.
 */
function decryptValue(
  input: EnvironmentAssociatedData,
  variable: StoredEnvironmentVariable,
  keyBuffer: Buffer,
): string {
  const parts =
    variable.encryptedValue.split(".");

  if (
    parts.length !== 4 ||
    parts[0] !== CIPHER_VERSION
  ) {
    throw configurationError(
      "A stored environment variable has an unsupported format.",
    );
  }

  let nonce: Buffer | undefined;

  let tag: Buffer | undefined;

  let ciphertext: Buffer | undefined;

  let aad: Buffer | undefined;

  let decryptedChunk: Buffer | undefined;

  let finalChunk: Buffer | undefined;

  let plaintextBuffer: Buffer | undefined;

  try {
    // --------------------------------------------------
    // Decode and validate nonce
    // --------------------------------------------------

    nonce = decodeBase64Url(
      parts[1]!,
      "nonce",
      {
        maxBytes: NONCE_BYTES,
      },
    );

    if (
      nonce.length !== NONCE_BYTES
    ) {
      throw configurationError(
        "A stored environment variable nonce is invalid.",
      );
    }

    // --------------------------------------------------
    // Decode and validate authentication tag
    // --------------------------------------------------

    tag = decodeBase64Url(
      parts[2]!,
      "authentication tag",
      {
        maxBytes: AUTH_TAG_BYTES,
      },
    );

    if (
      tag.length !== AUTH_TAG_BYTES
    ) {
      throw configurationError(
        "A stored environment variable authentication tag is invalid.",
      );
    }

    // --------------------------------------------------
    // Decode bounded ciphertext
    // --------------------------------------------------

    ciphertext = decodeBase64Url(
      parts[3]!,
      "ciphertext",
      {
        allowEmpty: true,
        maxBytes: MAX_VARIABLE_VALUE_BYTES,
      },
    );

    // --------------------------------------------------
    // Construct tenant-scoped associated data
    // --------------------------------------------------

    aad = associatedData(input);

    // --------------------------------------------------
    // Authenticate and decrypt
    // --------------------------------------------------

    try {
      const decipher = createDecipheriv(
        CIPHER_ALGORITHM,
        keyBuffer,
        nonce,
        {
          authTagLength: AUTH_TAG_BYTES,
        },
      );

      decipher.setAAD(aad);

      decipher.setAuthTag(tag);

      decryptedChunk =
        decipher.update(ciphertext);

      /**
       * Authentication is completed here.
       *
       * If the key, ciphertext, nonce, authentication
       * tag, or associated data is incorrect, this
       * operation must fail.
       */
      finalChunk = decipher.final();

      plaintextBuffer = Buffer.concat([
        decryptedChunk,
        finalChunk,
      ]);
    } catch {
      throw configurationError(
        "A stored environment variable could not be decrypted.",
      );
    }

    // --------------------------------------------------
    // Validate decrypted byte length
    // --------------------------------------------------

    if (
      plaintextBuffer.length >
        MAX_VARIABLE_VALUE_BYTES
    ) {
      throw configurationError(
        "A decrypted environment variable exceeds its size limit.",
      );
    }

    // --------------------------------------------------
    // Strict UTF-8 decoding
    // --------------------------------------------------

    let plaintext: string;

    try {
      plaintext =
        utf8Decoder.decode(
          plaintextBuffer,
        );
    } catch {
      throw configurationError(
        "A decrypted environment variable has invalid text encoding.",
      );
    }

    // --------------------------------------------------
    // Validate runtime representation
    // --------------------------------------------------

    assertValidPlaintext(
      plaintext,
    );

    return plaintext;
  } finally {
    // --------------------------------------------------
    // Best-effort sensitive buffer cleanup
    // --------------------------------------------------

    plaintextBuffer?.fill(0);

    decryptedChunk?.fill(0);

    finalChunk?.fill(0);

    ciphertext?.fill(0);

    nonce?.fill(0);

    tag?.fill(0);

    aad?.fill(0);
  }
}

// ==================================================
// Environment map cleanup
// ==================================================

/**
 * Remove references to previously decrypted values
 * when resolution fails partway through.
 *
 * This is best-effort cleanup. JavaScript strings
 * cannot be reliably overwritten in memory.
 */
function clearEnvironmentMap(
  environment: Record<string, string>,
): void {
  for (
    const key of Object.keys(environment)
  ) {
    environment[key] = "";

    delete environment[key];
  }
}

// ==================================================
// Public environment resolution
// ==================================================

/**
 * Decrypt stored environment variables into a runtime
 * environment map.
 *
 * Caller responsibilities:
 * - Verify deployment ownership.
 * - Select only variables belonging to the deployment's
 *   workspace, project, and environment.
 * - Supply the authorized encryption key.
 * - Never log the returned environment map.
 * - Never serialize plaintext into queue payloads.
 * - Never include plaintext values in process arguments.
 * - Securely create and remove temporary env files.
 *
 * This function fails closed and never returns a
 * partially resolved environment.
 */
export function resolveRuntimeEnvironment(
  input: {
    workspaceId: string;
    projectId: string;
    environment: string;
    encryptionKey: string;
  },
  variables: readonly StoredEnvironmentVariable[],
): Record<string, string> {
  // --------------------------------------------------
  // Validate execution context
  // --------------------------------------------------

  assertValidContext(input);

  // --------------------------------------------------
  // Validate collection boundaries
  // --------------------------------------------------

  if (
    !Array.isArray(variables) ||
    variables.length >
      MAX_ENVIRONMENT_VARIABLES
  ) {
    throw configurationError(
      "The runtime environment exceeds its variable limit.",
    );
  }

  // --------------------------------------------------
  // Resolve encryption key
  // --------------------------------------------------

  const keyBuffer =
    resolveEncryptionKey(
      input.encryptionKey,
    );

  /**
   * A null-prototype object prevents inherited property
   * collisions and special prototype setter behavior.
   */
  const environment: Record<string, string> =
    Object.create(null);

  let totalEnvironmentBytes = 0;

  try {
    // --------------------------------------------------
    // Validate and decrypt stored variables
    // --------------------------------------------------

    for (const variable of variables) {
      assertValidStoredVariable(
        variable,
      );

      // ----------------------------------------------
      // Duplicate variable protection
      // ----------------------------------------------

      if (
        Object.hasOwn(
          environment,
          variable.key,
        )
      ) {
        throw configurationError(
          "A duplicate environment variable name was resolved.",
        );
      }

      // ----------------------------------------------
      // Authenticated decryption
      // ----------------------------------------------

      const plaintext = decryptValue(
        {
          workspaceId: input.workspaceId,
          projectId: input.projectId,
          environment: input.environment,
          key: variable.key,
        },
        variable,
        keyBuffer,
      );

      // ----------------------------------------------
      // Aggregate environment size validation
      // ----------------------------------------------

      const entryBytes =
        Buffer.byteLength(
          variable.key,
          "utf8",
        ) +
        Buffer.byteLength(
          plaintext,
          "utf8",
        ) +
        2;

      /**
       * Additional two bytes account for the equals
       * separator and newline in a KEY=VALUE entry.
       */
      if (
        entryBytes >
          MAX_ENVIRONMENT_TOTAL_BYTES -
            totalEnvironmentBytes
      ) {
        throw configurationError(
          "The runtime environment exceeds its total size limit.",
        );
      }

      totalEnvironmentBytes +=
        entryBytes;

      // ----------------------------------------------
      // Store validated environment variable
      // ----------------------------------------------

      environment[variable.key] =
        plaintext;
    }

    // --------------------------------------------------
    // Return only a completely resolved environment
    // --------------------------------------------------

    return environment;
  } catch (error) {
    // --------------------------------------------------
    // Never retain a partially resolved environment
    // --------------------------------------------------

    clearEnvironmentMap(
      environment,
    );

    if (
      error instanceof RuntimeDeploymentError
    ) {
      throw error;
    }

    /**
     * Unexpected errors must also fail closed without
     * exposing sensitive implementation details.
     */
    throw configurationError(
      "The runtime environment could not be resolved.",
    );
  } finally {
    // --------------------------------------------------
    // Best-effort encryption key cleanup
    // --------------------------------------------------

    keyBuffer.fill(0);
  }
}