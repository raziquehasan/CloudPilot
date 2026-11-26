import { createReadStream } from "node:fs";
import {
  mkdir,
  open,
  lstat,
  realpath,
  rm,
} from "node:fs/promises";
import {
  join,
  resolve,
  relative,
  isAbsolute,
  sep,
  parse,
} from "node:path";
import { createGunzip } from "node:zlib";
import { pipeline } from "node:stream/promises";
import { TextDecoder } from "node:util";
import { SourcePreparationError } from "@cloudpilot/queue";

// ==================================================
// Constants
// ==================================================

const BLOCK = 512;
const READ_CHUNK = 64 * 1024;

const MAX_METADATA_BYTES = 64 * 1024;
const MAX_TOTAL_METADATA_BYTES = 8 * 1024 * 1024;
const MAX_METADATA_ENTRIES = 4096;
const MAX_PATH_BYTES = 4096;
const MAX_PATH_DEPTH = 64;

const DIRECTORY_MODE = 0o700;
const FILE_MODE = 0o600;

const UTF8 = new TextDecoder("utf-8", {
  fatal: true,
});

const WINDOWS_RESERVED =
  /^(CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³]|CONIN\$|CONOUT\$)$/iu;

// ==================================================
// Public contracts
// ==================================================

export interface SafeExtractionLimits {
  maxExtractedBytes: number;
  maxFileBytes: number;
  maxFileCount: number;
  maxDirectoryCount?: number;
}

export interface SafeExtractionResult {
  extractedBytes: number;
  fileCount: number;
  directoryCount: number;
}

interface ParsedHeader {
  name: string;
  size: number;
  typeFlag: string;
}

interface PaxRecords {
  path?: string;
  size?: number;
}

interface RegisteredPath {
  original: string;
  kind: "file" | "directory";
  explicit: boolean;
}

// ==================================================
// Sanitized errors
// ==================================================

function malformed(message: string): never {
  throw new SourcePreparationError(
    "MALFORMED_ARCHIVE",
    message,
  );
}

function unsafe(message: string): never {
  throw new SourcePreparationError(
    "UNSAFE_ARCHIVE",
    message,
  );
}

function workspaceUnavailable(
  message: string,
): never {
  throw new SourcePreparationError(
    "WORKSPACE_UNAVAILABLE",
    message,
  );
}

function isMissingPath(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "ENOENT"
  );
}

function isAlreadyExists(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "EEXIST"
  );
}

// ==================================================
// Strict UTF-8 decoding
// ==================================================

function decodeUtf8(buffer: Buffer): string {
  try {
    return UTF8.decode(buffer);
  } catch {
    return malformed(
      "Archive contains invalid UTF-8 metadata.",
    );
  }
}

function decodeString(
  buffer: Buffer,
  start: number,
  length: number,
): string {
  const field = buffer.subarray(
    start,
    start + length,
  );

  const zeroIndex = field.indexOf(0);

  if (zeroIndex === -1) {
    return decodeUtf8(field);
  }

  const trailing = field.subarray(zeroIndex);

  if (trailing.some((byte) => byte !== 0)) {
    return malformed(
      "Archive string field has invalid termination.",
    );
  }

  return decodeUtf8(
    field.subarray(0, zeroIndex),
  );
}

// ==================================================
// Strict TAR numeric parsing
// ==================================================

function parseOctal(
  buffer: Buffer,
  start: number,
  length: number,
): number {
  const field = buffer.subarray(
    start,
    start + length,
  );

  // Base-256 numeric fields are intentionally
  // unsupported by this bounded extractor.

  if ((field[0] ?? 0) & 0x80) {
    return malformed(
      "Unsupported TAR numeric encoding.",
    );
  }

  if (field.some((byte) => byte > 127)) {
    return malformed(
      "Invalid TAR numeric field.",
    );
  }

  const raw = field.toString("ascii");

  const match = /^ *([0-7]+)[\0 ]*$/u.exec(raw);

  if (!match?.[1]) {
    return malformed(
      "Invalid TAR octal field.",
    );
  }

  const value = Number.parseInt(match[1], 8);

  if (
    !Number.isSafeInteger(value) ||
    value < 0
  ) {
    return malformed(
      "TAR numeric field exceeds safe bounds.",
    );
  }

  return value;
}

function parseDecimal(value: string): number {
  if (!/^[0-9]+$/u.test(value)) {
    return malformed(
      "Invalid PAX numeric field.",
    );
  }

  const numeric = Number(value);

  if (
    !Number.isSafeInteger(numeric) ||
    numeric < 0
  ) {
    return malformed(
      "PAX numeric field exceeds safe bounds.",
    );
  }

  return numeric;
}

// ==================================================
// TAR checksum and header parsing
// ==================================================

function checksumMatches(
  buffer: Buffer,
): boolean {
  let checksum = 0;

  for (let index = 0; index < BLOCK; index++) {
    checksum +=
      index >= 148 && index < 156
        ? 32
        : buffer[index] ?? 0;
  }

  return checksum === parseOctal(
    buffer,
    148,
    8,
  );
}

function parseHeader(
  header: Buffer,
): ParsedHeader {
  if (header.length !== BLOCK) {
    return malformed(
      "Invalid TAR header length.",
    );
  }

  if (!checksumMatches(header)) {
    return malformed(
      "Archive header checksum is invalid.",
    );
  }

  const magic = header
    .subarray(257, 263)
    .toString("ascii");

  if (
    magic !== "ustar\0" &&
    magic !== "ustar "
  ) {
    return malformed(
      "Unsupported TAR archive format.",
    );
  }

  const name = decodeString(
    header,
    0,
    100,
  );

  // GNU TAR uses this area differently from USTAR.

  const prefix =
    magic === "ustar\0"
      ? decodeString(header, 345, 155)
      : "";

  return {
    name: prefix
      ? `${prefix}/${name}`
      : name,

    size: parseOctal(
      header,
      124,
      12,
    ),

    typeFlag: String.fromCharCode(
      header[156] ?? 0,
    ),
  };
}

function paddingFor(size: number): number {
  return (BLOCK - (size % BLOCK)) % BLOCK;
}

// ==================================================
// Strict byte-based PAX parsing
// ==================================================

function parsePaxRecords(
  content: Buffer,
  global: boolean,
): PaxRecords {
  const result: PaxRecords = {};
  const seen = new Set<string>();

  let offset = 0;

  while (offset < content.length) {
    const spaceIndex = content.indexOf(
      0x20,
      offset,
    );

    if (
      spaceIndex === -1 ||
      spaceIndex - offset > 12
    ) {
      return malformed(
        "Invalid PAX record length.",
      );
    }

    const lengthText = content
      .subarray(offset, spaceIndex)
      .toString("latin1");

    const length = parseDecimal(lengthText);

    const recordEnd = offset + length;

    if (
      length <= spaceIndex - offset + 2 ||
      recordEnd > content.length ||
      content[recordEnd - 1] !== 0x0a
    ) {
      return malformed(
        "Invalid PAX record boundary.",
      );
    }

    const record = decodeUtf8(
      content.subarray(
        spaceIndex + 1,
        recordEnd - 1,
      ),
    );

    const equalsIndex = record.indexOf("=");

    if (equalsIndex <= 0) {
      return malformed(
        "Invalid PAX key-value record.",
      );
    }

    const key = record.slice(
      0,
      equalsIndex,
    );

    const value = record.slice(
      equalsIndex + 1,
    );

    if (
      !/^[A-Za-z0-9_.-]+$/u.test(key) ||
      value.includes("\0")
    ) {
      return malformed(
        "Invalid PAX metadata.",
      );
    }

    if (seen.has(key)) {
      return malformed(
        "Duplicate PAX metadata key.",
      );
    }

    seen.add(key);

    // Sparse files and link metadata are unsupported.

    if (
      /sparse/iu.test(key) ||
      key === "linkpath" ||
      key === "SCHILY.realsize" ||
      key === "SCHILY.filetype"
    ) {
      return unsafe(
        "Unsupported archive extension.",
      );
    }

    if (key === "path" || key === "size") {
      // Global path and size overrides are rejected
      // rather than silently interpreted.

      if (global) {
        return unsafe(
          "Global archive path or size override is unsupported.",
        );
      }

      if (key === "path") {
        if (!value) {
          return malformed(
            "Empty PAX path.",
          );
        }

        result.path = value;
      } else {
        result.size = parseDecimal(value);
      }
    }

    // Other metadata such as timestamps, ownership
    // and comments is not applied to the filesystem.

    offset = recordEnd;
  }

  return result;
}

// ==================================================
// Archive path validation
// ==================================================

function isUnsafeSegment(
  segment: string,
): boolean {
  if (
    segment.length === 0 ||
    Buffer.byteLength(segment, "utf8") > 255 ||
    segment === "." ||
    segment === ".."
  ) {
    return true;
  }

  if (
    /[<>:"\\|?*\u0000-\u001f\u007f]/u.test(
      segment,
    )
  ) {
    return true;
  }

  if (/[. ]$/u.test(segment)) {
    return true;
  }

  const base =
    segment.split(".")[0]?.trim() ?? "";

  return WINDOWS_RESERVED.test(base);
}

export function validateArchivePath(
  rawName: string,
): string[] {
  if (
    typeof rawName !== "string" ||
    !rawName ||
    rawName.includes("\0") ||
    Buffer.byteLength(rawName, "utf8") >
      MAX_PATH_BYTES
  ) {
    return unsafe(
      "Archive entry has an invalid name.",
    );
  }

  if (
    rawName.includes("\\") ||
    rawName.startsWith("/") ||
    /^[A-Za-z]:/u.test(rawName)
  ) {
    return unsafe(
      "Archive entry uses an unsafe absolute or backslash path.",
    );
  }

  const normalized = rawName.endsWith("/")
    ? rawName.slice(0, -1)
    : rawName;

  const segments = normalized.split("/");

  if (segments.length > MAX_PATH_DEPTH) {
    return unsafe(
      "Archive path exceeds maximum depth.",
    );
  }

  for (const segment of segments) {
    if (isUnsafeSegment(segment)) {
      return unsafe(
        "Archive entry has an unsafe path segment.",
      );
    }
  }

  return segments;
}

// ==================================================
// Bounded streaming TAR reader
// ==================================================

class TarReader {
  private buffer: Buffer = Buffer.alloc(0);

  private ended = false;

  private totalBytes = 0;

  private readonly iterator:
    AsyncIterator<Buffer>;

  constructor(
    source: AsyncIterable<Buffer>,
    private readonly maxStreamBytes: number,
    private readonly ensureActive: () => void,
  ) {
    this.iterator =
      source[Symbol.asyncIterator]();
  }

  private async nextChunk(): Promise<void> {
    this.ensureActive();

    const next = await this.iterator.next();

    if (next.done) {
      this.ended = true;
      return;
    }

    const chunk = Buffer.isBuffer(next.value)
      ? next.value
      : Buffer.from(next.value);

    if (
      chunk.length >
      this.maxStreamBytes - this.totalBytes
    ) {
      throw new SourcePreparationError(
        "EXTRACTION_SIZE_EXCEEDED",
        "Archive decompressed stream exceeds its limit.",
      );
    }

    this.totalBytes += chunk.length;

    this.buffer =
      this.buffer.length === 0
        ? chunk
        : Buffer.concat([
            this.buffer,
            chunk,
          ]);
  }

  async readExact(
    length: number,
  ): Promise<Buffer | null> {
    if (
      !Number.isSafeInteger(length) ||
      length < 0 ||
      length > READ_CHUNK
    ) {
      return malformed(
        "Invalid archive read length.",
      );
    }

    if (length === 0) {
      return Buffer.alloc(0);
    }

    while (
      this.buffer.length < length &&
      !this.ended
    ) {
      await this.nextChunk();
    }

    if (
      this.buffer.length === 0 &&
      this.ended
    ) {
      return null;
    }

    if (this.buffer.length < length) {
      return malformed(
        "Archive ended unexpectedly.",
      );
    }

    const result = this.buffer.subarray(
      0,
      length,
    );

    this.buffer =
      this.buffer.subarray(length);

    return result;
  }

  async requireExact(
    length: number,
  ): Promise<Buffer> {
    const result =
      await this.readExact(length);

    if (result === null) {
      return malformed(
        "Archive ended unexpectedly.",
      );
    }

    return result;
  }

  async skipPadding(
    length: number,
  ): Promise<void> {
    if (length === 0) return;

    const padding =
      await this.requireExact(length);

    if (padding.some((byte) => byte !== 0)) {
      return malformed(
        "Archive contains invalid padding.",
      );
    }
  }

  async drainZeroPadding(): Promise<void> {
    for (;;) {
      this.ensureActive();

      if (
        this.buffer.some((byte) => byte !== 0)
      ) {
        return malformed(
          "Unexpected data after TAR termination.",
        );
      }

      this.buffer = Buffer.alloc(0);

      if (this.ended) return;

      await this.nextChunk();
    }
  }
}

// ==================================================
// Filesystem containment
// ==================================================

function assertContained(
  root: string,
  target: string,
): void {
  const difference = relative(
    root,
    target,
  );

  if (
    difference === ".." ||
    difference.startsWith(`..${sep}`) ||
    isAbsolute(difference)
  ) {
    return unsafe(
      "Archive entry escapes the destination.",
    );
  }
}

async function assertSafeDirectory(
  path: string,
): Promise<void> {
  const info = await lstat(path);

  if (
    !info.isDirectory() ||
    info.isSymbolicLink()
  ) {
    return unsafe(
      "Archive destination contains an unsafe directory.",
    );
  }
}

async function ensureDirectory(
  root: string,
  segments: string[],
): Promise<string> {
  let current = root;

  await assertSafeDirectory(root);

  for (const segment of segments) {
    current = join(current, segment);

    assertContained(root, current);

    try {
      await mkdir(current, {
        mode: DIRECTORY_MODE,
      });
    } catch (error) {
      if (!isAlreadyExists(error)) {
        throw error;
      }
    }

    await assertSafeDirectory(current);

    const canonical =
      await realpath(current);

    assertContained(root, canonical);
  }

  return current;
}

// ==================================================
// Extraction limit validation
// ==================================================

function validateLimits(
  limits: SafeExtractionLimits,
): number {
  const values = [
    limits.maxExtractedBytes,
    limits.maxFileBytes,
    limits.maxFileCount,
  ];

  if (
    values.some(
      (value) =>
        !Number.isSafeInteger(value) ||
        value < 1,
    )
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Invalid archive extraction limits.",
    );
  }

  if (
    limits.maxFileBytes >
    limits.maxExtractedBytes
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Individual file limit exceeds extraction limit.",
    );
  }

  const maxDirectoryCount =
    limits.maxDirectoryCount ??
    limits.maxFileCount;

  if (
    !Number.isSafeInteger(maxDirectoryCount) ||
    maxDirectoryCount < 1
  ) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Invalid archive directory limit.",
    );
  }

  return maxDirectoryCount;
}

// ==================================================
// Secure streaming archive extraction
// ==================================================

export async function extractTarGzSafely(
  input: {
    archivePath: string;
    destinationDir: string;
    limits: SafeExtractionLimits;
    shouldCancel?: () => boolean;
  },
): Promise<SafeExtractionResult> {
  const {
    archivePath,
    destinationDir,
    limits,
  } = input;

  const maxDirectoryCount =
    validateLimits(limits);

  const ensureActive = (): void => {
    if (input.shouldCancel?.()) {
      throw new SourcePreparationError(
        "SOURCE_PREPARATION_CANCELLED",
        "Source preparation was cancelled.",
      );
    }
  };

  ensureActive();

  const destination = resolve(
    destinationDir,
  );

  await assertSafeDirectory(destination);

  const root = await realpath(destination);

  // Bound the complete decompressed stream,
  // including headers, metadata and padding.

  const maxStreamBytes =
    limits.maxExtractedBytes +
    (
      limits.maxFileCount +
      maxDirectoryCount +
      MAX_METADATA_ENTRIES
    ) * BLOCK * 2 +
    MAX_TOTAL_METADATA_BYTES +
    1024 * 1024;

  if (!Number.isSafeInteger(maxStreamBytes)) {
    throw new SourcePreparationError(
      "UNEXPECTED_FAILURE",
      "Archive stream limit exceeds safe bounds.",
    );
  }

  const archiveStream =
    createReadStream(archivePath, {
      highWaterMark: READ_CHUNK,
    });

  const gunzip = createGunzip({
    chunkSize: READ_CHUNK,
  });

  // Pipeline propagates source and gzip errors.
  // Attach a rejection handler immediately.

  const completion = pipeline(
    archiveStream,
    gunzip,
  );

  void completion.catch(() => undefined);

  const reader = new TarReader(
    gunzip as AsyncIterable<Buffer>,
    maxStreamBytes,
    ensureActive,
  );

  const registered =
    new Map<string, RegisteredPath>();

  let extractedBytes = 0;
  let fileCount = 0;
  let directoryCount = 0;

  let metadataBytes = 0;
  let metadataEntries = 0;

  let pendingPath: string | undefined;
  let pendingSize: number | undefined;
  let pendingMetadata = false;

  // ================================================
  // Path registration and collision detection
  // ================================================

  const registerPath = (
    segments: string[],
    kind: "file" | "directory",
  ): void => {
    for (
      let index = 0;
      index < segments.length;
      index++
    ) {
      const original = segments
        .slice(0, index + 1)
        .join("/");

      const key = original
        .normalize("NFC")
        .toLowerCase();

      const final =
        index === segments.length - 1;

      const expectedKind =
        final ? kind : "directory";

      const existing = registered.get(key);

      if (existing) {
        if (
          existing.original !== original ||
          existing.kind !== expectedKind
        ) {
          return unsafe(
            "Archive contains conflicting paths.",
          );
        }

        if (final && existing.explicit) {
          return unsafe(
            "Archive contains duplicate entries.",
          );
        }

        if (final) {
          existing.explicit = true;
        }

        continue;
      }

      if (expectedKind === "directory") {
        directoryCount++;

        if (
          directoryCount > maxDirectoryCount
        ) {
          throw new SourcePreparationError(
            "FILE_COUNT_EXCEEDED",
            "Archive exceeds its directory count limit.",
          );
        }
      }

      registered.set(key, {
        original,
        kind: expectedKind,
        explicit: final,
      });
    }
  };

  // ================================================
  // Bounded metadata reading
  // ================================================

  const readMetadata = async (
    size: number,
  ): Promise<Buffer> => {
    metadataEntries++;

    if (
      size <= 0 ||
      size > MAX_METADATA_BYTES ||
      metadataEntries > MAX_METADATA_ENTRIES ||
      size >
        MAX_TOTAL_METADATA_BYTES -
          metadataBytes
    ) {
      return unsafe(
        "Archive metadata exceeds its limits.",
      );
    }

    metadataBytes += size;

    const content =
      await reader.requireExact(size);

    await reader.skipPadding(
      paddingFor(size),
    );

    return content;
  };

  // ================================================
  // Secure regular file extraction
  // ================================================

  const writeRegularFile = async (
    segments: string[],
    size: number,
  ): Promise<void> => {
    if (size > limits.maxFileBytes) {
      throw new SourcePreparationError(
        "FILE_SIZE_EXCEEDED",
        "Archive file exceeds its size limit.",
      );
    }

    if (
      size >
      limits.maxExtractedBytes -
        extractedBytes
    ) {
      throw new SourcePreparationError(
        "EXTRACTION_SIZE_EXCEEDED",
        "Archive exceeds its extracted size limit.",
      );
    }

    if (fileCount >= limits.maxFileCount) {
      throw new SourcePreparationError(
        "FILE_COUNT_EXCEEDED",
        "Archive exceeds its file count limit.",
      );
    }

    registerPath(segments, "file");

    const parent = await ensureDirectory(
      root,
      segments.slice(0, -1),
    );

    const filename =
      segments[segments.length - 1];

    if (!filename) {
      return unsafe(
        "Archive file name is invalid.",
      );
    }

    const target = join(parent, filename);

    assertContained(root, target);

    ensureActive();

    let handle;

    try {
      handle = await open(
        target,
        "wx",
        FILE_MODE,
      );
    } catch (error) {
      if (isAlreadyExists(error)) {
        return unsafe(
          "Archive attempts to overwrite an existing file.",
        );
      }

      throw error;
    }

    try {
      let remaining = size;

      while (remaining > 0) {
        ensureActive();

        const chunk =
          await reader.requireExact(
            Math.min(
              remaining,
              READ_CHUNK,
            ),
          );

        let offset = 0;

        // FileHandle.write may perform a partial write.

        while (offset < chunk.length) {
          ensureActive();

          const { bytesWritten } =
            await handle.write(
              chunk,
              offset,
              chunk.length - offset,
              null,
            );

          if (bytesWritten <= 0) {
            return workspaceUnavailable(
              "Archive file could not be written.",
            );
          }

          offset += bytesWritten;
        }

        remaining -= chunk.length;
        extractedBytes += chunk.length;
      }
    } finally {
      await handle.close();
    }

    fileCount++;
  };

  // ================================================
  // Main TAR processing loop
  // ================================================

  try {
    for (;;) {
      ensureActive();

      const header =
        await reader.requireExact(BLOCK);

      if (
        header.every((byte) => byte === 0)
      ) {
        if (pendingMetadata) {
          return malformed(
            "Archive contains orphaned extended metadata.",
          );
        }

        const second =
          await reader.requireExact(BLOCK);

        if (
          !second.every((byte) => byte === 0)
        ) {
          return malformed(
            "Invalid TAR termination blocks.",
          );
        }

        // Consume remaining decompressed bytes.
        // This also forces gzip trailer validation.

        await reader.drainZeroPadding();

        break;
      }

      const parsed = parseHeader(header);

      // --------------------------------------------
      // PAX extended metadata
      // --------------------------------------------

      if (
        parsed.typeFlag === "x" ||
        parsed.typeFlag === "g"
      ) {
        const global =
          parsed.typeFlag === "g";

        if (global && pendingMetadata) {
          return malformed(
            "Unexpected global metadata ordering.",
          );
        }

        const content =
          await readMetadata(parsed.size);

        const records = parsePaxRecords(
          content,
          global,
        );

        if (!global) {
          if (
            records.path !== undefined &&
            pendingPath !== undefined
          ) {
            return malformed(
              "Conflicting archive path overrides.",
            );
          }

          if (
            records.size !== undefined &&
            pendingSize !== undefined
          ) {
            return malformed(
              "Conflicting archive size overrides.",
            );
          }

          pendingPath =
            records.path ?? pendingPath;

          pendingSize =
            records.size ?? pendingSize;

          pendingMetadata = true;
        }

        continue;
      }

      // --------------------------------------------
      // GNU long filename
      // --------------------------------------------

      if (parsed.typeFlag === "L") {
        if (pendingPath !== undefined) {
          return malformed(
            "Conflicting archive long-name metadata.",
          );
        }

        const content =
          await readMetadata(parsed.size);

        const zeroIndex = content.indexOf(0);

        if (
          zeroIndex <= 0 ||
          content
            .subarray(zeroIndex)
            .some((byte) => byte !== 0)
        ) {
          return malformed(
            "Invalid GNU long filename.",
          );
        }

        pendingPath = decodeUtf8(
          content.subarray(0, zeroIndex),
        );

        pendingMetadata = true;

        continue;
      }

      // GNU long-link metadata is never allowed.

      if (parsed.typeFlag === "K") {
        return unsafe(
          "Archive long-link metadata is unsupported.",
        );
      }

      // --------------------------------------------
      // Resolve effective entry metadata
      // --------------------------------------------

      const effectiveName =
        pendingPath ?? parsed.name;

      const effectiveSize =
        pendingSize ?? parsed.size;

      pendingPath = undefined;
      pendingSize = undefined;
      pendingMetadata = false;

      const segments =
        validateArchivePath(effectiveName);

      // --------------------------------------------
      // Directory entry
      // --------------------------------------------

      if (parsed.typeFlag === "5") {
        if (effectiveSize !== 0) {
          return malformed(
            "Archive directory has unexpected content.",
          );
        }

        registerPath(
          segments,
          "directory",
        );

        await ensureDirectory(
          root,
          segments,
        );

        continue;
      }

      // --------------------------------------------
      // Regular file entry
      // --------------------------------------------

      if (
        parsed.typeFlag === "0" ||
        parsed.typeFlag === "\0"
      ) {
        if (effectiveName.endsWith("/")) {
          return malformed(
            "Archive regular file has a directory name.",
          );
        }

        await writeRegularFile(
          segments,
          effectiveSize,
        );

        // Padding must use the effective PAX size.

        await reader.skipPadding(
          paddingFor(effectiveSize),
        );

        continue;
      }

      // Reject links, devices, FIFOs, sparse files,
      // contiguous files and unknown entry types.

      return unsafe(
        "Archive contains an unsupported entry type.",
      );
    }

    await completion;

    ensureActive();

    return {
      extractedBytes,
      fileCount,
      directoryCount,
    };
  } catch (error) {
    if (error instanceof SourcePreparationError) {
      throw error;
    }

    ensureActive();

    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error
    ) {
      const code = error.code;

      if (
        code === "ENOSPC" ||
        code === "EDQUOT"
      ) {
        throw new SourcePreparationError(
          "INSUFFICIENT_DISK_SPACE",
          "Insufficient storage during archive extraction.",
        );
      }

      if (
        typeof code === "string" &&
        code.startsWith("Z_")
      ) {
        return malformed(
          "Compressed archive integrity verification failed.",
        );
      }
    }

    return workspaceUnavailable(
      "Source archive extraction could not be completed.",
    );
  } finally {
    archiveStream.destroy();
    gunzip.destroy();

    await completion.catch(
      () => undefined,
    );
  }
}

// ==================================================
// Restricted cleanup utility
// ==================================================

export async function safeRemoveTree(
  path: string,
  allowedRoot?: string,
): Promise<void> {
  // Preserve the callable signature temporarily,
  // but require an explicit trusted cleanup root.

  if (!allowedRoot) {
    return workspaceUnavailable(
      "A trusted cleanup root is required.",
    );
  }

  const root = resolve(allowedRoot);
  const target = resolve(path);

  if (
    root === parse(root).root ||
    target === root
  ) {
    return unsafe(
      "Refusing to remove a protected root.",
    );
  }

  assertContained(root, target);

  try {
    await assertSafeDirectory(root);

    const canonicalRoot =
      await realpath(root);

    const segments = relative(
      root,
      target,
    ).split(sep);

    let current = root;

    for (const segment of segments) {
      current = join(current, segment);

      await assertSafeDirectory(current);
    }

    const canonicalTarget =
      await realpath(target);

    assertContained(
      canonicalRoot,
      canonicalTarget,
    );

    if (canonicalTarget === canonicalRoot) {
      return unsafe(
        "Refusing to remove the cleanup root.",
      );
    }

    await rm(target, {
      recursive: true,
      force: true,
      maxRetries: 3,
      retryDelay: 100,
    });
  } catch (error) {
    if (isMissingPath(error)) {
      return;
    }

    if (error instanceof SourcePreparationError) {
      throw error;
    }

    return workspaceUnavailable(
      "Source directory cleanup failed.",
    );
  }
}