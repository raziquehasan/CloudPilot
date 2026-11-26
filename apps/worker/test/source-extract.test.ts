import assert from "node:assert/strict";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
  lstat,
} from "node:fs/promises";

import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  SourcePreparationError,
  type SourceFailureCode,
} from "@cloudpilot/queue";

import {
  extractTarGzSafely,
  validateArchivePath,
} from "../src/source/extract.ts";

// --------------------------------------------------
// TAR fixture configuration
// --------------------------------------------------

const BLOCK = 512;

const limits = {
  maxExtractedBytes: 10 * 1024 * 1024,
  maxFileBytes: 1024 * 1024,
  maxFileCount: 1000,
};

interface TarEntry {
  name: string;
  body?: Buffer;
  typeFlag?: string;
  linkName?: string;
}

// --------------------------------------------------
// TAR archive helpers
// --------------------------------------------------

function octal(
  value: number,
  length: number,
): Buffer {
  return Buffer.from(
    value.toString(8).padStart(length - 1, "0") + "\0",
    "ascii",
  );
}

function header(
  name: string,
  size: number,
  typeFlag: string,
  linkName = "",
): Buffer {
  const buffer = Buffer.alloc(BLOCK);

  buffer.write(name, 0, 100, "utf8");

  octal(0o644, 8).copy(buffer, 100);
  octal(0, 8).copy(buffer, 108);
  octal(0, 8).copy(buffer, 116);
  octal(size, 12).copy(buffer, 124);
  octal(0, 12).copy(buffer, 136);

  // Checksum field must contain spaces while calculating.
  buffer.fill(32, 148, 156);

  buffer.write(typeFlag, 156, 1, "ascii");
  buffer.write(linkName, 157, 100, "utf8");

  // POSIX USTAR format.
  buffer.write("ustar\0", 257, 6, "ascii");
  buffer.write("00", 263, 2, "ascii");

  let sum = 0;

  for (let index = 0; index < BLOCK; index += 1) {
    sum += buffer[index] ?? 0;
  }

  buffer.write(
    sum.toString(8).padStart(6, "0") + "\0 ",
    148,
    8,
    "ascii",
  );

  return buffer;
}

function buildTar(entries: TarEntry[]): Buffer {
  const chunks: Buffer[] = [];

  for (const entry of entries) {
    const body = entry.body ?? Buffer.alloc(0);
    const typeFlag = entry.typeFlag ?? "0";

    chunks.push(
      header(
        entry.name,
        typeFlag === "5" ? 0 : body.length,
        typeFlag,
        entry.linkName ?? "",
      ),
    );

    if (typeFlag !== "5" && body.length > 0) {
      chunks.push(body);

      const padding =
        (BLOCK - (body.length % BLOCK)) % BLOCK;

      if (padding > 0) {
        chunks.push(Buffer.alloc(padding));
      }
    }
  }

  // TAR end-of-archive marker.
  chunks.push(Buffer.alloc(BLOCK * 2));

  return Buffer.concat(chunks);
}

// --------------------------------------------------
// Isolated test workspace
// --------------------------------------------------

async function withTempDir(
  fn: (dir: string) => Promise<void>,
): Promise<void> {
  const dir = await mkdtemp(
    join(tmpdir(), "cp-extract-"),
  );

  try {
    // The hardened extractor requires an existing,
    // isolated destination directory.
    await mkdir(join(dir, "out"), {
      mode: 0o700,
    });

    await fn(dir);
  } finally {
    await rm(dir, {
      recursive: true,
      force: true,
      maxRetries: 3,
      retryDelay: 100,
    });
  }
}

async function writeArchive(
  dir: string,
  entries: TarEntry[],
): Promise<string> {
  const archivePath = join(dir, "a.tar.gz");

  await writeFile(
    archivePath,
    gzipSync(buildTar(entries)),
  );

  return archivePath;
}

// --------------------------------------------------
// Structured error assertions
// --------------------------------------------------

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
      `Expected source failure code ${expectedCode}`,
    );

    return true;
  };
}

// --------------------------------------------------
// Test 1: Valid archive extraction
// --------------------------------------------------

test("extracts a valid archive safely", async () => {
  await withTempDir(async (dir) => {
    const source = "export const x = 1;\n";
    const readme = "# hello\n";

    const archivePath = await writeArchive(dir, [
      {
        name: "src/",
        typeFlag: "5",
      },
      {
        name: "src/index.ts",
        body: Buffer.from(source),
      },
      {
        name: "README.md",
        body: Buffer.from(readme),
      },
    ]);

    const destinationDir = join(dir, "out");

    const result = await extractTarGzSafely({
      archivePath,
      destinationDir,
      limits,
    });

    assert.equal(result.fileCount, 2);

    assert.equal(
      result.extractedBytes,
      Buffer.byteLength(source) +
        Buffer.byteLength(readme),
    );

    const rootFiles = await readdir(destinationDir);

    assert.deepEqual(
      rootFiles.sort(),
      ["README.md", "src"].sort(),
    );

    const sourceFiles = await readdir(
      join(destinationDir, "src"),
    );

    assert.deepEqual(sourceFiles, ["index.ts"]);

    assert.equal(
      await readFile(
        join(destinationDir, "src", "index.ts"),
        "utf8",
      ),
      source,
    );

    assert.equal(
      await readFile(
        join(destinationDir, "README.md"),
        "utf8",
      ),
      readme,
    );
  });
});

// --------------------------------------------------
// Test 2: Path traversal protection
// --------------------------------------------------

test("rejects path traversal entries", async () => {
  await withTempDir(async (dir) => {
    const archivePath = await writeArchive(dir, [
      {
        name: "../escape.txt",
        body: Buffer.from("unsafe"),
      },
    ]);

    await assert.rejects(
      extractTarGzSafely({
        archivePath,
        destinationDir: join(dir, "out"),
        limits,
      }),
      expectSourceFailure("UNSAFE_ARCHIVE"),
    );

    // Confirm nothing was written outside the destination.
    await assert.rejects(
      lstat(join(dir, "escape.txt")),
      { code: "ENOENT" },
    );
  });
});

// --------------------------------------------------
// Test 3: Absolute and Windows paths
// --------------------------------------------------

test("rejects absolute and windows drive paths", () => {
  const unsafePaths = [
    "/etc/passwd",
    "C:/Windows/system32",
    "C:\\secret",
    "C:relative.txt",
    "\\\\server\\share\\file.txt",
    "../escape.txt",
    "src/../../escape.txt",
    "src\\file.txt",
  ];

  for (const name of unsafePaths) {
    assert.throws(
      () => validateArchivePath(name),
      expectSourceFailure("UNSAFE_ARCHIVE"),
      `Expected rejection of ${name}`,
    );
  }
});

// --------------------------------------------------
// Test 4: Symlink and hardlink protection
// --------------------------------------------------

test("rejects symlink and hardlink entries", async () => {
  for (const typeFlag of ["1", "2"]) {
    await withTempDir(async (dir) => {
      const archivePath = await writeArchive(dir, [
        {
          name: "unsafe-link",
          typeFlag,
          linkName: "/etc/passwd",
        },
      ]);

      const destinationDir = join(dir, "out");

      await assert.rejects(
        extractTarGzSafely({
          archivePath,
          destinationDir,
          limits,
        }),
        expectSourceFailure("UNSAFE_ARCHIVE"),
      );

      await assert.rejects(
        lstat(join(destinationDir, "unsafe-link")),
        { code: "ENOENT" },
      );
    });
  }
});

// --------------------------------------------------
// Test 5: Duplicate and case-insensitive collisions
// --------------------------------------------------

test("rejects duplicate and case-insensitive collisions", async () => {
  const cases: TarEntry[][] = [
    [
      {
        name: "a.txt",
        body: Buffer.from("first"),
      },
      {
        name: "a.txt",
        body: Buffer.from("second"),
      },
    ],
    [
      {
        name: "A.txt",
        body: Buffer.from("first"),
      },
      {
        name: "a.txt",
        body: Buffer.from("second"),
      },
    ],
    [
      {
        name: "Src/a.txt",
        body: Buffer.from("first"),
      },
      {
        name: "src/b.txt",
        body: Buffer.from("second"),
      },
    ],
  ];

  for (const entries of cases) {
    await withTempDir(async (dir) => {
      const archivePath = await writeArchive(
        dir,
        entries,
      );

      await assert.rejects(
        extractTarGzSafely({
          archivePath,
          destinationDir: join(dir, "out"),
          limits,
        }),
        expectSourceFailure("UNSAFE_ARCHIVE"),
      );
    });
  }
});

// --------------------------------------------------
// Test 6: File count limit
// --------------------------------------------------

test("enforces file count limit", async () => {
  await withTempDir(async (dir) => {
    const archivePath = await writeArchive(dir, [
      {
        name: "a.txt",
        body: Buffer.from("1"),
      },
      {
        name: "b.txt",
        body: Buffer.from("2"),
      },
    ]);

    await assert.rejects(
      extractTarGzSafely({
        archivePath,
        destinationDir: join(dir, "out"),
        limits: {
          ...limits,
          maxFileCount: 1,
        },
      }),
      expectSourceFailure("FILE_COUNT_EXCEEDED"),
    );
  });
});

// --------------------------------------------------
// Test 7: Aggregate extraction size limit
// --------------------------------------------------

test("enforces extracted size limit", async () => {
  await withTempDir(async (dir) => {
    // Each file is individually within the file limit.
    // Their combined size exceeds the extraction limit.
    const archivePath = await writeArchive(dir, [
      {
        name: "first.txt",
        body: Buffer.alloc(600, 1),
      },
      {
        name: "second.txt",
        body: Buffer.alloc(600, 2),
      },
    ]);

    await assert.rejects(
      extractTarGzSafely({
        archivePath,
        destinationDir: join(dir, "out"),
        limits: {
          ...limits,
          maxExtractedBytes: 1000,
          maxFileBytes: 800,
        },
      }),
      expectSourceFailure(
        "EXTRACTION_SIZE_EXCEEDED",
      ),
    );
  });
});

// --------------------------------------------------
// Test 8: Individual file size limit
// --------------------------------------------------

test("enforces per-file size limit", async () => {
  await withTempDir(async (dir) => {
    const archivePath = await writeArchive(dir, [
      {
        name: "big.txt",
        body: Buffer.alloc(5000, 1),
      },
    ]);

    await assert.rejects(
      extractTarGzSafely({
        archivePath,
        destinationDir: join(dir, "out"),
        limits: {
          ...limits,
          maxFileBytes: 1000,
        },
      }),
      expectSourceFailure("FILE_SIZE_EXCEEDED"),
    );
  });
});

// --------------------------------------------------
// Test 9: Windows filesystem security
// --------------------------------------------------

test("rejects windows reserved names and alternate data streams", () => {
  const unsafeNames = [
    "CON.txt",
    "aux",
    "NUL",
    "PRN.txt",
    "COM1.txt",
    "LPT1.txt",
    "file.txt:stream",
    "trailing.",
    "trailing ",
    "src/CON.txt",
    "src/file.txt:stream",
  ];

  for (const name of unsafeNames) {
    assert.throws(
      () => validateArchivePath(name),
      expectSourceFailure("UNSAFE_ARCHIVE"),
      `Expected rejection of ${name}`,
    );
  }
});