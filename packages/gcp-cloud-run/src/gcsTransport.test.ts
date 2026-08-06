/**
 * Unit tests for the GCS URI parser + tar helpers. Real GCS network calls
 * are covered by the dispatch tests in `handler.test.ts` via a fake
 * Storage client; here we pin the lower-level pure helpers.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { formatGcsUri, parseGcsUri, tarDirectory, untarDirectory } from "./gcsTransport.js";

let scratchRoot: string;

beforeAll(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), "hf-gcstransport-test-"));
});

afterAll(() => {
  rmSync(scratchRoot, { recursive: true, force: true });
});

describe("parseGcsUri", () => {
  it("parses a simple bucket+object URI", () => {
    expect(parseGcsUri("gs://my-bucket/path/to/object.zip")).toEqual({
      bucket: "my-bucket",
      object: "path/to/object.zip",
    });
  });

  it("preserves nested objects", () => {
    expect(parseGcsUri("gs://b/a/b/c/d.mp4").object).toBe("a/b/c/d.mp4");
  });

  it("throws on non-gs schemes", () => {
    expect(() => parseGcsUri("https://example.com/x")).toThrow(/expected gs:\/\//);
  });

  it("throws on missing object", () => {
    expect(() => parseGcsUri("gs://bucket-only")).toThrow(/missing object/);
  });

  it("throws on empty bucket", () => {
    expect(() => parseGcsUri("gs:///someobject")).toThrow(/empty bucket or object/);
  });
});

describe("formatGcsUri", () => {
  it("round-trips with parseGcsUri", () => {
    const uri = "gs://my-bucket/path/to/object.zip";
    expect(formatGcsUri(parseGcsUri(uri))).toBe(uri);
  });
});

describe("tar round-trip", () => {
  it("tars a directory and untars to identical contents", async () => {
    const sourceDir = join(scratchRoot, "src");
    const destDir = join(scratchRoot, "dest");
    const tarPath = join(scratchRoot, "out.tar.gz");

    const { mkdirSync } = await import("node:fs");
    mkdirSync(join(sourceDir, "nested"), { recursive: true });
    writeFileSync(join(sourceDir, "top.txt"), "hello-top");
    writeFileSync(join(sourceDir, "nested", "inner.txt"), "hello-inner");

    await tarDirectory(sourceDir, tarPath);
    await untarDirectory(tarPath, destDir);

    expect(readFileSync(join(destDir, "top.txt"), "utf-8")).toBe("hello-top");
    expect(readFileSync(join(destDir, "nested", "inner.txt"), "utf-8")).toBe("hello-inner");
  });

  it("wipes the destination before extracting", async () => {
    const sourceDir = join(scratchRoot, "src2");
    const destDir = join(scratchRoot, "dest2");
    const tarPath = join(scratchRoot, "out2.tar.gz");

    const { mkdirSync } = await import("node:fs");
    mkdirSync(sourceDir, { recursive: true });
    writeFileSync(join(sourceDir, "fresh.txt"), "new");

    mkdirSync(destDir, { recursive: true });
    writeFileSync(join(destDir, "stale.txt"), "leftover");

    await tarDirectory(sourceDir, tarPath);
    await untarDirectory(tarPath, destDir);

    expect(readFileSync(join(destDir, "fresh.txt"), "utf-8")).toBe("new");
    const { existsSync } = await import("node:fs");
    expect(existsSync(join(destDir, "stale.txt"))).toBe(false);
  });
});
