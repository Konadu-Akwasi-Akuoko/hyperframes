/**
 * Handler dispatch unit tests.
 *
 * Asserts that:
 *   - `dispatch` routes Action="plan" / "renderChunk" / "assemble" to the
 *     matching OSS primitive.
 *   - It rejects unknown actions with a clear message.
 *   - It plumbs GCS download/upload calls in the correct order.
 *   - `createHandlerServer` exposes the dispatcher over node:http.
 *
 * The real OSS primitives are NOT exercised here — they live in
 * `@hyperframes/producer/distributed` and have their own coverage in
 * `packages/producer`. The Cloud Run handler is thin glue; this file pins
 * the glue's contract.
 */

import { afterEach, describe, expect, it, mock } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Storage } from "@google-cloud/storage";
import type { AssembleResult, ChunkResult, PlanResult } from "@hyperframes/producer/distributed";
import type { AssembleEvent, HandlerEvent, PlanEvent, RenderChunkEvent } from "./events.js";
import { createHandlerServer, dispatch } from "./handler.js";

interface FakeGcsOp {
  kind: "download" | "upload";
  uri: string;
  bytes: number;
}

type Primitives = NonNullable<import("./handler.js").HandlerDeps["primitives"]>;

/**
 * In-memory Cloud Storage stand-in. Records every operation so test
 * assertions can pin the exact sequence of downloads and uploads, and serves
 * seeded objects through the same `bucket(b).file(o).download({destination})`
 * / `bucket(b).upload(localPath, {destination})` surface `gcsTransport` calls.
 */
class FakeStorage {
  ops: FakeGcsOp[] = [];
  objects = new Map<string, Buffer>();

  bucket(bucketName: string) {
    return {
      file: (object: string) => ({
        download: async (opts: { destination: string }): Promise<void> => {
          const uri = `gs://${bucketName}/${object}`;
          const bytes = this.objects.get(uri) ?? Buffer.alloc(0);
          this.ops.push({ kind: "download", uri, bytes: bytes.length });
          writeFileSync(opts.destination, bytes);
        },
      }),
      upload: async (localPath: string, opts: { destination: string }): Promise<void> => {
        const uri = `gs://${bucketName}/${opts.destination}`;
        const bytes = readFileSync(localPath);
        this.ops.push({ kind: "upload", uri, bytes: bytes.length });
        this.objects.set(uri, bytes);
      },
    };
  }
}

function asStorage(fake: FakeStorage): Storage {
  return fake as unknown as Storage;
}

const tmpDirs: string[] = [];

afterEach(() => {
  for (const dir of tmpDirs) {
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      // Best-effort cleanup.
    }
  }
  tmpDirs.length = 0;
});

function makeTmpRoot(): string {
  const dir = mkdtempSync(join(tmpdir(), "hf-cloudrun-test-"));
  tmpDirs.push(dir);
  return dir;
}

function throwingPrimitive(label: string) {
  return mock(async () => {
    throw new Error(`${label} should not be called`);
  });
}

function primitives(overrides: Partial<Primitives>): Primitives {
  return {
    plan: throwingPrimitive("plan") as unknown as Primitives["plan"],
    renderChunk: throwingPrimitive("renderChunk") as unknown as Primitives["renderChunk"],
    assemble: throwingPrimitive("assemble") as unknown as Primitives["assemble"],
    ...overrides,
  };
}

describe("dispatch", () => {
  it("routes Action='plan' to the plan primitive", async () => {
    const tmpRoot = makeTmpRoot();
    const storage = new FakeStorage();
    storage.objects.set("gs://bucket/project.tar.gz", await makeMinimalProjectTar());

    const planMock = mock(
      async (_projectDir: string, _config: unknown, planDir: string): Promise<PlanResult> => {
        mkdirSync(planDir, { recursive: true });
        writeFileSync(join(planDir, "plan.json"), JSON.stringify({ planHash: "fakehash" }));
        mkdirSync(join(planDir, "meta"), { recursive: true });
        writeFileSync(join(planDir, "meta", "chunks.json"), "[]");
        return {
          planDir,
          planHash: "fakehash",
          chunkCount: 4,
          totalFrames: 720,
          fps: 30 as const,
          width: 1920,
          height: 1080,
          format: "mp4" as const,
          ffmpegVersion: "6.0",
          producerVersion: "0.0.0-test",
        };
      },
    );

    const event: PlanEvent = {
      Action: "plan",
      ProjectGcsUri: "gs://bucket/project.tar.gz",
      PlanOutputGcsPrefix: "gs://bucket/renders/abc/",
      Config: { fps: 30, width: 1920, height: 1080, format: "mp4" },
    };

    const result = await dispatch(event, {
      storage: asStorage(storage),
      primitives: primitives({ plan: planMock as unknown as Primitives["plan"] }),
      tmpRoot,
    });

    expect(result.Action).toBe("plan");
    if (result.Action !== "plan") throw new Error("unreachable");
    expect(result.PlanHash).toBe("fakehash");
    expect(result.ChunkCount).toBe(4);
    expect(planMock).toHaveBeenCalledTimes(1);
    expect(
      storage.ops.some((o) => o.kind === "download" && o.uri === "gs://bucket/project.tar.gz"),
    ).toBe(true);
    expect(storage.ops.some((o) => o.kind === "upload" && o.uri.endsWith("/plan.tar.gz"))).toBe(
      true,
    );
  });

  it("routes Action='renderChunk' to the renderChunk primitive", async () => {
    const tmpRoot = makeTmpRoot();
    const storage = new FakeStorage();
    storage.objects.set("gs://bucket/plan.tar.gz", await makeMinimalPlanTar());

    const renderChunkMock = mock(
      async (
        _planDir: string,
        _chunkIndex: number,
        outputChunkPath: string,
      ): Promise<ChunkResult> => {
        writeFileSync(outputChunkPath, Buffer.from("FAKE-MP4-CHUNK"));
        return {
          outputPath: outputChunkPath,
          outputKind: "file",
          framesEncoded: 240,
          sha256: "0".repeat(64),
          durationMs: 12345,
        } as ChunkResult;
      },
    );

    const event: RenderChunkEvent = {
      Action: "renderChunk",
      PlanGcsUri: "gs://bucket/plan.tar.gz",
      PlanHash: "fakehash",
      ChunkIndex: 2,
      ChunkOutputGcsPrefix: "gs://bucket/renders/abc/",
      Format: "mp4",
    };

    const result = await dispatch(event, {
      storage: asStorage(storage),
      primitives: primitives({
        renderChunk: renderChunkMock as unknown as Primitives["renderChunk"],
      }),
      tmpRoot,
    });

    expect(result.Action).toBe("renderChunk");
    if (result.Action !== "renderChunk") throw new Error("unreachable");
    expect(result.ChunkIndex).toBe(2);
    expect(result.Sha256).toBe("0".repeat(64));
    expect(result.FramesEncoded).toBe(240);
    expect(result.ChunkGcsUri).toBe("gs://bucket/renders/abc/chunks/0002.mp4");
    expect(renderChunkMock).toHaveBeenCalledTimes(1);
  });

  it("rejects renderChunk when event.PlanHash diverges from plan.json", async () => {
    const tmpRoot = makeTmpRoot();
    const storage = new FakeStorage();
    storage.objects.set("gs://bucket/plan.tar.gz", await makeMinimalPlanTar());

    const renderChunkMock = throwingPrimitive("renderChunk (hash mismatch)");

    const event: RenderChunkEvent = {
      Action: "renderChunk",
      PlanGcsUri: "gs://bucket/plan.tar.gz",
      PlanHash: "not-the-real-hash",
      ChunkIndex: 0,
      ChunkOutputGcsPrefix: "gs://bucket/renders/abc/",
      Format: "mp4",
    };

    let caught: unknown;
    try {
      await dispatch(event, {
        storage: asStorage(storage),
        primitives: primitives({
          renderChunk: renderChunkMock as unknown as Primitives["renderChunk"],
        }),
        tmpRoot,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).name).toBe("PLAN_HASH_MISMATCH");
    expect((caught as Error).message).toMatch(/not-the-real-hash/);
    expect(renderChunkMock).not.toHaveBeenCalled();
  });

  it("routes Action='assemble' to the assemble primitive", async () => {
    const tmpRoot = makeTmpRoot();
    const storage = new FakeStorage();
    storage.objects.set("gs://bucket/plan.tar.gz", await makeMinimalPlanTar());
    storage.objects.set("gs://bucket/chunks/0001.mp4", Buffer.from("CHUNK-1"));
    storage.objects.set("gs://bucket/chunks/0002.mp4", Buffer.from("CHUNK-2"));

    const assembleMock = mock(
      async (
        _planDir: string,
        _chunkPaths: readonly string[],
        _audioPath: string | null,
        outputPath: string,
      ): Promise<AssembleResult> => {
        writeFileSync(outputPath, Buffer.from("FAKE-FINAL-MP4"));
        return { outputPath, durationMs: 7777, framesEncoded: 480, fileSize: 14 };
      },
    );

    const event: AssembleEvent = {
      Action: "assemble",
      PlanGcsUri: "gs://bucket/plan.tar.gz",
      ChunkGcsUris: ["gs://bucket/chunks/0001.mp4", "gs://bucket/chunks/0002.mp4"],
      AudioGcsUri: null,
      OutputGcsUri: "gs://bucket/renders/abc/output.mp4",
      Format: "mp4",
    };

    const result = await dispatch(event, {
      storage: asStorage(storage),
      primitives: primitives({ assemble: assembleMock as unknown as Primitives["assemble"] }),
      tmpRoot,
    });

    expect(result.Action).toBe("assemble");
    if (result.Action !== "assemble") throw new Error("unreachable");
    expect(result.OutputGcsUri).toBe("gs://bucket/renders/abc/output.mp4");
    expect(result.FramesEncoded).toBe(480);
    expect(assembleMock).toHaveBeenCalledTimes(1);
    expect(storage.ops.some((o) => o.kind === "upload" && o.uri === event.OutputGcsUri)).toBe(true);
  });

  it("rejects unknown actions", async () => {
    const tmpRoot = makeTmpRoot();
    await expect(
      dispatch({ Action: "doSomething" } as unknown as HandlerEvent, {
        storage: asStorage(new FakeStorage()),
        tmpRoot,
      }),
    ).rejects.toThrow(/unknown Action/);
  });
});

describe("createHandlerServer", () => {
  async function withServer(
    deps: import("./handler.js").HandlerDeps,
    fn: (baseUrl: string) => Promise<void>,
  ): Promise<void> {
    const server = createHandlerServer(deps);
    await new Promise<void>((resolve) => server.listen(0, resolve));
    const { port } = server.address() as AddressInfo;
    try {
      await fn(`http://127.0.0.1:${port}`);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  }

  it("answers GET with 200 for health probes", async () => {
    await withServer({ storage: asStorage(new FakeStorage()) }, async (baseUrl) => {
      const res = await fetch(baseUrl, { method: "GET" });
      expect(res.status).toBe(200);
    });
  });

  it("dispatches a valid POST and returns the JSON result", async () => {
    const tmpRoot = makeTmpRoot();
    const storage = new FakeStorage();
    storage.objects.set("gs://bucket/project.tar.gz", await makeMinimalProjectTar());

    const planMock = mock(
      async (_projectDir: string, _config: unknown, planDir: string): Promise<PlanResult> => {
        mkdirSync(planDir, { recursive: true });
        writeFileSync(join(planDir, "plan.json"), JSON.stringify({ planHash: "fakehash" }));
        return {
          planDir,
          planHash: "fakehash",
          chunkCount: 1,
          totalFrames: 30,
          fps: 30 as const,
          width: 1920,
          height: 1080,
          format: "mp4" as const,
          ffmpegVersion: "6.0",
          producerVersion: "0.0.0-test",
        };
      },
    );

    const event: PlanEvent = {
      Action: "plan",
      ProjectGcsUri: "gs://bucket/project.tar.gz",
      PlanOutputGcsPrefix: "gs://bucket/renders/abc/",
      Config: { fps: 30, width: 1920, height: 1080, format: "mp4" },
    };

    await withServer(
      {
        storage: asStorage(storage),
        primitives: primitives({ plan: planMock as unknown as Primitives["plan"] }),
        tmpRoot,
      },
      async (baseUrl) => {
        const res = await fetch(baseUrl, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(event),
        });
        expect(res.status).toBe(200);
        const body = (await res.json()) as { Action: string; PlanHash: string };
        expect(body.Action).toBe("plan");
        expect(body.PlanHash).toBe("fakehash");
      },
    );
  });

  it("returns 400 on malformed JSON", async () => {
    await withServer({ storage: asStorage(new FakeStorage()) }, async (baseUrl) => {
      const res = await fetch(baseUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{not json",
      });
      expect(res.status).toBe(400);
    });
  });
});

// ── helpers ─────────────────────────────────────────────────────────────────

/**
 * Build the smallest valid `.tar.gz` the handler's untar step accepts: a
 * single file inside an archive. Uses the npm `tar` package (same as
 * `gcsTransport.ts`) so the fixture builder runs cross-platform.
 */
async function makeMinimalProjectTar(): Promise<Buffer> {
  const tar = await import("tar");
  const dir = mkdtempSync(join(tmpdir(), "hf-cloudrun-mktar-"));
  try {
    writeFileSync(join(dir, "index.html"), "<!doctype html><title>test</title>");
    const tarPath = join(dir, "out.tar.gz");
    await tar.create({ gzip: true, file: tarPath, cwd: dir }, ["index.html"]);
    return readFileSync(tarPath);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

/**
 * Build a minimal `.tar.gz` for a tiny planDir containing `plan.json` +
 * `meta/chunks.json`. Used by renderChunk/assemble tests where the handler
 * untars but the mock primitive doesn't inspect contents.
 */
async function makeMinimalPlanTar(): Promise<Buffer> {
  const tar = await import("tar");
  const dir = mkdtempSync(join(tmpdir(), "hf-cloudrun-test-plan-"));
  tmpDirs.push(dir);
  mkdirSync(join(dir, "meta"), { recursive: true });
  writeFileSync(join(dir, "plan.json"), JSON.stringify({ planHash: "fakehash" }));
  writeFileSync(join(dir, "meta", "chunks.json"), "[]");
  const tarPath = join(dir, "out.tar.gz");
  await tar.create({ gzip: true, file: tarPath, cwd: dir }, ["plan.json", "meta"]);
  return readFileSync(tarPath);
}
