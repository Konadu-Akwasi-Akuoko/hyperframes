/**
 * Cloud Run worker for HyperFrames distributed rendering.
 *
 * One container image, three roles. Cloud Workflows dispatches by POSTing a
 * JSON body whose `Action` field selects the role; the worker forwards to the
 * matching OSS primitive from `@hyperframes/producer/distributed`.
 *
 * Everything heavy — capture, encode, audio mix — happens inside the OSS
 * primitives. The handler is thin glue: parse request → GCS download → call
 * primitive → GCS upload → return small JSON result.
 *
 * Chrome and ffmpeg are provided by the container image (the Dockerfile
 * installs them and sets `PRODUCER_HEADLESS_SHELL_PATH`); unlike the Lambda
 * adapter there is no runtime binary resolution or PATH priming here.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { Storage } from "@google-cloud/storage";
import {
  assemble,
  type AssembleResult,
  type ChunkResult,
  type DistributedRenderConfig,
  plan,
  type PlanResult,
  renderChunk,
} from "@hyperframes/producer/distributed";
import type {
  AssembleEvent,
  AssembleHandlerResult,
  HandlerAction,
  HandlerEvent,
  HandlerResult,
  PlanEvent,
  PlanHandlerResult,
  RenderChunkEvent,
  RenderChunkHandlerResult,
} from "./events.js";
import { type DistributedFormat, formatExtension } from "./formatExtension.js";
import {
  downloadGcsObjectToFile,
  parseGcsUri,
  tarDirectory,
  untarDirectory,
  uploadFileToGcs,
} from "./gcsTransport.js";

/**
 * Lazily-constructed Cloud Storage client. Cached at module scope so a warm
 * Cloud Run container instance reuses the underlying HTTP keep-alive pool
 * across requests. `new Storage()` resolves credentials via Application
 * Default Credentials (the service's attached service account).
 */
let cachedStorage: Storage | null = null;
function getStorageClient(): Storage {
  if (cachedStorage) return cachedStorage;
  cachedStorage = new Storage();
  return cachedStorage;
}

/**
 * Optional injection points used by the handler's unit tests. Production
 * callers leave these unset; the real OSS primitives and a default `Storage`
 * client are used.
 */
export interface HandlerDeps {
  storage?: Storage;
  primitives?: {
    plan: typeof plan;
    renderChunk: typeof renderChunk;
    assemble: typeof assemble;
  };
  /** Override the per-request workdir root (defaults to the OS tmpdir). */
  tmpRoot?: string;
}

/** Client-side (non-retryable) handler error — maps to HTTP 400. */
class HandlerBadRequestError extends Error {
  override readonly name = "HandlerBadRequestError";
}

/**
 * Dispatch a single render request to the matching primitive. Pure async
 * function with no HTTP coupling so it can be unit-tested directly and reused
 * by `createHandlerServer`.
 */
export async function dispatch(event: HandlerEvent, deps?: HandlerDeps): Promise<HandlerResult> {
  // Single structured boot log line — Cloud Logging ingests each stdout JSON
  // line as a structured entry, so this is greppable by Action when triaging.
  logEvent({ event: "handler_start", action: actionOf(event), input: summarizeEvent(event) });
  try {
    switch (event.Action) {
      case "plan":
        return await handlePlan(event, deps);
      case "renderChunk":
        return await handleRenderChunk(event, deps);
      case "assemble":
        return await handleAssemble(event, deps);
      default: {
        // Compile-time exhaustiveness: a new HandlerAction member trips the
        // `never` assignment before the runtime error is reachable.
        const _exhaustive: never = event;
        throw new HandlerBadRequestError(
          `[handler] unknown Action: ${JSON.stringify(
            (_exhaustive as { Action?: string }).Action,
          )}. Expected one of "plan", "renderChunk", "assemble".`,
        );
      }
    }
  } catch (err) {
    logEvent({
      event: "handler_error",
      action: actionOf(event),
      message: err instanceof Error ? err.message : String(err),
      name: err instanceof Error ? err.name : undefined,
    });
    throw err;
  }
}

function actionOf(event: HandlerEvent): string {
  return (event as { Action?: string }).Action ?? "unknown";
}

/** Emit a single JSON line to stdout for Cloud Logging to ingest as structured data. */
function logEvent(payload: Record<string, unknown>): void {
  console.log(JSON.stringify(payload));
}

/**
 * Compact, non-PII summary of a request for logging. The full event can
 * include the entire project config; we only emit the routable fields (GCS
 * URIs, chunk index, format) needed to triage a failure from Cloud Logging.
 */
function summarizeEvent(event: HandlerEvent): Record<string, unknown> {
  switch (event.Action) {
    case "plan":
      return {
        projectGcsUri: event.ProjectGcsUri,
        planOutputGcsPrefix: event.PlanOutputGcsPrefix,
        format: event.Config.format,
        fps: event.Config.fps,
      };
    case "renderChunk":
      return { planGcsUri: event.PlanGcsUri, chunkIndex: event.ChunkIndex, format: event.Format };
    case "assemble":
      return {
        planGcsUri: event.PlanGcsUri,
        chunkCount: event.ChunkGcsUris.length,
        hasAudio: event.AudioGcsUri !== null,
        outputGcsUri: event.OutputGcsUri,
        format: event.Format,
      };
    default:
      return { action: actionOf(event) };
  }
}

// ── Plan ────────────────────────────────────────────────────────────────────

async function handlePlan(event: PlanEvent, deps?: HandlerDeps): Promise<PlanHandlerResult> {
  const started = Date.now();
  const storage = deps?.storage ?? getStorageClient();
  const primitive = deps?.primitives?.plan ?? plan;

  const work = mkdtempSync(join(deps?.tmpRoot ?? tmpdir(), "hf-cloudrun-plan-"));
  const projectArchive = join(work, "project.tar.gz");
  const projectDir = join(work, "project");
  const planDir = join(work, "plan");

  try {
    await downloadGcsObjectToFile(storage, event.ProjectGcsUri, projectArchive);
    await untarDirectory(projectArchive, projectDir);

    const config: DistributedRenderConfig = { ...event.Config };
    const result: PlanResult = await primitive(projectDir, config, planDir);

    // Upload the planDir as a single tarball — Cloud Workflows cannot pass a
    // directory-shaped artifact between steps; the consumer (renderChunk /
    // assemble) untars it. Audio is co-located alongside the plan so
    // renderChunk doesn't have to pull it when it isn't relevant to the chunk.
    const planTar = join(work, "plan.tar.gz");
    await tarDirectory(planDir, planTar);
    const planTarUri = `${trimTrailingSlash(event.PlanOutputGcsPrefix)}/plan.tar.gz`;
    const audioPath = join(planDir, "audio.aac");
    const hasAudio = existsSync(audioPath) && statSync(audioPath).size > 0;
    const audioUri = hasAudio ? `${trimTrailingSlash(event.PlanOutputGcsPrefix)}/audio.aac` : null;
    await Promise.all([
      uploadFileToGcs(storage, planTar, planTarUri, "application/gzip"),
      hasAudio && audioUri ? uploadFileToGcs(storage, audioPath, audioUri, "audio/aac") : null,
    ]);

    return {
      Action: "plan",
      PlanGcsUri: planTarUri,
      PlanHash: result.planHash,
      ChunkCount: result.chunkCount,
      TotalFrames: result.totalFrames,
      Fps: result.fps,
      Width: result.width,
      Height: result.height,
      Format: result.format,
      HasAudio: audioUri !== null,
      AudioGcsUri: audioUri,
      FfmpegVersion: result.ffmpegVersion,
      ProducerVersion: result.producerVersion,
      DurationMs: Date.now() - started,
    };
  } finally {
    cleanupDir(work);
  }
}

// ── RenderChunk ─────────────────────────────────────────────────────────────

async function handleRenderChunk(
  event: RenderChunkEvent,
  deps?: HandlerDeps,
): Promise<RenderChunkHandlerResult> {
  const started = Date.now();
  const storage = deps?.storage ?? getStorageClient();
  const primitive = deps?.primitives?.renderChunk ?? renderChunk;

  const work = mkdtempSync(join(deps?.tmpRoot ?? tmpdir(), "hf-cloudrun-chunk-"));
  const planTar = join(work, "plan.tar.gz");
  const planDir = join(work, "plan");

  try {
    await downloadGcsObjectToFile(storage, event.PlanGcsUri, planTar);
    await untarDirectory(planTar, planDir);

    // Verify the plan's hash matches what the workflow told us to render.
    // The producer's renderChunk re-checks internally (defense-in-depth), but
    // doing it here lets us fail before paying the Chrome-launch + render cost
    // on a misrouted chunk. Throws a typed PLAN_HASH_MISMATCH the workflow can
    // route as non-retryable.
    verifyPlanHash(planDir, event.PlanHash);

    const chunkOutputBase = join(
      work,
      event.Format === "png-sequence"
        ? `chunk-${pad(event.ChunkIndex)}`
        : `chunk-${pad(event.ChunkIndex)}${formatExtension(event.Format)}`,
    );

    const result: ChunkResult = await primitive(planDir, event.ChunkIndex, chunkOutputBase);

    const chunkUri = await uploadChunkOutput(
      storage,
      result,
      event.ChunkOutputGcsPrefix,
      event.ChunkIndex,
    );

    return {
      Action: "renderChunk",
      ChunkGcsUri: chunkUri,
      ChunkIndex: event.ChunkIndex,
      Sha256: result.sha256,
      FramesEncoded: result.framesEncoded,
      DurationMs: Date.now() - started,
    };
  } finally {
    cleanupDir(work);
  }
}

async function uploadChunkOutput(
  storage: Storage,
  result: ChunkResult,
  prefix: string,
  chunkIndex: number,
): Promise<string> {
  const trimmed = trimTrailingSlash(prefix);
  if (result.outputKind === "file") {
    const ext = result.outputPath.slice(result.outputPath.lastIndexOf("."));
    const uri = `${trimmed}/chunks/${pad(chunkIndex)}${ext}`;
    await uploadFileToGcs(storage, result.outputPath, uri);
    return uri;
  }
  // frame-dir: upload as a tarball so a single GCS object represents the chunk.
  // Assemble's png-sequence path expects a directory per chunk; it untars on
  // its end.
  const tarball = `${result.outputPath}.tar.gz`;
  await tarDirectory(result.outputPath, tarball);
  const uri = `${trimmed}/chunks/${pad(chunkIndex)}.tar.gz`;
  await uploadFileToGcs(storage, tarball, uri, "application/gzip");
  return uri;
}

// ── Assemble ────────────────────────────────────────────────────────────────

async function handleAssemble(
  event: AssembleEvent,
  deps?: HandlerDeps,
): Promise<AssembleHandlerResult> {
  const started = Date.now();
  const storage = deps?.storage ?? getStorageClient();
  const primitive = deps?.primitives?.assemble ?? assemble;

  const work = mkdtempSync(join(deps?.tmpRoot ?? tmpdir(), "hf-cloudrun-assemble-"));
  const planTar = join(work, "plan.tar.gz");
  const planDir = join(work, "plan");

  try {
    await downloadGcsObjectToFile(storage, event.PlanGcsUri, planTar);
    await untarDirectory(planTar, planDir);

    const chunkPaths = await downloadChunkObjects(storage, event.ChunkGcsUris, work, event.Format);

    let audioPath: string | null = null;
    if (event.AudioGcsUri) {
      audioPath = join(planDir, "audio.aac");
      await downloadGcsObjectToFile(storage, event.AudioGcsUri, audioPath);
    }

    const finalOutput =
      event.Format === "png-sequence"
        ? join(work, "output-frames")
        : join(work, `output${formatExtension(event.Format)}`);

    const result: AssembleResult = await primitive(planDir, chunkPaths, audioPath, finalOutput, {
      cfr: event.Cfr === true,
    });

    if (event.Format === "png-sequence") {
      const tarball = `${finalOutput}.tar.gz`;
      await tarDirectory(finalOutput, tarball);
      await uploadFileToGcs(storage, tarball, event.OutputGcsUri, "application/gzip");
    } else {
      await uploadFileToGcs(storage, finalOutput, event.OutputGcsUri);
    }

    return {
      Action: "assemble",
      OutputGcsUri: event.OutputGcsUri,
      FramesEncoded: result.framesEncoded,
      FileSize: result.fileSize,
      DurationMs: Date.now() - started,
    };
  } finally {
    cleanupDir(work);
  }
}

async function downloadChunkObjects(
  storage: Storage,
  uris: string[],
  workDir: string,
  format: DistributedFormat,
): Promise<string[]> {
  const chunksDir = join(workDir, "chunks");
  mkdirSync(chunksDir, { recursive: true });
  // Each chunk is an independent GCS download (+ untar for png-sequence). Run
  // them in parallel — assemble's wall-clock is otherwise dominated by
  // `Σ chunk-download-ms` instead of `max(chunk-download-ms)`. Preserve input
  // order by writing into a pre-sized array rather than pushing as each
  // task settles.
  const local: string[] = new Array<string>(uris.length);
  await Promise.all(
    uris.map(async (uri, i) => {
      if (!uri) {
        throw new HandlerBadRequestError(`[handler] chunk URI at index ${i} is empty`);
      }
      const { object } = parseGcsUri(uri);
      const localPath = join(chunksDir, basename(object));
      await downloadGcsObjectToFile(storage, uri, localPath);
      if (format === "png-sequence") {
        const dirPath = join(chunksDir, `frames-${pad(i)}`);
        await untarDirectory(localPath, dirPath);
        local[i] = dirPath;
      } else {
        local[i] = localPath;
      }
    }),
  );
  return local;
}

// ── HTTP server ───────────────────────────────────────────────────────────

/**
 * Wrap {@link dispatch} in a `node:http` server — the Cloud Run entry point.
 * Cloud Run sends one HTTP request per invocation; the body is the JSON event
 * Cloud Workflows posted. The HTTP status doubles as the workflow's retry
 * signal: 4xx is non-retryable (bad request / plan-hash mismatch), 5xx is
 * retryable (transient failure). This factory has no `listen()` side effect so
 * the module is import-safe; the deploy tier's entry binds it to `$PORT`.
 */
export function createHandlerServer(deps?: HandlerDeps): Server {
  return createServer((req, res) => {
    void handleRequest(req, res, deps);
  });
}

async function handleRequest(
  req: IncomingMessage,
  res: ServerResponse,
  deps?: HandlerDeps,
): Promise<void> {
  if (req.method === "GET") {
    sendJson(res, 200, { status: "ok" });
    return;
  }
  if (req.method !== "POST") {
    sendJson(res, 405, { error: `method ${req.method ?? "?"} not allowed; use POST` });
    return;
  }

  let event: HandlerEvent;
  try {
    event = JSON.parse(await readBody(req)) as HandlerEvent;
  } catch (err) {
    sendJson(res, 400, { error: `invalid JSON body: ${messageOf(err)}` });
    return;
  }

  try {
    const result = await dispatch(event, deps);
    sendJson(res, 200, result);
  } catch (err) {
    const status = isClientError(err) ? 400 : 500;
    sendJson(res, status, {
      error: messageOf(err),
      name: err instanceof Error ? err.name : undefined,
    });
  }
}

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString("utf-8");
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

/** A bad-request (non-retryable) error: unknown action, malformed event, plan-hash mismatch. */
function isClientError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  return err.name === "HandlerBadRequestError" || err.name === "PLAN_HASH_MISMATCH";
}

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function pad(n: number): string {
  return n.toString().padStart(4, "0");
}

function trimTrailingSlash(prefix: string): string {
  return prefix.endsWith("/") ? prefix.slice(0, -1) : prefix;
}

function cleanupDir(dir: string): void {
  try {
    // A warm container instance is reused across requests; clean up
    // aggressively so we don't leak a chunk-sized footprint between renders.
    rmSync(dir, { recursive: true, force: true });
  } catch {
    // Best-effort — a leak is preferable to crashing on the success path.
  }
}

/**
 * Read the untarred planDir's `plan.json` and assert its `planHash` matches
 * what the request claims. Throws on mismatch with a typed
 * `PLAN_HASH_MISMATCH` error name so the workflow routes it as non-retryable.
 *
 * Defense-in-depth — the producer's `renderChunk` does the same check
 * internally — but performing it here lets us fail before paying the
 * Chrome-launch + per-frame capture cost on a misrouted chunk.
 */
function verifyPlanHash(planDir: string, expected: string): void {
  const planJsonPath = join(planDir, "plan.json");
  let parsed: { planHash?: unknown };
  try {
    parsed = JSON.parse(readFileSync(planJsonPath, "utf-8")) as { planHash?: unknown };
  } catch (err) {
    const error = new Error(
      `PLAN_HASH_MISMATCH: failed to read ${planJsonPath}: ${messageOf(err)}`,
    );
    error.name = "PLAN_HASH_MISMATCH";
    throw error;
  }
  const actual = parsed.planHash;
  if (typeof actual !== "string" || actual !== expected) {
    const error = new Error(
      `PLAN_HASH_MISMATCH: event PlanHash=${expected} did not match plan.json planHash=${String(actual)}`,
    );
    error.name = "PLAN_HASH_MISMATCH";
    throw error;
  }
}

export type { HandlerAction };
