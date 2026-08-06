/**
 * @hyperframes/gcp-cloud-run — Google Cloud adapter for HyperFrames distributed rendering.
 *
 * Mirrors @hyperframes/aws-lambda for Google Cloud: Cloud Run (compute) +
 * Cloud Workflows (orchestration) + Cloud Storage (artifact handoff), reusing the
 * provider-agnostic primitives from @hyperframes/producer/distributed
 * (plan, renderChunk, assemble) unchanged. See issue #932.
 *
 * Subpaths:
 *   ./handler — Cloud Run worker (node:http; dispatched on action)
 *   ./sdk     — client-side helpers (start render via Cloud Workflows, poll progress)
 */

export { createHandlerServer, dispatch, type HandlerDeps } from "./handler.js";
export {
  type AssembleEvent,
  type AssembleHandlerResult,
  type HandlerAction,
  type HandlerEvent,
  type HandlerResult,
  type PlanEvent,
  type PlanHandlerResult,
  type RenderChunkEvent,
  type RenderChunkHandlerResult,
  type SerializableDistributedRenderConfig,
} from "./events.js";
export { type DistributedFormat, formatExtension } from "./formatExtension.js";
export {
  downloadGcsObjectToFile,
  formatGcsUri,
  type GcsLocation,
  parseGcsUri,
  tarDirectory,
  untarDirectory,
  uploadFileToGcs,
} from "./gcsTransport.js";

// ── Client-side SDK ─────────────────────────────────────────────────────────
// Implemented in a follow-on tier (deploySite / renderToCloudRun /
// getRenderProgress / validateConfig / costAccounting) — see issue #932.
