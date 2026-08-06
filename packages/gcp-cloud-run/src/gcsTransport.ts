/**
 * Thin Google Cloud Storage transport for the Cloud Run handler.
 *
 * The OSS distributed primitives are pure functions over local file paths;
 * the Cloud Run handler bridges GCS ↔ the container's local filesystem on
 * each request. Functions here are intentionally narrow: parse a URI,
 * download an object to a local path, upload a path, tar-pack a planDir,
 * tar-extract a planDir.
 *
 * Tar (not zip) for planDir transit:
 *   - planDirs contain symlinks (the extract stage materializes them but the
 *     compiled/ subtree may include linked assets); tar preserves them, zip
 *     does not.
 *   - We use the `tar` npm package (pure JS over `node:zlib`) rather than
 *     spawning a system tar binary, so the transport never depends on which
 *     userland tools the container image happens to ship.
 */

import { existsSync, mkdirSync, rmSync, statSync } from "node:fs";
import { dirname } from "node:path";
import { Storage } from "@google-cloud/storage";
import * as tar from "tar";

/** Parsed `gs://bucket/object` URI. */
export interface GcsLocation {
  bucket: string;
  object: string;
}

/** Parse `gs://bucket/object/path` → `{ bucket, object }`. Throws on malformed input. */
export function parseGcsUri(uri: string): GcsLocation {
  if (!uri.startsWith("gs://")) {
    throw new Error(`[gcsTransport] expected gs:// URI, got: ${JSON.stringify(uri)}`);
  }
  const rest = uri.slice("gs://".length);
  const slash = rest.indexOf("/");
  if (slash === -1) {
    throw new Error(`[gcsTransport] missing object in gs URI: ${JSON.stringify(uri)}`);
  }
  const bucket = rest.slice(0, slash);
  const object = rest.slice(slash + 1);
  if (!bucket || !object) {
    throw new Error(`[gcsTransport] empty bucket or object in gs URI: ${JSON.stringify(uri)}`);
  }
  return { bucket, object };
}

/** Build `gs://bucket/object` from a location. */
export function formatGcsUri(loc: GcsLocation): string {
  return `gs://${loc.bucket}/${loc.object}`;
}

/**
 * Download a GCS object to a local file path. `File#download` streams the
 * object to disk and verifies its CRC32C/MD5 checksum, but does not create
 * the destination's parent directory — so we create it first.
 */
export async function downloadGcsObjectToFile(
  storage: Storage,
  uri: string,
  destPath: string,
): Promise<void> {
  const { bucket, object } = parseGcsUri(uri);
  mkdirSync(dirname(destPath), { recursive: true });
  await storage.bucket(bucket).file(object).download({ destination: destPath });
}

/**
 * Upload a local file's contents to a GCS URI. A single `Bucket#upload`
 * handles every artifact this adapter moves (planDirs ≤ 2 GB, chunks
 * ≤ ~200 MB); the client transparently switches to a resumable upload for
 * large objects.
 */
export async function uploadFileToGcs(
  storage: Storage,
  localPath: string,
  uri: string,
  contentType?: string,
): Promise<void> {
  if (!existsSync(localPath)) {
    throw new Error(`[gcsTransport] upload source missing: ${localPath}`);
  }
  const { bucket, object } = parseGcsUri(uri);
  await storage.bucket(bucket).upload(localPath, { destination: object, contentType });
}

/**
 * Pack a directory into a `.tar.gz` at `destTarball`. Uses the `tar` npm
 * package (pure JS over `node:zlib`) rather than spawning a system tar
 * binary, so it never depends on the container image's userland tools and
 * preserves symlinks in the planDir.
 */
export async function tarDirectory(sourceDir: string, destTarball: string): Promise<void> {
  if (!existsSync(sourceDir) || !statSync(sourceDir).isDirectory()) {
    throw new Error(`[gcsTransport] tar source must be an existing directory: ${sourceDir}`);
  }
  mkdirSync(dirname(destTarball), { recursive: true });
  await tar.create({ gzip: true, file: destTarball, cwd: sourceDir }, ["."]);
}

/**
 * Extract a `.tar.gz` produced by {@link tarDirectory} into `destDir`.
 * The directory is wiped before extraction so a reused warm Cloud Run
 * container instance doesn't observe stale files from a prior request.
 */
export async function untarDirectory(tarballPath: string, destDir: string): Promise<void> {
  if (!existsSync(tarballPath)) {
    throw new Error(`[gcsTransport] tarball missing: ${tarballPath}`);
  }
  // Wipe target so a warm container's prior planDir doesn't bleed into the
  // new request — Cloud Run reuses container instances across requests.
  if (existsSync(destDir)) {
    rmSync(destDir, { recursive: true, force: true });
  }
  mkdirSync(destDir, { recursive: true });
  await tar.extract({ file: tarballPath, cwd: destDir });
}
