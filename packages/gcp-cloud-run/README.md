# @hyperframes/gcp-cloud-run

Google Cloud adapter for HyperFrames distributed rendering — the Google Cloud counterpart of
[`@hyperframes/aws-lambda`](../aws-lambda). Tracks [issue #932](https://github.com/heygen-com/hyperframes/issues/932).

The distributed rendering primitives in [`@hyperframes/producer/distributed`](../producer)
(`plan`, `renderChunk`, `assemble`) are provider-agnostic and reused **unchanged**. This package
only supplies the Google Cloud deployment, orchestration, and storage adapters.

## Architecture

| Role                                           | AWS (`aws-lambda`) | GCP (`gcp-cloud-run`)                                                |
| ---------------------------------------------- | ------------------ | -------------------------------------------------------------------- |
| Muscle — runs plan / renderChunk / assemble    | Lambda             | **Cloud Run** (container; `node:http` worker dispatched on `action`) |
| Brain — order + parallel chunk fan-out         | Step Functions     | **Cloud Workflows**                                                  |
| Shared disk — artifact handoff                 | S3                 | **Cloud Storage**                                                    |
| Remote control — start + monitor (client side) | SDK                | **SDK**                                                              |

Cloud Run is chosen over Cloud Functions so the worker ships its own container image with Chrome and
ffmpeg installed (no `@sparticuz/chromium` bundling workaround). Cloud Functions 2nd gen runs on
Cloud Run anyway, and Cloud Run scales to zero / bills per request, so this costs no more.

## Status

Scaffolding only. The three tiers are implemented incrementally:

- **Handler** (`./handler`) — Cloud Run worker: download from GCS → call the producer primitive → upload to GCS.
- **SDK** (`./sdk`) — client-side helpers: `deploySite`, `renderToCloudRun`, `getRenderProgress`, cost accounting.
- **Deploy** — Terraform module + `Dockerfile` + the Cloud Workflows definition.
