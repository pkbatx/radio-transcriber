# Suggested Fixes and Improvements

## Robustness and resource management
- **Close HTTP response bodies inside loops:** `sendToGroupMe` defers `resp.Body.Close()` inside the loop, so responses stay open until the function exits. Close each body immediately after checking status to avoid file-descriptor leaks and to let persistent connections be reused sooner.【F:main.go†L367-L411】
- **Avoid blocking on an unused channel in the watcher:** `watchDirectory` creates a `done` channel but never signals it, which prevents graceful shutdown and makes the goroutine leak when tests try to stop the watcher. Refactor to return an error and allow callers to stop via context or to close `done` on fatal errors.【F:main.go†L133-L169】

## Error handling and validation
- **Handle missing watch directory early:** `watchDirectory` assumes the directory exists; when it does not, `watcher.Add` fails at runtime. Validate `watchDir` in `init()` or before starting the watcher and create the directory (or return a clear error) to fail fast with actionable logs.【F:main.go†L63-L168】
- **Preserve API error details:** `transcribeAudio` and `postProcessTranscription` return raw response bodies on non-200 codes. Include HTTP status, request context (e.g., file name), and possibly the JSON error message to make operational troubleshooting easier.【F:main.go†L204-L365】

## Performance and correctness
- **Use `math.Ceil` for message chunking:** The current calculation for GroupMe chunk count uses integer division and manual remainder handling. Using `math.Ceil(float64(len)/float64(maxMessageLength))` or iterating while slicing until empty simplifies the logic and avoids off-by-one issues.【F:main.go†L367-L384】
- **Release processed-file entries:** `processedFiles` grows indefinitely as more MP3s arrive. Track a bounded cache (e.g., LRU keyed by filename) or periodically prune entries after successful processing to avoid unbounded memory growth on long-running deployments.【F:main.go†L31-L201】

## Observability and security
- **Mask secrets in logs:** Failures when hitting OpenAI or GroupMe currently log raw errors; ensure logs never include API keys or tokens, and consider redacting `Authorization` headers before logging.【F:main.go†L204-L365】
- **Add structured logging:** Replace bare `log.Println/Printf` calls with structured logging (e.g., `log/slog`) to include fields like `fileName`, `status`, and `duration`, improving traceability for production incidents.【F:main.go†L133-L411】

## UX and resiliency improvements
- **Expose health and metrics endpoints:** Add `/healthz` and `/metrics` handlers alongside `/transcriptions` so orchestrators can probe liveness/readiness and operators can monitor throughput, error counts, and latency.【F:main.go†L433-L452】
- **Clarify template link origin:** The transcription list links to an external host derived only from `FileName`. Accept a base URL via configuration or generate signed URLs so links remain valid across environments without hardcoding production domains.【F:templates/transcriptions.html†L17-L31】
