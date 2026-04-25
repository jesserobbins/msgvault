# GitHub Issue Draft: Embedding Backfill Fails on Empty Post-Processed Messages

> To be filed at: https://github.com/wesm/msgvault/issues

---

## Title

`build-embeddings` aborts when preprocessing yields empty input; 4xx embedding bodies are hidden

## Body

**TL;DR:** `msgvault build-embeddings` can get stuck aborting after repeated 400s when one message in a batch preprocesses down to an empty string. The worker still sends `""` to the embedding endpoint, and some OpenAI-compatible servers reject that with `Invalid embedding input: Input must not be empty`. On top of that, the embedding client currently collapses 4xx responses to `embed: HTTP 400`, which makes the real root cause invisible from the CLI.

I hit this against a local OpenAI-compatible embedding endpoint. The failure was reproducible and persistent until I traced the request body behavior.

### Environment

- `msgvault` built from current `main`
- Vector backend: `sqlite-vec`
- Embedding endpoint: local OpenAI-compatible `/v1/embeddings`
- Model examples tested:
  - `google/embedding-gemma-300m` on a local ANE-backed interface
  - other LM Studio / local embedding-server variants showed the same class of issue when they reject blank inputs

### Config

Example of the working endpoint config shape:

```toml
[vector]
enabled = true

[vector.embeddings]
endpoint = "http://127.0.0.1:1234/v1"
model = "google/embedding-gemma-300m"
dimension = 768
batch_size = 24
```

### What happens

Running:

```bash
msgvault build-embeddings
```

produces repeated batch failures and then aborts:

```text
Resuming building generation 1 (apple-nl-contextual-en:512).
time=2026-04-23T15:47:52.885-07:00 level=WARN msg="embed batch failed" run_id=8ec058bfb2dd gen=1 ids=50 error="embed: embed: HTTP 400: {\"error\":{\"type\":\"embedding_error\",\"message\":\"Invalid embedding input: Input must not be empty\"}}"
time=2026-04-23T15:47:52.896-07:00 level=WARN msg="embed batch failed" run_id=8ec058bfb2dd gen=1 ids=50 error="embed: embed: HTTP 400: {\"error\":{\"type\":\"embedding_error\",\"message\":\"Invalid embedding input: Input must not be empty\"}}"
time=2026-04-23T15:47:52.906-07:00 level=WARN msg="embed batch failed" run_id=8ec058bfb2dd gen=1 ids=50 error="embed: embed: HTTP 400: {\"error\":{\"message\":\"Invalid embedding input: Input must not be empty\",\"type\":\"embedding_error\"}}"
time=2026-04-23T15:47:52.915-07:00 level=WARN msg="embed batch failed" run_id=8ec058bfb2dd gen=1 ids=50 error="embed: embed: HTTP 400: {\"error\":{\"type\":\"embedding_error\",\"message\":\"Invalid embedding input: Input must not be empty\"}}"
time=2026-04-23T15:47:52.924-07:00 level=WARN msg="embed batch failed" run_id=8ec058bfb2dd gen=1 ids=50 error="embed: embed: HTTP 400: {\"error\":{\"type\":\"embedding_error\",\"message\":\"Invalid embedding input: Input must not be empty\"}}"
Error: embed run: embed worker aborting after 5 consecutive failures: embed: embed: HTTP 400: {"error":{"type":"embedding_error","message":"Invalid embedding input: Input must not be empty"}}
```

Before surfacing the 4xx response body, the CLI only showed:

```text
embed run: embed worker aborting after 5 consecutive failures: embed: embed: HTTP 400
```

which made this much harder to diagnose.

### Root cause

`internal/vector/embed/preprocess.go` can legitimately return `""`:

- no subject
- body becomes empty after quote stripping and/or signature stripping
- whitespace trim leaves nothing

`internal/vector/embed/worker.go` currently appends every preprocessed message into the batch inputs, including those empty strings, and then sends the whole batch to the embedding endpoint.

Servers that reject blank inputs fail the entire batch, so a single empty message can poison 24 or 50 otherwise-valid messages and cause repeated aborts.

### Minimal reproduction

1. Create or import a message whose body is only quoted text or only a signature block, and whose subject is empty.
2. Ensure vector preprocessing strips quotes/signatures.
3. Use an embedding endpoint that rejects blank strings on `/v1/embeddings`.
4. Run:

```bash
msgvault build-embeddings
```

Expected current behavior: repeated 400s and eventual abort.

### Expected behavior

Two changes seem appropriate:

1. **Do not send empty post-processed messages to the embedder.**
   - Treat them similarly to missing messages: drain their queue rows without generating embeddings.
   - They have no semantic content left after preprocessing, so embedding `""` is not useful anyway.

2. **Surface 4xx response bodies from embedding endpoints.**
   - Example:
     ```text
     embed: HTTP 400: {"error":{"message":"Invalid embedding input: Input must not be empty"}}
     ```
   - This makes server/model/API mismatches diagnosable from the CLI.

### Why this matters

- A single pathological message can block an entire generation from completing.
- The failure mode is batch-wide and non-obvious.
- Different OpenAI-compatible servers vary on whether empty string inputs are accepted, so this is an interoperability issue, not just one backend being strict.

### Suggested implementation

- In `embedBatch`, skip any message whose preprocessed text is empty after trimming.
- Return those IDs in a separate `empty` collection.
- In `RunOnce`, complete those rows from `pending_embeddings` the same way missing rows are drained.
- In the HTTP embed client, include bounded 4xx response bodies in returned errors.

### Notes

I’ve locally verified both of the above fixes:

- 4xx response bodies are now surfaced in the embed client error.
- Empty post-processed messages are removed from the queue instead of being forwarded as `""` to the embedder.

If helpful, I can open a PR with the worker and client tests that reproduce both issues.
