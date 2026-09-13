# Store and cron extensions

`createStoreExtension(store)` creates portable SQL tables and returns the
methods used by `PlatformAdapter.store`. Items are JSON objects addressed by
namespace and key. Namespace listing, exact metadata filtering, pagination,
and item TTL work without an index. A semantic `query` returns HTTP 501 when
no index is configured.

Set `store.index` in `valida.json` to enable semantic search:

```json
{
  "graphs": { "echo": "./examples/graphs.ts:echo" },
  "store": {
    "index": {
      "dims": 3,
      "embed": "./examples/embeddings.ts:embedTexts",
      "fields": ["text"]
    }
  }
}
```

The module export receives `string[]` and returns `number[][]` or a promise of
that shape. Wire it into startup after loading `valida.json`:

```ts
const index = await loadStoreIndex(config.value.store?.index, config.directory);
const kv = await createStoreExtension(runtime.store, { index });
```

The example function uses word counts and needs no network. The
default field is `["$"]`, which embeds the full JSON value. Dot-separated
paths such as `"profile.bio"` select nested fields; missing fields are skipped.
`put({ ..., index: false })` stores an item without a vector, while an array of
paths in `index` overrides the configured fields for that item.

Search computes cosine similarity over finite, nonzero vectors of the configured
size. It returns each matching item with a `score` from -1 to 1, sorted by score
before `offset` and `limit`. TTL, namespace prefix, metadata and authorization
filters run before ranking. SQLite stores vectors as JSON in
`valida_store_embeddings` and ranks matching rows in application memory.
PostgreSQL stores them in native `vector` columns and ranks with pgvector;
dimension-specific HNSW indexes support up to 4000 dimensions (`halfvec`
above 2000). PostgreSQL needs the `vector` extension, included in the Compose
image. Existing JSON embeddings from an earlier PostgreSQL Valida version
are imported into pgvector at startup. Items written before any index was
enabled still need a `put` to create vectors.

`createCronExtension(store, runtime)` creates the cron table and returns the
methods used by `PlatformAdapter.crons`. Enabled crons fire once at creation,
then the scheduler calls `tick()` at each polling interval. Call `start()` on
API or worker instances and `stop()` during shutdown. The scheduler claims due
rows through conditional SQL updates, so concurrent instances do not fire the
same due record together. A crash after starting a run but before advancing
the schedule can replay that firing after its lease expires; scheduled runs are
therefore at least once, not exactly once.

The cron parser supports five-field and six-field expressions with IANA
timezones. Cron payloads run through the same registered graph runtime as
ordinary runs. The extension does not deliver webhooks or generate embeddings.

`ThreadPruner` also sweeps opt-in thread TTL rows. `threadTtlForRequest` parses
SDK minute counts or `{ ttl, strategy }` objects, and `resolveThreadTtlPolicy` reads
the optional `checkpointer.ttl` block or `VALIDA_THREAD_TTL` environment value.
Without a configured default or an explicit per-thread TTL, ordinary threads
do not expire. The database
stores the deadline separately from thread metadata, so metadata-only updates
do not reset it. Expired `delete` rows remove the thread, runs, events, and
checkpoints; `keep_latest` retains the newest checkpoint and pending writes,
then schedules the next compaction. The sweeper skips active runs and acts on
them after they finish.
