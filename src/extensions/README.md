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
filters run before ranking. SQLite and PostgreSQL store the same vector JSON in
`valida_store_embeddings`. The current implementation scans matching rows in
application memory, so search time grows with the number of indexed items.
Existing items written before index activation need a `put` to create vectors.

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
