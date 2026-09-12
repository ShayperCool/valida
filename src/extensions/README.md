# Store and cron extensions

`createStoreExtension(store)` creates a portable SQL table and returns the
methods used by `PlatformAdapter.store`. Items are JSON objects addressed by
namespace and key. Namespace listing, exact metadata filtering, pagination,
and item TTL are supported. A nonempty semantic `query` returns HTTP 501 through
the API because no embedding model or vector index is configured.

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
