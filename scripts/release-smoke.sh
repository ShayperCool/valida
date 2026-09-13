#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bun_bin="${BUN_BIN:-bun}"
release_version="$("$bun_bin" -e 'console.log(require(process.argv[1]).version)' "$repo_root/package.json")"
archive_path="${1:-$repo_root/dist/valida-$release_version.tar.gz}"
archive_path="$(realpath "$archive_path")"
archive_base="valida-$release_version"
archive_dir="$(dirname "$archive_path")"

(
  cd "$archive_dir"
  sha256sum --check SHA256SUMS
)

scratch_dir="$(mktemp -d)"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$scratch_dir"
}
trap cleanup EXIT

tar -xzf "$archive_path" -C "$scratch_dir"
source_dir="$scratch_dir/$archive_base"
test -f "$source_dir/bun.lock"
test -f "$source_dir/drizzle/sqlite/meta/_journal.json"

cd "$source_dir"
"$bun_bin" install --frozen-lockfile

server_log="$scratch_dir/server.log"
HOST=127.0.0.1 PORT=0 DATABASE_URL="file:$scratch_dir/valida.db" EXECUTION_MODE=standalone \
  "$bun_bin" src/main.ts > "$server_log" 2>&1 &
server_pid="$!"

server_url=""
for ((attempt = 0; attempt < 100; attempt++)); do
  server_url="$(sed -n 's/.*Valida listening on \(http:\/\/127\.0\.0\.1:[0-9][0-9]*\).*/\1/p' "$server_log" | tail -n 1)"
  if [[ -n "$server_url" ]] && curl --silent --fail "$server_url/health" >/dev/null; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log" >&2
    echo "Release server exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.3
done

if [[ -z "$server_url" ]] || ! curl --silent --fail "$server_url/health" >/dev/null; then
  cat "$server_log" >&2
  echo "Release server did not become ready" >&2
  exit 1
fi

if ! VALIDA_API_URL="$server_url" VALIDA_API_URL_2="$server_url" "$bun_bin" scripts/smoke.ts; then
  cat "$server_log" >&2
  exit 1
fi
printf 'Release archive smoke passed at %s\n' "$server_url"
