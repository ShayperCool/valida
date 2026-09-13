#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bun_bin="${BUN_BIN:-bun}"
release_ref="${RELEASE_REF:-HEAD}"
dist_dir="${RELEASE_DIST_DIR:-$repo_root/dist}"
if [[ "$dist_dir" != /* ]]; then dist_dir="$repo_root/$dist_dir"; fi

release_version="$(git -C "$repo_root" show "$release_ref:package.json" | "$bun_bin" -e 'console.log(JSON.parse(await Bun.stdin.text()).version)')"
if [[ ! "$release_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Invalid package version: $release_version" >&2
  exit 1
fi

archive_base="valida-$release_version"
archive_name="$archive_base.tar.gz"
mkdir -p "$dist_dir"
git -C "$repo_root" archive --format=tar --prefix="$archive_base/" "$release_ref" |
  gzip -n -9 > "$dist_dir/$archive_name"

if ! tar -tzf "$dist_dir/$archive_name" "$archive_base/bun.lock" >/dev/null; then
  echo "Release archive is missing bun.lock" >&2
  exit 1
fi

(
  cd "$dist_dir"
  sha256sum "$archive_name" > SHA256SUMS
)

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'version=%s\n' "$release_version" >> "$GITHUB_OUTPUT"
fi
printf 'Built %s and SHA256SUMS from %s\n' "$dist_dir/$archive_name" "$release_ref"
