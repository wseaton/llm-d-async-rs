#!/usr/bin/env bash
# Builds both processors: the Rust one from this repo, and the Go one with the
# sql transport from GO_REPO at GO_COMMIT (checked out under .go-async, which
# go.mod also points producer-sql at).
set -euo pipefail

GO_REPO=${GO_REPO:-https://github.com/wseaton/llm-d-async.git}
GO_COMMIT=${GO_COMMIT:-b9dcfaf4b4514157031948ab4fca308708cd6a0f}

here=$(cd "$(dirname "$0")" && pwd)
src="$here/.go-async"

if [ ! -d "$src/.git" ]; then
  git clone --filter=blob:none --no-checkout "$GO_REPO" "$src"
fi
git -C "$src" fetch --quiet origin "$GO_COMMIT"
git -C "$src" checkout --quiet --detach "$GO_COMMIT"

mkdir -p "$here/.bin"
(cd "$src" && go build -o "$here/.bin/llm-d-async-go" ./cmd)
(cd "$here/.." && cargo build --release --quiet)
ln -sf "$here/../target/release/llm-d-async" "$here/.bin/llm-d-async-rs"
(cd "$here" && go build -o "$here/.bin/bench" .)

echo "go:   $(git -C "$src" log -1 --format='%h %s')"
echo "rust: $(git -C "$here/.." log -1 --format='%h %s')"
