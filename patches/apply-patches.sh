#!/usr/bin/env bash
# apply-patches.sh
# Run this after `go mod vendor` to re-apply all custom patches.
# Usage: bash patches/apply-patches.sh

set -e
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PATCHES_DIR="$REPO_ROOT/patches"

echo "Applying custom patches to vendor/..."
for patch in "$PATCHES_DIR"/*.patch; do
    echo "  -> $patch"
    patch -p1 --directory="$REPO_ROOT" < "$patch"
done
echo "Done."
