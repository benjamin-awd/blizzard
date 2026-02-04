#!/usr/bin/env bash
set -euo pipefail

# Bump version in Cargo.toml
# Usage: ./scripts/bump-version.sh <package> [major|minor|patch]

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <package> [major|minor|patch]" >&2
    echo "Packages: blizzard, penguin" >&2
    exit 1
fi

PACKAGE="$1"
BUMP_TYPE="${2:-patch}"
CARGO_TOML="crates/${PACKAGE}/Cargo.toml"

if [[ ! -f "$CARGO_TOML" ]]; then
    echo "Error: $CARGO_TOML not found" >&2
    exit 1
fi

CURRENT_VERSION=$(cargo metadata --format-version 1 --no-deps | jq -r --arg pkg "$PACKAGE" '.packages[] | select(.name == $pkg) | .version')

if [[ -z "$CURRENT_VERSION" ]]; then
    echo "Error: Could not find version for package '$PACKAGE'" >&2
    exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

case "$BUMP_TYPE" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    patch)
        PATCH=$((PATCH + 1))
        ;;
    *)
        echo "Usage: $0 <package> [major|minor|patch]" >&2
        exit 1
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"

sed -i '' "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" "$CARGO_TOML"

# Update Cargo.lock to reflect the version change
cargo update -p "$PACKAGE" --workspace

echo "$PACKAGE: $CURRENT_VERSION -> $NEW_VERSION"
