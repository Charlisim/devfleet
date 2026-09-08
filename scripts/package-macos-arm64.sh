#!/usr/bin/env sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 OUTPUT_DIRECTORY" >&2
  exit 64
fi

if [ "$(uname -s)" != "Darwin" ] || [ "$(uname -m)" != "arm64" ]; then
  echo "DevFleet v0.1 packages only on macOS Apple Silicon" >&2
  exit 69
fi

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=$1
version=0.1.0
name="devfleet-v${version}-macos-arm64"
archive="$output/$name.tar.gz"

if [ -e "$archive" ]; then
  echo "refusing to overwrite $archive" >&2
  exit 73
fi

cd "$root"
cargo build --release --workspace --locked

stage=$(mktemp -d "${TMPDIR:-/tmp}/df-package.XXXXXX")
package="$stage/$name"
trap 'rm -rf "$stage"' EXIT INT TERM
mkdir -p "$package/bin" "$package/plugins" "$package/share/devfleet"

for binary in \
  devfleet \
  devfleetd \
  devfleet-supervisor \
  devfleet-plugin-process \
  devfleet-plugin-compose \
  devfleet-plugin-metro \
  devfleet-plugin-ios \
  devfleet-plugin-react-native; do
  install -m 755 "target/release/$binary" "$package/bin/$binary"
done

for provider in process compose metro ios react-native; do
  "$package/bin/devfleet-plugin-$provider" --descriptor >"$package/plugins/official.$provider.json"
done

{
  printf '{\n  "format": 1,\n  "release": "%s",\n  "plugins": [\n' "$version"
  first=true
  for provider in process compose metro ios react-native; do
    if [ "$first" = true ]; then
      first=false
    else
      printf ',\n'
    fi
    binary="devfleet-plugin-$provider"
    checksum=$(/usr/bin/shasum -a 256 "$package/bin/$binary" | /usr/bin/awk '{print $1}')
    printf '    {"binary":"%s","descriptor":"plugins/official.%s.json","sha256":"%s"}' \
      "$binary" "$provider" "$checksum"
  done
  printf '\n  ]\n}\n'
} >"$package/plugins.lock.json"

install -m 644 README.md "$package/share/devfleet/README.md"
install -m 644 docs/architecture.md "$package/share/devfleet/architecture.md"
install -m 644 docs/release.md "$package/share/devfleet/release.md"
install -m 644 release/compatibility.json "$package/share/devfleet/compatibility.json"
install -m 644 Cargo.lock "$package/share/devfleet/Cargo.lock"
install -m 644 rust-toolchain.toml "$package/share/devfleet/rust-toolchain.toml"

{
  printf '{\n'
  printf '  "release": "%s",\n' "$version"
  printf '  "platform": "macos-aarch64",\n'
  printf '  "rustc": "%s",\n' "$(rustc --version)"
  printf '  "source_commit": "%s"\n' "$(git rev-parse HEAD)"
  printf '}\n'
} >"$package/manifest.json"

(
  cd "$package"
  find bin plugins share -type f -print | LC_ALL=C sort | xargs /usr/bin/shasum -a 256
) >"$package/SHA256SUMS"

mkdir -p "$output"
tar -czf "$archive" -C "$stage" "$name"
printf '%s\n' "$archive"
