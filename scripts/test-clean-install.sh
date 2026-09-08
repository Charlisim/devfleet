#!/usr/bin/env sh
set -eu

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 PACKAGE_ARCHIVE GIT_WORKTREE" >&2
  exit 64
fi

archive=$1
source_worktree=$2
if [ ! -f "$archive" ]; then
  echo "package archive does not exist: $archive" >&2
  exit 66
fi
git -C "$source_worktree" rev-parse --is-inside-work-tree >/dev/null

scratch=$(mktemp -d "${TMPDIR:-/tmp}/df-clean-install.XXXXXX")
install_root="$scratch/install"
fixture_root="$scratch/worktrees"
state="$scratch/state"
daemon_pid=

cleanup() {
  if [ -n "$daemon_pid" ]; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  if [ -d "$fixture_root/a" ]; then
    git -C "$source_worktree" worktree remove --force "$fixture_root/a" 2>/dev/null || true
  fi
  if [ -d "$fixture_root/b" ]; then
    git -C "$source_worktree" worktree remove --force "$fixture_root/b" 2>/dev/null || true
  fi
  rm -rf "$scratch"
}
trap cleanup EXIT INT TERM

mkdir -p "$install_root" "$fixture_root"
tar -xzf "$archive" -C "$install_root"
package=$(find "$install_root" -mindepth 1 -maxdepth 1 -type d -name 'devfleet-v*-macos-arm64' -print -quit)
if [ -z "$package" ]; then
  echo "package layout is invalid" >&2
  exit 65
fi
(
  cd "$package"
  /usr/bin/shasum -a 256 -c SHA256SUMS
)

for component in devfleet devfleetd devfleet-supervisor; do
  "$package/bin/$component" --smoke >/dev/null
done
for provider in process compose metro ios react-native; do
  descriptor="$package/plugins/official.$provider.json"
  test "$("$package/bin/devfleet-plugin-$provider" --descriptor)" = "$(cat "$descriptor")"
done

git -C "$source_worktree" worktree add --detach "$fixture_root/a" HEAD >/dev/null
git -C "$source_worktree" worktree add --detach "$fixture_root/b" HEAD >/dev/null
socket="$state/d.sock"
"$package/bin/devfleetd" --serve --state-dir "$state" --socket "$socket" \
  --supervisor "$package/bin/devfleet-supervisor" \
  --process-plugin "$package/bin/devfleet-plugin-process" >"$scratch/daemon.log" 2>&1 &
daemon_pid=$!

for _ in $(seq 1 100); do
  if "$package/bin/devfleet" --socket "$socket" plugin list >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done
"$package/bin/devfleet" --socket "$socket" plugin list >/dev/null
"$package/bin/devfleet" --socket "$socket" plugin install "$package/bin/devfleet-plugin-process" >/dev/null
"$package/bin/devfleet" --socket "$socket" plugin trust official.process >/dev/null

start_environment() {
  fixture=$1
  instance=$2
  accepted=$("$package/bin/devfleet" --socket "$socket" --json up --detach --trust \
    --worktree "$fixture" --cwd "$fixture" --instance "$instance" -- /bin/sleep 60)
  printf '%s\n' "$accepted" | /usr/bin/sed -n 's/.*"environment_id":"\([^"]*\)".*/\1/p'
}

first=$(start_environment "$fixture_root/a" clean-a)
second=$(start_environment "$fixture_root/b" clean-b)
if [ -z "$first" ] || [ -z "$second" ] || [ "$first" = "$second" ]; then
  echo "two-worktree smoke did not return distinct environment identities" >&2
  exit 70
fi
"$package/bin/devfleet" --socket "$socket" --json status "$first" >/dev/null
"$package/bin/devfleet" --socket "$socket" --json status "$second" >/dev/null
"$package/bin/devfleet" --socket "$socket" stop "$first" >/dev/null
"$package/bin/devfleet" --socket "$socket" stop "$second" >/dev/null
printf 'clean install smoke passed: %s %s\n' "$first" "$second"
