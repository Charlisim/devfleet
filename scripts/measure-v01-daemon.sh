#!/usr/bin/env sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 OUTPUT_JSON [WORKTREE]" >&2
  exit 64
fi

output=$1
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
worktree=${2:-$root}

if [ -e "$output" ]; then
  echo "refusing to overwrite $output" >&2
  exit 73
fi
git -C "$worktree" rev-parse --is-inside-work-tree >/dev/null

cd "$root"
cargo build --release --workspace --locked

state=$(mktemp -d "${TMPDIR:-/tmp}/df-v01-metrics.XXXXXX")
socket="$state/d.sock"
daemon="$root/target/release/devfleetd"
cli="$root/target/release/devfleet"
supervisor="$root/target/release/devfleet-supervisor"
plugin="$root/target/release/devfleet-plugin-process"

cleanup() {
  if [ -n "${daemon_pid:-}" ]; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  rm -rf "$state"
}
trap cleanup EXIT INT TERM

"$daemon" --serve --state-dir "$state/state" --socket "$socket" \
  --supervisor "$supervisor" --process-plugin "$plugin" >"$state/daemon.log" 2>&1 &
daemon_pid=$!

for _ in $(seq 1 100); do
  if "$cli" --socket "$socket" plugin list >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done
"$cli" --socket "$socket" plugin list >/dev/null
"$cli" --socket "$socket" plugin install "$plugin" >/dev/null
"$cli" --socket "$socket" plugin trust official.process >/dev/null

accepted=$("$cli" --socket "$socket" --json up --detach --trust \
  --worktree "$worktree" --cwd "$worktree" -- /bin/sleep 60)
environment_id=$(printf '%s\n' "$accepted" | /usr/bin/sed -n \
  's/.*"environment_id":"\([^"]*\)".*/\1/p')
if [ -z "$environment_id" ]; then
  echo "daemon returned no environment_id" >&2
  exit 70
fi

samples="$state/status-ms"
for _ in $(seq 1 100); do
  started=$(/usr/bin/perl -MTime::HiRes=time -e 'printf "%.3f", time * 1000')
  "$cli" --socket "$socket" --json status "$environment_id" >/dev/null
  finished=$(/usr/bin/perl -MTime::HiRes=time -e 'printf "%.3f", time * 1000')
  /usr/bin/awk -v started="$started" -v finished="$finished" \
    'BEGIN { printf "%.3f\n", finished - started }' >>"$samples"
done

count=$(wc -l <"$samples" | tr -d ' ')
sample_rank=$(( (count * 95 + 99) / 100 ))
p95_ms=$(sort -n "$samples" | /usr/bin/sed -n "${sample_rank}p")
if [ -z "$p95_ms" ]; then
  echo "no status samples were recorded" >&2
  exit 70
fi
rss_kib=$(ps -o rss= -p "$daemon_pid" | tr -d ' ')
status_passed=$(/usr/bin/awk -v value="$p95_ms" 'BEGIN { print (value <= 200 ? "true" : "false") }')
rss_passed=$(/usr/bin/awk -v value="$rss_kib" 'BEGIN { print (value <= 102400 ? "true" : "false") }')
cpu_logical_cores=$(sysctl -n hw.logicalcpu)
memory_bytes=$(sysctl -n hw.memsize)
disk_available_kib=$(df -k "$worktree" | /usr/bin/awk 'NR == 2 { print $4; exit }')

mkdir -p "$(dirname -- "$output")"
cat >"$output" <<EOF
{
  "status_samples": $count,
  "status_p95_ms": $p95_ms,
  "status_target_ms": 200,
  "status_target_passed": $status_passed,
  "daemon_idle_rss_kib": $rss_kib,
  "daemon_rss_target_kib": 102400,
  "daemon_rss_target_passed": $rss_passed,
  "environment_count": 1,
  "service_count": 1,
  "native_build_concurrency": 0,
  "cpu_logical_cores": $cpu_logical_cores,
  "host_memory_bytes": $memory_bytes,
  "worktree_available_disk_kib": $disk_available_kib,
  "macos_version": "$(sw_vers -productVersion)",
  "architecture": "$(uname -m)",
  "rustc": "$(rustc --version)",
  "external_tools_excluded": true,
  "four_environment_run_completed": false,
  "ten_environment_run_completed": false
}
EOF
