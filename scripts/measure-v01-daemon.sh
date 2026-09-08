#!/usr/bin/env sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 OUTPUT_JSON [WORKTREE] [ENVIRONMENT_COUNT]" >&2
  exit 64
fi

output=$1
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
worktree=${2:-$root}
environment_count=${3:-1}

case "$environment_count" in
  ''|*[!0-9]*|0)
    echo "ENVIRONMENT_COUNT must be a positive integer" >&2
    exit 64
    ;;
esac

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
environment_ids="$state/environments"

cleanup() {
  if [ -n "${daemon_pid:-}" ] && [ -f "$environment_ids" ]; then
    while IFS= read -r environment_id; do
      "$cli" --socket "$socket" stop "$environment_id" >/dev/null 2>&1 || true
    done <"$environment_ids"
  fi
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

for index in $(seq 1 "$environment_count"); do
  accepted=$("$cli" --socket "$socket" --json up --detach --trust \
    --worktree "$worktree" --cwd "$worktree" --instance "measurement-$index" -- /bin/sleep 60)
  environment_id=$(printf '%s\n' "$accepted" | /usr/bin/sed -n \
    's/.*"environment_id":"\([^"]*\)".*/\1/p')
  if [ -z "$environment_id" ]; then
    echo "daemon returned no environment_id" >&2
    exit 70
  fi
  printf '%s\n' "$environment_id" >>"$environment_ids"
done

samples="$state/status-ms"
for sample in $(seq 1 100); do
  environment_line=$(( (sample - 1) % environment_count + 1 ))
  environment_id=$(/usr/bin/sed -n "${environment_line}p" "$environment_ids")
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
cpu_percent=$(ps -o %cpu= -p "$daemon_pid" | tr -d ' ')
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
  "environment_count": $environment_count,
  "service_count": $environment_count,
  "native_build_concurrency": 0,
  "daemon_idle_cpu_percent": $cpu_percent,
  "cpu_logical_cores": $cpu_logical_cores,
  "host_memory_bytes": $memory_bytes,
  "worktree_available_disk_kib": $disk_available_kib,
  "macos_version": "$(sw_vers -productVersion)",
  "architecture": "$(uname -m)",
  "rustc": "$(rustc --version)",
  "external_tools_excluded": true,
  "workload": "process-backed environments; no native build or simulator",
  "four_environment_run_completed": $([ "$environment_count" -ge 4 ] && printf true || printf false),
  "ten_environment_run_completed": $([ "$environment_count" -ge 10 ] && printf true || printf false)
}
EOF
