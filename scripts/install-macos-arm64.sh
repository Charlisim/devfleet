#!/usr/bin/env sh
set -eu

usage() {
  cat <<'EOF'
Usage: install-macos-arm64.sh [PACKAGE_ARCHIVE] [options]

Install a signed-by-checksum DevFleet v0.1 macOS Apple Silicon package, its
Homebrew dependencies, official provider plugins, and a per-user launch agent.

Options:
  --archive PATH          Package archive when this script is run from source.
  --prefix PATH           Package destination.
  --state-dir PATH        Private daemon state directory.
  --socket PATH           Private daemon Unix socket.
  --install-homebrew      Install Homebrew from brew.sh when it is absent.
  --no-start              Install files and plugins without starting the daemon.
  --no-trust              Install official plugins without trusting them.
  --doctor WORKTREE       Run a read-only doctor check after installation.
  -h, --help              Show this help.

When run from an unpacked DevFleet archive, no archive argument is needed.
EOF
}

fail() {
  printf '%s\n' "$*" >&2
  exit 1
}

archive=
prefix="${HOME:?}/.local/share/devfleet/v0.1.0"
state_dir="${HOME:?}/Library/Application Support/DevFleet"
socket_dir="/tmp/df-$(id -u)"
socket="$socket_dir/devfleet.sock"
start_daemon=true
trust_plugins=true
install_homebrew=false
doctor_worktree=

while [ "$#" -gt 0 ]; do
  case "$1" in
    --archive)
      [ "$#" -ge 2 ] || fail "--archive requires a path"
      archive=$2
      shift 2
      ;;
    --prefix)
      [ "$#" -ge 2 ] || fail "--prefix requires a path"
      prefix=$2
      shift 2
      ;;
    --state-dir)
      [ "$#" -ge 2 ] || fail "--state-dir requires a path"
      state_dir=$2
      shift 2
      ;;
    --socket)
      [ "$#" -ge 2 ] || fail "--socket requires a path"
      socket=$2
      socket_dir=$(dirname "$socket")
      shift 2
      ;;
    --install-homebrew)
      install_homebrew=true
      shift
      ;;
    --no-start)
      start_daemon=false
      shift
      ;;
    --no-trust)
      trust_plugins=false
      shift
      ;;
    --doctor)
      [ "$#" -ge 2 ] || fail "--doctor requires a Git worktree path"
      doctor_worktree=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      fail "unknown option: $1"
      ;;
    *)
      [ -z "$archive" ] || fail "only one package archive may be provided"
      archive=$1
      shift
      ;;
  esac
done

[ "$(uname -s)" = "Darwin" ] || fail "DevFleet v0.1 installs only on macOS"
[ "$(uname -m)" = "arm64" ] || fail "DevFleet v0.1 requires Apple Silicon"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
package=
scratch=

cleanup() {
  if [ -n "$scratch" ]; then
    rm -rf "$scratch"
  fi
}
trap cleanup EXIT INT TERM

if [ -x "$script_dir/bin/devfleet" ] && [ -f "$script_dir/SHA256SUMS" ]; then
  package=$script_dir
else
  [ -n "$archive" ] || fail "pass PACKAGE_ARCHIVE when running this script from the source checkout"
  [ -f "$archive" ] || fail "package archive does not exist: $archive"
  scratch=$(mktemp -d "${TMPDIR:-/tmp}/df-installer.XXXXXX")
  tar -xzf "$archive" -C "$scratch"
  package=$(find "$scratch" -mindepth 1 -maxdepth 1 -type d -name 'devfleet-v*-macos-arm64' -print -quit)
  [ -n "$package" ] || fail "package archive layout is invalid"
fi

(
  cd "$package"
  /usr/bin/shasum -a 256 -c SHA256SUMS
) || fail "package checksum verification failed"

if ! command -v brew >/dev/null 2>&1; then
  if [ "$install_homebrew" != true ]; then
    fail "Homebrew is required. Install it from https://brew.sh/ or rerun with --install-homebrew."
  fi
  printf '%s\n' "Installing Homebrew from https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh"
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  if [ -x /opt/homebrew/bin/brew ]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
  fi
fi

command -v brew >/dev/null 2>&1 || fail "Homebrew installation did not make brew available"

install_formula() {
  if ! brew list --versions "$1" >/dev/null 2>&1; then
    brew install "$1"
  fi
}

install_cask() {
  if ! brew list --cask "$1" >/dev/null 2>&1; then
    brew install --cask "$1"
  fi
}

printf '%s\n' "Installing runtime prerequisites with Homebrew: git, node, cocoapods, Docker Desktop"
install_formula git
install_formula node
install_formula cocoapods
install_cask docker

if [ -e "$prefix" ]; then
  [ -f "$prefix/manifest.json" ] || fail "prefix already exists and is not a DevFleet package: $prefix"
  cmp -s "$package/manifest.json" "$prefix/manifest.json" || \
    fail "prefix contains a different DevFleet payload; choose a new --prefix"
else
  mkdir -p "$(dirname "$prefix")"
  cp -R "$package" "$prefix"
fi

mkdir -p "$state_dir" "$socket_dir" "${HOME:?}/.local/bin" "${HOME:?}/Library/LaunchAgents"
chmod 700 "$state_dir" "$socket_dir"

link_binary() {
  binary=$1
  target="$prefix/bin/$binary"
  link="${HOME:?}/.local/bin/$binary"
  [ -x "$target" ] || fail "package binary is missing: $target"
  if [ -L "$link" ]; then
    [ "$(readlink "$link")" = "$target" ] || fail "refusing to replace existing link: $link"
  elif [ -e "$link" ]; then
    fail "refusing to replace existing file: $link"
  else
    ln -s "$target" "$link"
  fi
}

for binary in \
  devfleet \
  devfleetd \
  devfleet-supervisor \
  devfleet-plugin-process \
  devfleet-plugin-compose \
  devfleet-plugin-metro \
  devfleet-plugin-ios \
  devfleet-plugin-react-native; do
  link_binary "$binary"
done

launch_agent="${HOME:?}/Library/LaunchAgents/com.devfleet.daemon.plist"
launch_agent_new="$(mktemp "${TMPDIR:-/tmp}/com.devfleet.daemon.XXXXXX")"
cat >"$launch_agent_new" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.devfleet.daemon</string>
  <key>ProgramArguments</key>
  <array>
    <string>$prefix/bin/devfleetd</string>
    <string>--serve</string>
    <string>--state-dir</string>
    <string>$state_dir</string>
    <string>--socket</string>
    <string>$socket</string>
    <string>--supervisor</string>
    <string>$prefix/bin/devfleet-supervisor</string>
    <string>--process-plugin</string>
    <string>$prefix/bin/devfleet-plugin-process</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$state_dir/daemon.log</string>
  <key>StandardErrorPath</key>
  <string>$state_dir/daemon.log</string>
</dict>
</plist>
EOF

if [ -e "$launch_agent" ]; then
  cmp -s "$launch_agent_new" "$launch_agent" || \
    fail "launch agent already exists with a different configuration: $launch_agent"
else
  install -m 600 "$launch_agent_new" "$launch_agent"
fi
rm -f "$launch_agent_new"

if [ "$start_daemon" = true ]; then
  user_id=$(id -u)
  if ! launchctl print "gui/$user_id/com.devfleet.daemon" >/dev/null 2>&1; then
    launchctl bootstrap "gui/$user_id" "$launch_agent"
  fi

  attempt=0
  while [ "$attempt" -lt 100 ]; do
    if "$prefix/bin/devfleet" --socket "$socket" plugin list >/dev/null 2>&1; then
      break
    fi
    attempt=$((attempt + 1))
    sleep 0.05
  done
  "$prefix/bin/devfleet" --socket "$socket" plugin list >/dev/null || \
    fail "daemon did not become healthy; inspect $state_dir/daemon.log"

  for provider in process compose metro ios react-native; do
    "$prefix/bin/devfleet" --socket "$socket" plugin install \
      "$prefix/bin/devfleet-plugin-$provider"
    if [ "$trust_plugins" = true ]; then
      "$prefix/bin/devfleet" --socket "$socket" plugin trust "official.$provider"
    fi
  done
fi

if ! command -v xcodebuild >/dev/null 2>&1; then
  printf '%s\n' "Xcode is still required for iOS Simulator environments. Install Xcode from Apple, open it once, then run xcodebuild -runFirstLaunch."
fi
if ! docker info >/dev/null 2>&1; then
  printf '%s\n' "Open Docker Desktop once and wait for it to finish starting before using Compose environments."
fi
if ! pod --version >/dev/null 2>&1; then
  printf '%s\n' "CocoaPods is installed through Homebrew but is not runnable yet; resolve pod --version before using iOS environments."
fi

if [ -n "$doctor_worktree" ]; then
  "$prefix/bin/devfleet" --socket "$socket" doctor --worktree "$doctor_worktree"
fi

printf '%s\n' "DevFleet installed at $prefix"
printf '%s\n' "Socket: $socket"
printf '%s\n' "Add ${HOME:?}/.local/bin to PATH, then run: devfleet --socket $socket doctor --worktree <git-worktree>"
