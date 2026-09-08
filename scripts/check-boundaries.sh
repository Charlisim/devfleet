#!/usr/bin/env sh
set -eu

reject() {
  package="$1"
  forbidden="$2"
  tree="$(cargo tree --package "$package" --edges normal --prefix none | sed '1d')"
  if printf '%s\n' "$tree" | grep -Eq "$forbidden"; then
    printf '%s has a forbidden dependency:\n%s\n' "$package" "$tree" >&2
    exit 1
  fi
}

reject devfleet-core '^devfleet-(daemon|state|supervisor|plugin-sdk) |^devfleet-plugin-'
reject devfleet-state '^devfleet-(core|daemon|supervisor|plugin-sdk) |^devfleet-plugin-'
reject devfleet-plugin-sdk '^devfleet-(core|daemon|state|supervisor) |^devfleet-plugin-'
reject devfleet-supervisor '^devfleet-(core|daemon|state) |^devfleet-plugin-'
