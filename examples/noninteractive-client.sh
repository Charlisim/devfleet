#!/bin/sh
set -eu

socket=${1:?usage: noninteractive-client.sh SOCKET ENVIRONMENT_ID WORKTREE}
environment_id=${2:?usage: noninteractive-client.sh SOCKET ENVIRONMENT_ID WORKTREE}
worktree=${3:?usage: noninteractive-client.sh SOCKET ENVIRONMENT_ID WORKTREE}

# Snapshot recovery is required before resuming from an expired event cursor.
devfleet --socket "$socket" --json status "$environment_id"
devfleet --socket "$socket" --json resource exec "$environment_id" \
  --cwd "$worktree" --env CLIENT_MODE=noninteractive -- /usr/bin/env
devfleet --socket "$socket" resource open "$environment_id"

# The CLI repeatedly calls events.subscribe, advances its cursor and keeps a
# bounded event-ID deduplicator while this command is running.
devfleet --socket "$socket" --json events "$environment_id" --after 0 --follow
