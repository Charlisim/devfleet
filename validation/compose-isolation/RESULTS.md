# Compose isolation results

Validated locally on 2026-09-08 with Docker's Unix-socket context and the
fifteen-service fixture.

- Started two `devfleet compose up` environments from separate Git worktrees,
  with API ports `127.0.0.1:20003` and `127.0.0.1:20004`.
- DevFleet reported both endpoints `healthy: true`; each `/health` request
  returned `ok`.
- Wrote a marker to the first stack's `files` volume. The second stack's
  `/data/mutation` path remained absent.
- Stopped both stacks through `devfleet stop`. Containers and networks stopped;
  each stack retained four distinct named volumes (`database`, `queue`,
  `cache`, and `files`).
- Verified supervisor control sockets were removed after the owned processes
  stopped, so a stopped environment can start again without taking ownership
  of an active process.

The fixture uses `DEVFLEET_API_PORT`; pass it through `--env` together with the
matching declared endpoint.
