# Compose isolation fixture

This fixture has fifteen services and one loopback-only API publication. It
keeps databases, queues, caches, and files in project-scoped named volumes.

Run two copies from different Git worktrees through `devfleet compose up`, with
`--endpoint api=api:3000:0:/health`. DevFleet writes each resolved port to
`DEVFLEET_API_PORT`, which this fixture consumes. Write a marker through the
`files` service in one project and assert that the other project's
`/data/mutation` path does not exist. Stop each environment through DevFleet;
Compose stops containers while retaining named volumes.
