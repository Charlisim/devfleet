# Initialization task results

The integration test `initialization_tasks_are_ordered_persisted_and_scoped_to_one_environment`
creates two Git worktrees and two owned backend processes. It verifies that a
migration completes before its dependent seed, both completion records survive
a repeated request without rerunning `never` tasks, and the second worktree
does not receive the first worktree's mutation. It also verifies a failed
migration prevents readiness and is retained as failed.
