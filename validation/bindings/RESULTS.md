# Binding results

`bindings_write_private_phase_artifacts_and_expose_only_public_metadata`
validates all four phase/integration contracts. It verifies the matching
invalidation decision, reserved endpoint precedence, private `0600` artifacts,
status-secret exclusion, and rejection of invalid integrations or endpoint
overrides before configuration is written.
