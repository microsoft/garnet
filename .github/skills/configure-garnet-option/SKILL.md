---
name: configure-garnet-option
description: Add or modify a Garnet server configuration option across startup parsing, defaults, programmatic options, runtime CONFIG, consumers, tests and documentation.
---

# Configure a Garnet option

Trace a comparable option before editing. Decide whether the value is startup-only,
read live, captured per operation, or requires a lifecycle action.

1. Add or update `libs/host/Configuration/Options.cs`, its validation attribute in
   `OptionsValidators.cs`, `libs/host/defaults.conf`, the corresponding
   `GarnetServerOptions` member and the `Options.GetServerOptions()` mapping.
2. Keep help text, defaults comments and XML documentation consistent about units,
   defaults, bounds, scope and when changes take effect. Share parsers and validators.
3. For runtime support, register a member in `libs/server/Config/ServerConfigType.cs`
   and metadata in `libs/server/Config/RuntimeServerConfig.cs`
   (`RuntimeServerConfig.BuildMeta`), seed it in `Init`, and use the declared typed
   accessor at every consumer. Register startup-only values as read-only when needed.
   Do not mutate startup options to implement `CONFIG SET`.
4. Use an update action when a live task or physical consumer must be changed.
   Validate coupled settings before publication and capture a consistent snapshot
   for operations that cannot safely change halfway through. Check buffer pools,
   caches and resource sizing as well as the immediate consumer.
5. Validate physical constraints with the configured provider without creating
   resources merely to probe them; retain defensive checks at the point of use.
6. Cover CLI, JSON, defaults, programmatic validation, `CONFIG GET`/`SET` and `GET *`,
   rejection without mutation, actual consumer behavior and operation lifetime.
   Follow the existing net10.0 test-project layout.
7. Update `website/docs/getting-started/configuration.md` and the feature's operator
   and developer documentation. Explain normalization and runtime update ordering.
