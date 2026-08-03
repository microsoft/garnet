// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Handles side effects required to apply a dynamic configuration update (e.g., starting, stopping,
    /// or restarting a background task, such as reacting to an AOF commit-frequency change).
    /// <para/>
    /// Executed during <c>CONFIG SET</c> after value validation and after the new value has been written
    /// into the slot (so any task the action restarts observes it).
    /// Return <see langword="true"/> to allow the update to proceed, or <see langword="false"/> (setting <paramref name="error"/>)
    /// to abort the update, in which case the slot is rolled back to <paramref name="oldValue"/>. Implementations must be stateless and static so metadata can be safely shared across instances.
    /// </summary>
    /// <param name="config">The target server configuration instance.</param>
    /// <param name="oldValue">The value the option held before this update (the roll-back target on failure).</param>
    /// <param name="newValue">The validated new configuration value.</param>
    /// <param name="error">The error message returned to the client if the update is rejected (typically formatted as <c>"ERR ..."</c>).</param>
    /// <returns><see langword="true"/> if the update succeeded and can be committed; otherwise, <see langword="false"/>.</returns>
    internal delegate bool ConfigUpdateAction(RuntimeServerConfig config, long oldValue, long newValue, out string error);

    /// <summary>
    /// Per-option description of a runtime configuration slot: how the raw 8-byte cell is interpreted,
    /// the range it accepts, and how it may be read back.
    /// </summary>
    /// <param name="Name">Canonical CONFIG wire name of the option.</param>
    /// <param name="Kind">Storage kind of the slot, plus every typed view it may be read through.</param>
    /// <param name="Min">Inclusive lower bound accepted by CONFIG SET.</param>
    /// <param name="Max">Inclusive upper bound accepted by CONFIG SET.</param>
    /// <param name="EnumType">Declared enum type, for <see cref="ConfigKind.Enum"/> options.</param>
    /// <param name="IsRuntime">Whether the option is served by <see cref="RuntimeServerConfig"/>.</param>
    /// <param name="ReadOnly">Whether CONFIG SET rejects the option.</param>
    /// <param name="TimeUnit">Unit the slot is stored in, for duration-valued options.</param>
    /// <param name="NonPositiveIsInfinite">Whether a non-positive value denotes an infinite timeout.</param>
    /// <param name="ReadOnlyFormatter">
    /// For <see cref="ReadOnly"/> options, computes the CONFIG GET value directly from the live
    /// <see cref="GarnetServerOptions"/> (the read-only fall-through). Must not capture the options
    /// instance so the metadata stays static and shared across every <see cref="RuntimeServerConfig"/>.
    /// </param>
    /// <param name="UpdateAction">
    /// Optional action that enacts the update on live server state (for example, restarting or killing the
    /// AOF commit task). Runs during CONFIG SET after validation and after the slot is written; if it
    /// fails the slot is rolled back and the update is rejected. See <see cref="ConfigUpdateAction"/>.
    /// </param>
    internal readonly record struct ConfigMeta(
        string Name,
        ConfigKind Kind,
        long Min,
        long Max,
        Type EnumType,
        bool IsRuntime,
        bool ReadOnly,
        ConfigTimeUnit TimeUnit = ConfigTimeUnit.None,
        bool NonPositiveIsInfinite = false,
        Func<GarnetServerOptions, string> ReadOnlyFormatter = null,
        ConfigUpdateAction UpdateAction = null);
}