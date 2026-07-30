// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
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
    internal readonly record struct ConfigMeta(
        string Name,
        ConfigKind Kind,
        long Min,
        long Max,
        Type EnumType,
        bool IsRuntime,
        bool ReadOnly,
        ConfigTimeUnit TimeUnit = ConfigTimeUnit.None,
        bool NonPositiveIsInfinite = false);
}