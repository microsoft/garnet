// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Set of representations valid for a runtime configuration slot. A slot is a raw 8-byte cell; the
    /// storage kind determines how it is parsed (CONFIG SET), validated and formatted (CONFIG GET), while
    /// the additional flags declare which typed accessors the option may be read through.
    /// <para/>
    /// Exactly one storage kind (<see cref="Int32"/>, <see cref="Int64"/>, <see cref="Bool"/>,
    /// <see cref="Enum"/> or <see cref="String"/>) must be set. A duration-valued option additionally
    /// declares every unit it may be read as, so that a single option can be consumed as milliseconds,
    /// seconds or a <see cref="System.TimeSpan"/> without the caller guessing the stored unit.
    /// </summary>
    [Flags]
    internal enum ConfigKind : ushort
    {
        None = 0,

        // Storage kinds: exactly one per option.
        Int32 = 1 << 0,
        Int64 = 1 << 1,
        Bool = 1 << 2,
        Enum = 1 << 3,
        String = 1 << 4,

        // Duration views: any combination, valid only for options that declare a time unit.
        Milliseconds = 1 << 5,
        Seconds = 1 << 6,
        Microseconds = 1 << 7,
        TimeSpan = 1 << 8,

        /// <summary>Mask of the storage kinds.</summary>
        StorageMask = Int32 | Int64 | Bool | Enum | String,

        /// <summary>Mask of the duration views.</summary>
        DurationMask = Milliseconds | Seconds | Microseconds | TimeSpan,
    }
}