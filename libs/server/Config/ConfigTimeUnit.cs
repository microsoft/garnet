// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Unit in which a duration-valued configuration slot is stored and expressed on the CONFIG wire.
    /// </summary>
    internal enum ConfigTimeUnit : byte
    {
        None,
        Microseconds,
        Milliseconds,
        Seconds,
    }
}