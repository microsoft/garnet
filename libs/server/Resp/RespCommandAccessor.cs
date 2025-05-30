// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Accessor to simplify access for a subset of <see cref="RespCommand"/> usable by external components such as cluster plugins.
    /// </summary>
    public static class RespCommandAccessor
    {
        /// <summary>
        /// MIGRATE
        /// </summary>
        public static ushort MIGRATE => (ushort)RespCommand.MIGRATE;
    }
}