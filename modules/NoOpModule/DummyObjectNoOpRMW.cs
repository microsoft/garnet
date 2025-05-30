// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace NoOpModule
{
    /// <summary>
    /// Represents a no-op RMW operation on a dummy object
    /// </summary>
    public class DummyObjectNoOpRMW : CustomObjectFunctions
    {
        /// <inheritdoc />
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref ObjectInput input,
            ref RespMemoryWriter writer) => true;

        /// <inheritdoc />
        public override bool Updater(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value,
            ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            return true;
        }
    }
}