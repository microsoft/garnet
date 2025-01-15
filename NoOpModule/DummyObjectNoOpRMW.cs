// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
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
        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input,
            ref (IMemoryOwner<byte>, int) output) => true;

        /// <inheritdoc />
        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value,
            ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            WriteSimpleString(ref output, "OK");
            return true;
        }
    }
}