// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;

namespace Garnet.fuzz.Targets
{
    /// <summary>
    /// Fuzz target for <see cref="RespCommand"/> parsing.
    /// </summary>
    public sealed class RespCommandParsing : IFuzzerTarget
    {
        /// <inheritdoc/>
        public static void Fuzz(ReadOnlySpan<byte> input)
        {
        }
    }
}
