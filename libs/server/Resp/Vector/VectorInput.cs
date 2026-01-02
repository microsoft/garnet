// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Header for Garnet Main Store inputs but for Vector element r/w/d ops
    /// </summary>
    public struct VectorInput : IStoreInput
    {
        /// <inheritdoc/>
        public readonly int SerializedLength => throw new NotImplementedException();

        public int ReadDesiredSize { get; set; }

        public int WriteDesiredSize { get; set; }

        public int Index { get; set; }
        public nint CallbackContext { get; set; }
        public nint Callback { get; set; }

        public ulong Namespace { get; set; }

        public VectorInput()
        {
        }

        /// <inheritdoc/>
        public readonly unsafe int CopyTo(byte* dest, int length) => throw new NotImplementedException();
        /// <inheritdoc/>
        public readonly unsafe int DeserializeFrom(byte* src) => throw new NotImplementedException();
    }
}
