// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Roots the buffer so the pinned byte[] is not collected while IO is in-flight.
    /// If the caller abandons the wait (timeout/cancellation), the buffer is intentionally
    /// not returned to the pool — the GC will collect it after the IO completes and
    /// the callback releases this context.
    /// </summary>
    internal sealed class IOCallbackContext
    {
        public SectorAlignedMemory Buffer;
    }
}