// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Sequence number generator
    /// </summary>
    /// <param name="startingOffset"></param>
    public sealed class SequenceNumberGenerator(long startingOffset)
    {
        readonly long baseTimestamp = Stopwatch.GetTimestamp();
        readonly long startingOffset = startingOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSequenceNumber() => Stopwatch.GetTimestamp() - baseTimestamp + startingOffset;
        
        public override string ToString() => $"{startingOffset},{baseTimestamp},{Stopwatch.GetTimestamp()}";
    }
}