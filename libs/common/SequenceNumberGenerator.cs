// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Sequence number generator
    /// </summary>
    /// <param name="start"></param>
    public class SequenceNumberGenerator(long start)
    {
        readonly long baseTimestamp = Stopwatch.GetTimestamp();
        readonly long start = start;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSequenceNumber() => Stopwatch.GetTimestamp() - baseTimestamp + start;

        public override string ToString() => $"{start},{baseTimestamp}";
    }
}