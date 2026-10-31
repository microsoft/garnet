// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>Admission, failure, and reference bounds for one Snapshot checkpoint's object log.</summary>
    internal sealed class SnapshotObjectLogState
    {
        internal readonly ObjectLog Log;

        /// <summary>This checkpoint's tag in an object's phase word (<see cref="HeapObjectBase"/>), which tells a capture or a stamp
        /// by this checkpoint from one left by an earlier checkpoint.</summary>
        internal readonly int Tag;

        readonly ManualResetEventSlim drained = new(false);
        int inFlight;
        int closed;
        Exception failure;
        long lowestMainLogAddress = long.MaxValue;

        internal SnapshotObjectLogState(ObjectLog log, int tag)
        {
            Log = log ?? throw new ArgumentNullException(nameof(log));
            if (tag < 0 || tag > HeapObjectBase.MaxCheckpointTag)
                throw new ArgumentOutOfRangeException(nameof(tag));
            Tag = tag;
        }

        internal Exception Failure => Volatile.Read(ref failure);

        internal long LowestMainLogAddress => Volatile.Read(ref lowestMainLogAddress);

        internal void NoteMainLogReference(long address)
        {
            long previous;
            do
            {
                previous = Volatile.Read(ref lowestMainLogAddress);
                if (address >= previous)
                    return;
            } while (Interlocked.CompareExchange(ref lowestMainLogAddress, address, previous) != previous);
        }

        internal bool TryBeginCapture()
        {
            _ = Interlocked.Increment(ref inFlight);
            if (Volatile.Read(ref closed) == 0)
                return true;
            EndCapture();
            return false;
        }

        internal void EndCapture()
        {
            if (Interlocked.Decrement(ref inFlight) == 0 && Volatile.Read(ref closed) != 0)
                drained.Set();
        }

        internal void RecordFailure(Exception exception) => _ = Interlocked.CompareExchange(ref failure, exception, null);

        internal void CloseAndDrain()
        {
            _ = Interlocked.Exchange(ref closed, 1);
            if (Volatile.Read(ref inFlight) != 0)
                drained.Wait();
        }
    }
}