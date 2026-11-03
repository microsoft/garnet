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

        /// <summary>The sentinel a capture of an overflow value leaves in its source's value slot; see <see cref="OverflowByteArray.CreateSentinel"/>.
        /// A flush keeps a marked record's reference only while its slot holds the active checkpoint's sentinel.</summary>
        internal readonly OverflowByteArray OverflowSentinel = OverflowByteArray.CreateSentinel();

        /// <summary>The placeholder a CopyUpdate leaves in the value slot of a source whose object this checkpoint captured; see
        /// <see cref="SupersededObjectValue"/>. A flush keeps a marked record's reference only while its slot holds the active checkpoint's
        /// placeholder.</summary>
        internal readonly SupersededObjectValue ObjectSentinel;

        /// <summary>Object-log writers that captures return for later captures, so that each capture does not create a writer, a value
        /// serializer and its stream. At most one per processor is kept, and <see cref="CloseAndDrain"/> disposes them.</summary>
        readonly IStreamBuffer[] captureWriters = new IStreamBuffer[Environment.ProcessorCount];

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
            ObjectSentinel = new SupersededObjectValue(this);
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

        /// <summary>Take a writer that an earlier capture returned, or null if none is kept.</summary>
        /// <remarks>Call only within a capture admitted by <see cref="TryBeginCapture"/>.</remarks>
        internal TWriter RentCaptureWriter<TWriter>() where TWriter : class, IStreamBuffer
        {
            var start = Environment.CurrentManagedThreadId;
            for (var i = 0; i < captureWriters.Length; i++)
            {
                ref var slot = ref captureWriters[(int)((uint)(start + i) % (uint)captureWriters.Length)];
                if (Volatile.Read(ref slot) is not null && Interlocked.Exchange(ref slot, null) is TWriter writer)
                    return writer;
            }
            return null;
        }

        /// <summary>Keep <paramref name="writer"/>, which has no record in progress, for a later capture; dispose it if every slot is taken.</summary>
        /// <remarks>Call only within a capture admitted by <see cref="TryBeginCapture"/>, before <see cref="EndCapture"/>, so that
        /// <see cref="CloseAndDrain"/> finds every kept writer.</remarks>
        internal void ReturnCaptureWriter(IStreamBuffer writer)
        {
            var start = Environment.CurrentManagedThreadId;
            for (var i = 0; i < captureWriters.Length; i++)
            {
                if (Interlocked.CompareExchange(ref captureWriters[(int)((uint)(start + i) % (uint)captureWriters.Length)], writer, null) is null)
                    return;
            }
            writer.Dispose();
        }

        /// <summary>Stop admitting captures, wait for those in progress, and dispose the writers they kept.</summary>
        internal void CloseAndDrain()
        {
            _ = Interlocked.Exchange(ref closed, 1);
            if (Volatile.Read(ref inFlight) != 0)
                drained.Wait();
            for (var i = 0; i < captureWriters.Length; i++)
                Interlocked.Exchange(ref captureWriters[i], null)?.Dispose();
        }
    }
}