// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Where a (v+1) CopyUpdate wrote the (v) out-of-line components of the record it superseded, in a Snapshot checkpoint's object log.
    /// </summary>
    internal readonly struct SnapshotCapture
    {
        /// <summary>The superseded record's logical address.</summary>
        internal readonly long Address;

        /// <summary>The capture's position word and hints.</summary>
        internal readonly ObjectLogWriteResult Result;

        /// <summary>Whether the record's key is an overflow key, which the capture includes.</summary>
        internal readonly bool KeyIsOverflow;

        internal SnapshotCapture(long address, in ObjectLogWriteResult result, bool keyIsOverflow)
        {
            Address = address;
            Result = result;
            KeyIsOverflow = keyIsOverflow;
        }

        /// <summary>The reference a reader needs to read the capture back. A captured value is always an object.</summary>
        internal ObjectLogRecordRef RecordRef => ObjectLogRecordRef.FromWriteResult(in Result, KeyIsOverflow, valueIsOverflow: false, valueIsObject: true);
    }

    /// <summary>
    /// The captures of one Snapshot checkpoint: the (v) values that its (v+1) CopyUpdates wrote to its object log, in place of caching
    /// serialized bytes on the superseded object.
    /// </summary>
    /// <remarks>
    /// <para>A source is captured only while its main-log image is not durable. The Checkpoint Flush stamps a capture's position into the
    /// snapshot image of its record, and the ReadOnly flush writes the null image for a captured value. So a record that the ReadOnly flush
    /// writes before PERSISTENCE_CALLBACK samples <c>mainLogRecoveryEndAddress</c> has a null value in the main log, which recovery reads
    /// below that address. PERSISTENCE_CALLBACK therefore persists an index of the captures of those records, and recovery copies them into
    /// the main object log and restamps the records before it reads the main log.</para>
    /// <para>Admission closes when PERSISTENCE_CALLBACK has sampled <c>mainLogRecoveryEndAddress</c>. No capture is needed after that: a
    /// record below it was written by a ReadOnly flush that completed before the sample, and so serialized the (v) value before any later
    /// CopyUpdate; a record at or above it is in the snapshot, whose Checkpoint Flush completed every page before the sample.</para>
    /// </remarks>
    internal sealed class SnapshotCaptures
    {
        /// <summary>"CPIX", little-endian.</summary>
        const int IndexMagic = 0x58495043;
        const int IndexFormatVersion = 1;
        const int IndexHeaderSize = 16;
        const int IndexEntrySize = 32;
        const int KeyIsOverflowFlag = 1;

        /// <summary>The checkpoint's object log, which captures are written to.</summary>
        internal readonly ObjectLog Log;

        readonly ConcurrentDictionary<long, SnapshotCapture> captures = new();

        /// <summary>Set when admission has closed and no capture is in flight. Not disposed: a capture that is refused admission after the
        /// drain may still signal it.</summary>
        readonly ManualResetEventSlim drained = new(false);

        int inFlight;
        int closed;
        Exception failure;

        internal SnapshotCaptures(ObjectLog log) => Log = log ?? throw new ArgumentNullException(nameof(log));

        /// <summary>The first exception a capture failed with; a checkpoint with a failed capture cannot recover that record's (v) value.</summary>
        internal Exception Failure => Volatile.Read(ref failure);

        /// <summary>Admit a capture unless admission has closed. A true result must be paired with <see cref="EndCapture"/>.</summary>
        internal bool TryBeginCapture()
        {
            // The increment precedes the test, and CloseAndDrain closes before it counts, so either this capture sees the close or the
            // drain sees this capture.
            _ = Interlocked.Increment(ref inFlight);
            if (Volatile.Read(ref closed) == 0)
                return true;
            EndCapture();
            return false;
        }

        /// <summary>End a capture admitted by <see cref="TryBeginCapture"/>.</summary>
        internal void EndCapture()
        {
            if (Interlocked.Decrement(ref inFlight) == 0 && Volatile.Read(ref closed) != 0)
                drained.Set();
        }

        /// <summary>Record a completed capture of the record at <paramref name="address"/>. Must precede marking its source superseded, which
        /// is what tells the Checkpoint Flush to look for it.</summary>
        internal void Add(long address, in ObjectLogWriteResult result, bool keyIsOverflow)
            => captures[address] = new SnapshotCapture(address, in result, keyIsOverflow);

        /// <summary>Find the capture of the record at <paramref name="address"/>.</summary>
        internal bool TryGet(long address, out SnapshotCapture capture) => captures.TryGetValue(address, out capture);

        /// <summary>Record a capture failure; the first one fails the checkpoint.</summary>
        internal void RecordFailure(Exception exception) => _ = Interlocked.CompareExchange(ref failure, exception, null);

        /// <summary>Close admission and wait for the captures already admitted.</summary>
        /// <remarks>Blocks rather than spins: an admitted capture serializes an object and may wait for object-log space, which can take
        /// milliseconds, and this runs once per checkpoint.</remarks>
        internal void CloseAndDrain()
        {
            _ = Interlocked.Exchange(ref closed, 1);
            if (Volatile.Read(ref inFlight) != 0)
                drained.Wait();
        }

        /// <summary>Encode the captures of records below <paramref name="untilAddress"/>, in address order, as the index recovery reads.</summary>
        /// <returns>The encoded index, or null if there are none.</returns>
        internal byte[] EncodeIndex(long untilAddress, out long count)
        {
            var selected = new List<SnapshotCapture>();
            foreach (var entry in captures)
            {
                if (entry.Key < untilAddress)
                    selected.Add(entry.Value);
            }
            count = selected.Count;
            if (count == 0)
                return null;
            selected.Sort(static (left, right) => left.Address.CompareTo(right.Address));

            var bytes = new byte[checked(IndexHeaderSize + (int)count * IndexEntrySize)];
            var span = bytes.AsSpan();
            BinaryPrimitives.WriteInt32LittleEndian(span, IndexMagic);
            BinaryPrimitives.WriteInt32LittleEndian(span[4..], IndexFormatVersion);
            BinaryPrimitives.WriteInt64LittleEndian(span[8..], count);
            span = span[IndexHeaderSize..];
            foreach (var capture in selected)
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, capture.Address);
                BinaryPrimitives.WriteUInt64LittleEndian(span[8..], capture.Result.PositionWord);
                BinaryPrimitives.WriteInt32LittleEndian(span[16..], capture.Result.KeySizeHint);
                BinaryPrimitives.WriteInt32LittleEndian(span[20..], capture.Result.KeyRdhLengthBits);
                BinaryPrimitives.WriteInt32LittleEndian(span[24..], capture.Result.ValueSizeHint);
                BinaryPrimitives.WriteInt32LittleEndian(span[28..], capture.KeyIsOverflow ? KeyIsOverflowFlag : 0);
                span = span[IndexEntrySize..];
            }
            return bytes;
        }

        /// <summary>Decode an index written by <see cref="EncodeIndex"/>, verifying its framing and that its records are in ascending address order.</summary>
        internal static SnapshotCapture[] DecodeIndex(ReadOnlySpan<byte> bytes, long expectedCount)
        {
            if (bytes.Length < IndexHeaderSize)
                throw new TsavoriteException($"Capture index of {bytes.Length} bytes is shorter than its header");
            if (BinaryPrimitives.ReadInt32LittleEndian(bytes) != IndexMagic)
                throw new TsavoriteException("Capture index has an invalid header");
            var formatVersion = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);
            if (formatVersion != IndexFormatVersion)
                throw new TsavoriteException($"Capture index format {formatVersion} is not supported");
            var count = BinaryPrimitives.ReadInt64LittleEndian(bytes[8..]);
            if (count != expectedCount || count <= 0 || (bytes.Length - IndexHeaderSize) / IndexEntrySize != count || (bytes.Length - IndexHeaderSize) % IndexEntrySize != 0)
                throw new TsavoriteException($"Capture index holds {count} entries in {bytes.Length} bytes; the checkpoint recorded {expectedCount}");

            var result = new SnapshotCapture[count];
            bytes = bytes[IndexHeaderSize..];
            var previousAddress = long.MinValue;
            for (var i = 0; i < result.Length; i++)
            {
                var address = BinaryPrimitives.ReadInt64LittleEndian(bytes);
                if (address <= previousAddress)
                    throw new TsavoriteException($"Capture index entry {i} at address {address} is not above the previous entry's {previousAddress}");
                previousAddress = address;

                ObjectLogWriteResult writeResult = default;
                writeResult.PositionWord = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]);
                writeResult.KeySizeHint = BinaryPrimitives.ReadInt32LittleEndian(bytes[16..]);
                writeResult.KeyRdhLengthBits = BinaryPrimitives.ReadInt32LittleEndian(bytes[20..]);
                writeResult.ValueSizeHint = BinaryPrimitives.ReadInt32LittleEndian(bytes[24..]);
                var flags = BinaryPrimitives.ReadInt32LittleEndian(bytes[28..]);
                result[i] = new SnapshotCapture(address, in writeResult, (flags & KeyIsOverflowFlag) != 0);
                bytes = bytes[IndexEntrySize..];
            }
            return result;
        }
    }
}