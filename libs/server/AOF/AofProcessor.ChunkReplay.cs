// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed unsafe partial class AofProcessor
    {
        /// <summary>
        /// Replay a completed chunked record directly from its <see cref="ChunkedAccumulator"/> (no contiguous record image is built).
        /// A chunked record is always a data op (Store/Object/Unified Upsert/RMW/Delete): it is never a transaction marker,
        /// checkpoint, flush, stored procedure, or vector op, so only the transaction buffer and the op dispatch are consulted.
        /// </summary>
        internal void ProcessAofRecordInternal(int virtualSublogIdx, ChunkedAccumulator acc, bool asReplica, long logAddressSequenceNumber = 0)
        {
            // If a transaction is active for this session, the op is buffered into its group; otherwise it is standalone.
            if (aofReplayCoordinator.AddOrReplayTransactionOperation(virtualSublogIdx, acc))
                return;

            var replayContext = aofReplayCoordinator.GetReplayContext(virtualSublogIdx);
            _ = ReplayOpDispatch(virtualSublogIdx, acc, replayContext,
                replayContext.StringBasicContext, replayContext.ObjectBasicContext, replayContext.UnifiedBasicContext,
                asReplica, logAddressSequenceNumber);
        }

        /// <summary>
        /// ChunkedAccumulator-fed counterpart of the pointer-based <c>ReplayOpDispatch</c>: performs the per-topology
        /// read-consistency update (mirroring the preprocessKey structs) and dispatches to <c>ReplayOp</c>.
        /// </summary>
        internal bool ReplayOpDispatch<TStringContext, TObjectContext, TUnifiedContext>(
                int virtualSublogIdx,
                ChunkedAccumulator acc,
                AofReplayContext replayContext,
                TStringContext stringContext,
                TObjectContext objectContext,
                TUnifiedContext unifiedContext,
                bool asReplica,
                long logAddressSequenceNumber = 0)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Read-consistency update: sharded records carry an embedded sequence number; single-physical-log multi-replay
            // uses the entry log address; single-log needs no update (see the SingleLog/SinglePhysicalLog/Sharded preprocessKey).
            if (storeWrapper.serverOptions.AofPhysicalSublogCount > 1)
                storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(virtualSublogIdx, acc.keyHash, acc.sequenceNumber);
            else if (usingSinglePhysicalLogMultiReplay)
                storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(virtualSublogIdx, acc.keyHash, logAddressSequenceNumber);

            return ReplayOp(virtualSublogIdx, acc, replayContext, stringContext, objectContext, unifiedContext, asReplica);
        }

        private bool ReplayOp<TStringContext, TObjectContext, TUnifiedContext>(
                int virtualSublogIdx,
                ChunkedAccumulator acc,
                AofReplayContext replayContext,
                TStringContext stringContext,
                TObjectContext objectContext,
                TUnifiedContext unifiedContext,
                bool asReplica)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // StoreRMW can queue VADDs onto different threads; everything else must wait for those to complete first.
            // Skip (1) entries from a prior checkpoint; buffer (2) future entries in the fuzzy region.
            if (!BeginReplayOp(replayContext, acc.opType, ShouldSkipRecord(virtualSublogIdx, replayContext.inFuzzyRegion, acc, asReplica), out var bufferPtr, out var bufferLength))
                return false;

            switch (acc.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(acc, stringContext, ref replayContext.parseState);
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(acc, stringContext, ref replayContext.parseState);
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(acc, stringContext);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(acc, objectContext, storeWrapper.GarnetObjectSerializer, bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(acc, objectContext, ref replayContext.parseState, bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(acc, objectContext);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    UnifiedStoreStringUpsert(acc, unifiedContext, ref replayContext.parseState, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    UnifiedStoreRMW(acc, unifiedContext, ref replayContext.parseState, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreObjectUpsert:
                    UnifiedStoreObjectUpsert(acc, unifiedContext, storeWrapper.GarnetObjectSerializer, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreDelete:
                    UnifiedStoreDelete(acc, unifiedContext);
                    break;
                default:
                    throw new GarnetException($"Unexpected chunked op type: {acc.opType}");
            }
            return true;
        }

        /// <summary>
        /// On recovery apply records with header.version greater than CurrentVersion.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="inFuzzyRegion"></param>
        /// <param name="acc"></param>
        /// <param name="asReplica"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        bool ShouldSkipRecord(int sublogIdx, bool inFuzzyRegion, ChunkedAccumulator acc, bool asReplica)
        {
            return (asReplica && inFuzzyRegion) // Buffer logic only for AOF version > 1
                ? BufferNewVersionRecord()
                : acc.storeVersion < storeWrapper.store.CurrentVersion;

            bool BufferNewVersionRecord()
            {
                if (acc.storeVersion > storeWrapper.store.CurrentVersion)
                {
                    aofReplayCoordinator.AddFuzzyRegionOperation(sublogIdx, acc);
                    return true;
                }
                return false;
            }
        }

        static void StoreUpsert<TStringContext>(ChunkedAccumulator acc, TStringContext stringContext, ref SessionParseState parseState)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
            fixed (byte* valuePtr = acc.value)
            fixed (byte* inputPtr = acc.input)
            {
                var key = (FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset);
                var value = PinnedSpanByte.FromPinnedPointer(valuePtr, acc.valueOffset);
                var stringInput = new StringInput { parseState = parseState };
                _ = stringInput.DeserializeFrom(inputPtr);

                StringOutput output = default;
                var upsertOptions = new UpsertOptions() { KeyHash = acc.keyHash };
                _ = stringContext.Upsert(key, ref stringInput, value, ref output, ref upsertOptions);
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();
            }
        }

        static void StoreRMW<TStringContext>(ChunkedAccumulator acc, TStringContext stringContext, ref SessionParseState parseState)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
            fixed (byte* inputPtr = acc.input)
            {
                var stringInput = new StringInput { parseState = parseState };
                _ = stringInput.DeserializeFrom(inputPtr);
                // Vector/range-index RMW sub-dispatch is intentionally not supported on the chunked path.
                Debug.Assert(stringInput.header.cmd is not (RespCommand.VADD or RespCommand.VREM),
                    "chunked vector operations are not supported on the accumulator replay path");

                var key = (FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset);
                var output = StringOutput.FromPinnedSpan(stackalloc byte[32]);
                var rmwOptions = new RMWOptions { KeyHash = acc.keyHash };
                var status = stringContext.RMW(key, ref stringInput, ref output, ref rmwOptions);
                if (status.IsPending)
                    StorageSession.CompletePendingForSession(ref status, ref output, ref stringContext);
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();
            }
        }

        static void StoreDelete<TStringContext>(ChunkedAccumulator acc, TStringContext stringContext)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
                _ = stringContext.Delete((FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset));
        }

        static void ObjectStoreUpsert<TObjectContext>(ChunkedAccumulator acc, TObjectContext objectContext, GarnetObjectSerializer garnetObjectSerializer, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Stream-deserialize the object value from its chunks (no contiguous copy).
            var valueObject = garnetObjectSerializer.Deserialize(acc.GetValueSequence());
            fixed (byte* keyPtr = acc.key)
            {
                var key = (FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset);
                var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
                var upsertOptions = new UpsertOptions() { KeyHash = acc.keyHash };
                _ = objectContext.Upsert(key, valueObject, ref upsertOptions);
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();
            }
        }

        static void ObjectStoreRMW<TObjectContext>(ChunkedAccumulator acc, TObjectContext objectContext, ref SessionParseState parseState, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
            fixed (byte* inputPtr = acc.input)
            {
                var objectInput = new ObjectInput { parseState = parseState };
                _ = objectInput.DeserializeFrom(inputPtr);

                var key = (FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset);
                var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
                var rmwOptions = new RMWOptions { KeyHash = acc.keyHash };
                var status = objectContext.RMW(key, ref objectInput, ref output, ref rmwOptions);
                if (status.IsPending)
                    StorageSession.CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();
            }
        }

        static void ObjectStoreDelete<TObjectContext>(ChunkedAccumulator acc, TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
                _ = objectContext.Delete((FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset));
        }

        static void UnifiedStoreStringUpsert<TUnifiedContext>(ChunkedAccumulator acc, TUnifiedContext unifiedContext, ref SessionParseState parseState, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
            fixed (byte* valuePtr = acc.value)
            fixed (byte* inputPtr = acc.input)
            {
                var key = (FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset);
                var value = PinnedSpanByte.FromPinnedPointer(valuePtr, acc.valueOffset);
                var unifiedInput = new UnifiedInput { parseState = parseState };
                _ = unifiedInput.DeserializeFrom(inputPtr);

                var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
                var upsertOptions = new UpsertOptions() { KeyHash = acc.keyHash };
                _ = unifiedContext.Upsert(key, ref unifiedInput, value, ref output, ref upsertOptions);
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();
            }
        }

        static void UnifiedStoreRMW<TUnifiedContext>(ChunkedAccumulator acc, TUnifiedContext unifiedContext, ref SessionParseState parseState, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
            fixed (byte* inputPtr = acc.input)
            {
                var unifiedInput = new UnifiedInput { parseState = parseState };
                _ = unifiedInput.DeserializeFrom(inputPtr);

                var key = (FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset);
                var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
                var rmwOptions = new RMWOptions { KeyHash = acc.keyHash };
                var status = unifiedContext.RMW(key, ref unifiedInput, ref output, ref rmwOptions);
                if (status.IsPending)
                    StorageSession.CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();
            }
        }

        static void UnifiedStoreObjectUpsert<TUnifiedContext>(ChunkedAccumulator acc, TUnifiedContext unifiedContext, GarnetObjectSerializer garnetObjectSerializer, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var valueObject = garnetObjectSerializer.Deserialize(acc.GetValueSequence());
            fixed (byte* keyPtr = acc.key)
            {
                var key = (FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset);
                var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
                var upsertOptions = new UpsertOptions() { KeyHash = acc.keyHash };
                _ = unifiedContext.Upsert(key, valueObject, ref upsertOptions);
                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();
            }
        }

        static void UnifiedStoreDelete<TUnifiedContext>(ChunkedAccumulator acc, TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fixed (byte* keyPtr = acc.key)
                _ = unifiedContext.Delete((FixedSpanByteKey)new Span<byte>(keyPtr, acc.keyOffset));
        }
    }
}