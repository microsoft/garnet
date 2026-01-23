// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public struct SessionFunctions : ISessionFunctions<Key, Value, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) { value = default; return true; }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostDeleteOperation<TEpochAccessor>(ref Key key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            dst = src;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostUpsertOperation<TEpochAccessor>(ref Key key, ref Input input, ref Value src, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue.value = input.value + oldValue.value;
            return true;
        }

        public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostRMWOperation<TEpochAccessor>(ref Key key, ref Input input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        public int GetRMWModifiedValueLength(ref Value value, ref Input input) => 0;
        public int GetRMWInitialValueLength(ref Input input) => 0;
        public int GetUpsertValueLength(ref Value value, ref Input input) => Value.Size;

        public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo) { }

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void ConvertOutputToHeap(ref Input input, ref Output output) { }
    }
}