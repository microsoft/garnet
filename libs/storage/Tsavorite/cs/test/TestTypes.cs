﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

#pragma warning disable CA2211 // Non-constant fields should not be visible (This is for the .Instance members)

namespace Tsavorite.test
{
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct KeyStruct
    {
        [FieldOffset(0)]
        public long kfield1;
        [FieldOffset(8)]
        public long kfield2;

        public override readonly string ToString() => $"kfield1 {kfield1}, kfield2 {kfield2}";

        public struct Comparer : IKeyComparer
        {
            public readonly long GetHashCode64(SpanByte key) => Utility.GetHashCode(key.AsRef<KeyStruct>().kfield1);

            public readonly bool Equals(SpanByte key1, SpanByte key2)
            {
                var k1 = key1.AsRef<KeyStruct>();
                var k2 = key2.AsRef<KeyStruct>();
                return k1.kfield1 == k2.kfield1 && k1.kfield2 == k2.kfield2;
            }

            public static Comparer Instance = new();
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct ValueStruct
    {
        [FieldOffset(0)]
        public long vfield1;
        [FieldOffset(8)]
        public long vfield2;

        public static int AsSpanByteDataSize => sizeof(ValueStruct);

        public override readonly string ToString() => $"vfield1 {vfield1}, vfield2 {vfield2}";
    }

    public struct InputStruct
    {
        public long ifield1;
        public long ifield2;
        public override readonly string ToString() => $"ifield1 {ifield1}, ifield2 {ifield2}";
    }

    public struct OutputStruct
    {
        public ValueStruct value;
    }

    public struct ContextStruct
    {
        public long cfield1;
        public long cfield2;
        public override readonly string ToString() => $"cfield1 {cfield1}, cfield2 {cfield2}";
    }

    public class Functions : FunctionsWithContext<Empty>
    {
    }

    public class FunctionsWithContext<TContext> : SessionFunctionsBase<SpanByte, InputStruct, OutputStruct, TContext>
    {
        public override void RMWCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield1 + input.ifield1, output.value.vfield1);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield2 + input.ifield2, output.value.vfield2);
        }

        public override void ReadCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield1, output.value.vfield1);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            return true;
        }

        public override bool ConcurrentReader(ref LogRecord<SpanByte> logRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
            => SingleReader(ref logRecord, ref input, ref output, ref readInfo);

        // RMW functions
        public override bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            output.value = value;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            output.value = value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ClassicAssert.IsTrue(srcLogRecord.IsSet);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            var oldValue = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<ValueStruct>();

            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            output.value = newValue;
            return true;
        }

        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref InputStruct input)
            => new() { KeyDataSize = key.Length, ValueDataSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref InputStruct input)
            => new() { KeyDataSize = key.Length, ValueDataSize = value.Length };
    }

    public class FunctionsCompaction : SessionFunctionsBase<SpanByte, InputStruct, OutputStruct, int>
    {
        public override void RMWCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                ClassicAssert.IsTrue(status.Found);
                var key = diskLogRecord.Key.AsRef<KeyStruct>();
                ClassicAssert.AreEqual(key.kfield1, output.value.vfield1);
                ClassicAssert.AreEqual(key.kfield2, output.value.vfield2);
            }
            else
            {
                ClassicAssert.IsFalse(status.Found);
            }
        }

        // Read functions
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            return true;
        }

        public override bool ConcurrentReader(ref LogRecord<SpanByte> logRecord, ref InputStruct input, ref OutputStruct dst, ref ReadInfo readInfo)
            => SingleReader(ref logRecord, ref input, ref dst, ref readInfo);

        // RMW functions
        public override bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            var oldValue = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<ValueStruct>();

            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            return true;
        }

        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref InputStruct input)
            => new() { KeyDataSize = key.Length, ValueDataSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref InputStruct input)
            => new() { KeyDataSize = key.Length, ValueDataSize = value.Length };
    }

    public class FunctionsCopyOnWrite : SessionFunctionsBase<SpanByte, InputStruct, OutputStruct, Empty>
    {
        private int concurrentWriterCallCount;
        private int inPlaceUpdaterCallCount;

        public int ConcurrentWriterCallCount => concurrentWriterCallCount;
        public int InPlaceUpdaterCallCount => inPlaceUpdaterCallCount;

        public override void RMWCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            var key = diskLogRecord.Key.AsRef<KeyStruct>();
            ClassicAssert.AreEqual(key.kfield1, output.value.vfield1);
            ClassicAssert.AreEqual(key.kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord logRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
        {
            output.value = logRecord.ValueSpan.AsRef<ValueStruct>();
            return true;
        }

        public override bool ConcurrentReader(ref LogRecord<SpanByte> logRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
            => SingleReader(ref logRecord, ref input, ref output, ref readInfo);

        // Upsert functions
        public override bool SingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, SpanByte srcValue, ref OutputStruct output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            logRecord.ValueSpan.AsRef<ValueStruct>() = srcValue.AsRef<ValueStruct>();
            return true;
        }

        public override bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, SpanByte srcValue, ref OutputStruct output, ref UpsertInfo upsertInfo)
        {
            _ = Interlocked.Increment(ref concurrentWriterCallCount);
            return false;
        }

        // RMW functions
        public override bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref inPlaceUpdaterCallCount);
            return false;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ClassicAssert.IsTrue(srcLogRecord.IsSet);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            var oldValue = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<ValueStruct>();

            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            return true;
        }

        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref InputStruct input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref InputStruct input)
            => new() { KeyDataSize = key.Length, ValueDataSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref InputStruct input)
            => new() { KeyDataSize = key.Length, ValueDataSize = value.Length };
    }

    public class SimpleLongSimpleFunctions : SessionFunctionsBase<SpanByte, long, long, Empty>
    {
        private readonly Func<SpanByte, SpanByte, long> merger;

        public SimpleLongSimpleFunctions() : base() => merger = (input, oldValue) => input.AsRef<long>();

        public SimpleLongSimpleFunctions(Func<SpanByte, SpanByte, long> merger) => this.merger = merger;

        /// <inheritdoc/>
        public override bool ConcurrentReader(ref LogRecord<SpanByte> logRecord, ref long input, ref long output, ref ReadInfo readInfo)
        {
            output = logRecord.ValueSpan.AsRef<long>();
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref long input, ref long output, ref ReadInfo readInfo)
        {
            output = srcLogRecord.ValueSpan.AsRef<long>();
            return true;
        }

        public override bool SingleWriter(ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref long input, SpanByte srcValue, ref long output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var result = base.SingleWriter(ref dstLogRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);
            if (result)
                output = srcValue.AsRef<long>();
            return result;
        }

        public override bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref long input, SpanByte srcValue, ref long output, ref UpsertInfo upsertInfo)
        {
            var result = base.ConcurrentWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = srcValue.AsRef<long>();
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
        {
            var ok = dstLogRecord.TrySetValueSpan(SpanByteFrom(ref input), ref sizeInfo);
            if (ok)
                output = input;
            return ok;
        }

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
        {
            ClassicAssert.IsTrue(dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo), "Failed TryCopyRecordValues");
            var result = output = merger(SpanByteFrom(ref input), srcLogRecord.ValueSpan);   // 'result' must be local for SpanByteFrom; 'output' may be on the heap
            return dstLogRecord.TrySetValueSpan(SpanByteFrom(ref result), ref sizeInfo);
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
        {
            var result = output = merger(SpanByteFrom(ref input), logRecord.ValueSpan);   // 'result' must be local for SpanByteFrom; 'output' may be on the heap
            return logRecord.TrySetValueSpan(SpanByteFrom(ref result), ref sizeInfo);
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref long input)
            => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = sizeof(long) };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref long input)
            => new() { KeyDataSize = key.Length, ValueDataSize = sizeof(long) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref long input)
            => new() { KeyDataSize = key.Length, ValueDataSize = value.Length };
    }
}