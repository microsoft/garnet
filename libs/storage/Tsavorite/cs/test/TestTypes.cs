// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;
using NUnit.Framework.Legacy;
using Tsavorite.core;

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
            public readonly long GetHashCode64(ReadOnlySpan<byte> key) => Utility.GetHashCode(key.AsRef<KeyStruct>().kfield1);

            public readonly bool Equals(ReadOnlySpan<byte> key1, ReadOnlySpan<byte> key2)
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

    public class FunctionsWithContext<TContext> : SessionFunctionsBase<InputStruct, OutputStruct, TContext>
    {
        public override void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield1 + input.ifield1, output.value.vfield1);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield2 + input.ifield2, output.value.vfield2);
        }

        public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield1, output.value.vfield1);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<KeyStruct>().kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            output.value = value;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            output.value = value;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ClassicAssert.IsTrue(srcLogRecord.IsSet);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            var oldValue = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<ValueStruct>();

            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            output.value = newValue;
            return true;
        }

        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref InputStruct input)
            => new() { KeySize = key.Length, ValueSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref InputStruct input)
            => new() { KeySize = key.Length, ValueSize = value.Length };
    }

    public class FunctionsCompaction : SessionFunctionsBase<InputStruct, OutputStruct, int>
    {
        public override void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
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
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            var oldValue = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<ValueStruct>();

            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            return true;
        }

        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref InputStruct input)
            => new() { KeySize = key.Length, ValueSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref InputStruct input)
            => new() { KeySize = key.Length, ValueSize = value.Length };
    }

    public class FunctionsCopyOnWrite : SessionFunctionsBase<InputStruct, OutputStruct, Empty>
    {
        private int inPlaceWriterCallCount;
        private int inPlaceUpdaterCallCount;

        public int InPlaceWriterCallCount => inPlaceWriterCallCount;
        public int InPlaceUpdaterCallCount => inPlaceUpdaterCallCount;

        public override void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            var key = diskLogRecord.Key.AsRef<KeyStruct>();
            ClassicAssert.AreEqual(key.kfield1, output.value.vfield1);
            ClassicAssert.AreEqual(key.kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
        {
            output.value = logRecord.ValueSpan.AsRef<ValueStruct>();
            return true;
        }

        // Upsert functions
        public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ReadOnlySpan<byte> srcValue, ref OutputStruct output, ref UpsertInfo upsertInfo)
        {
            logRecord.ValueSpan.AsRef<ValueStruct>() = srcValue.AsRef<ValueStruct>();
            return true;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ReadOnlySpan<byte> srcValue, ref OutputStruct output, ref UpsertInfo upsertInfo)
        {
            _ = Interlocked.Increment(ref inPlaceWriterCallCount);
            return false;
        }

        // RMW functions
        public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ref var value = ref logRecord.ValueSpan.AsRef<ValueStruct>();
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref inPlaceUpdaterCallCount);
            return false;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            ClassicAssert.IsTrue(srcLogRecord.IsSet);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref InputStruct input, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            var oldValue = srcLogRecord.ValueSpan.AsRef<ValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<ValueStruct>();

            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            return true;
        }

        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref InputStruct input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref InputStruct input)
            => new() { KeySize = key.Length, ValueSize = sizeof(ValueStruct) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref InputStruct input)
            => new() { KeySize = key.Length, ValueSize = value.Length };
    }

    public class SimpleLongSimpleFunctions : SessionFunctionsBase<long, long, Empty>
    {
        private readonly Func<long, long, long> merger;

        public SimpleLongSimpleFunctions() : base() => merger = (input, oldValue) => input;

        public SimpleLongSimpleFunctions(Func<long, long, long> merger) => this.merger = merger;

        /// <inheritdoc/>
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref long input, ref long output, ref ReadInfo readInfo)
        {
            output = srcLogRecord.ValueSpan.AsRef<long>();
            return true;
        }

        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ReadOnlySpan<byte> srcValue, ref long output, ref UpsertInfo upsertInfo)
        {
            var result = base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = srcValue.AsRef<long>();
            return result;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref long input, ReadOnlySpan<byte> srcValue, ref long output, ref UpsertInfo upsertInfo)
        {
            var result = base.InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = srcValue.AsRef<long>();
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
        {
            var ok = dstLogRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref input), in sizeInfo);
            if (ok)
                output = input;
            return ok;
        }

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
        {
            ClassicAssert.IsTrue(dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo), "Failed TryCopyRecordValues");
            var result = output = merger(input, srcLogRecord.ValueSpan.AsRef<long>());   // 'result' must be local for SpanByte.From; 'output' may be on the heap
            return dstLogRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref result), in sizeInfo);
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
        {
            var result = output = merger(input, logRecord.ValueSpan.AsRef<long>());   // 'result' must be local for SpanByte.From; 'output' may be on the heap
            return logRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref result), in sizeInfo);
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref long input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = sizeof(long) };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref long input)
            => new() { KeySize = key.Length, ValueSize = sizeof(long) };
        /// <inheritdoc/>
        public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref long input)
            => new() { KeySize = key.Length, ValueSize = value.Length };
    }
}