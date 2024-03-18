// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    public struct KeyStruct : ITsavoriteEqualityComparer<KeyStruct>
    {
        public long kfield1;
        public long kfield2;

        public long GetHashCode64(ref KeyStruct key)
        {
            return Utility.GetHashCode(key.kfield1);
        }
        public bool Equals(ref KeyStruct k1, ref KeyStruct k2)
        {
            return k1.kfield1 == k2.kfield1 && k1.kfield2 == k2.kfield2;
        }

        public override string ToString() => $"kfield1 {kfield1}, kfield2 {kfield2}";
    }

    public struct ValueStruct
    {
        public long vfield1;
        public long vfield2;
        public override string ToString() => $"vfield1 {vfield1}, vfield2 {vfield2}";
    }

    public struct InputStruct
    {
        public long ifield1;
        public long ifield2;
        public override string ToString() => $"ifield1 {ifield1}, ifield2 {ifield2}";
    }

    public struct OutputStruct
    {
        public ValueStruct value;
    }

    public struct ContextStruct
    {
        public long cfield1;
        public long cfield2;
        public override string ToString() => $"cfield1 {cfield1}, cfield2 {cfield2}";
    }

    public class Functions : FunctionsWithContext<Empty>
    {
    }

    public class FunctionsWithContext<TContext> : FunctionsBase<KeyStruct, ValueStruct, InputStruct, OutputStruct, TContext>
    {
        public override void RMWCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.Record.CopyUpdated);
            Assert.AreEqual(key.kfield1 + input.ifield1, output.value.vfield1);
            Assert.AreEqual(key.kfield2 + input.ifield2, output.value.vfield2);
        }

        public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key.kfield1, output.value.vfield1);
            Assert.AreEqual(key.kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
        {
            Assert.IsFalse(readInfo.RecordInfo.IsNull());
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(readInfo.RecordInfo.IsNull());
            dst.value = value;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            output.value = value;
            return true;
        }

        public override bool InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            output.value = value;
            return true;
        }

        public override bool NeedCopyUpdate(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            return true;
        }

        public override bool CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            output.value = newValue;
            return true;
        }
    }

    public class FunctionsCompaction : FunctionsBase<KeyStruct, ValueStruct, InputStruct, OutputStruct, int>
    {
        public override void RMWCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                Assert.IsTrue(status.Found);
                Assert.AreEqual(key.kfield1, output.value.vfield1);
                Assert.AreEqual(key.kfield2, output.value.vfield2);
            }
            else
            {
                Assert.IsFalse(status.Found);
            }
        }

        // Read functions
        public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            return true;
        }

        public override bool InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            return true;
        }

        public override bool NeedCopyUpdate(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref OutputStruct output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            return true;
        }
    }

    public class FunctionsCopyOnWrite : FunctionsBase<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
    {
        private int _concurrentWriterCallCount;
        private int _inPlaceUpdaterCallCount;

        public int ConcurrentWriterCallCount => _concurrentWriterCallCount;
        public int InPlaceUpdaterCallCount => _inPlaceUpdaterCallCount;

        public override void RMWCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.Record.CopyUpdated);
        }

        public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key.kfield1, output.value.vfield1);
            Assert.AreEqual(key.kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
        {
            Assert.IsFalse(readInfo.RecordInfo.IsNull());
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(readInfo.RecordInfo.IsNull());
            dst.value = value;
            return true;
        }

        // Upsert functions
        public override bool SingleWriter(ref KeyStruct key, ref InputStruct input, ref ValueStruct src, ref ValueStruct dst, ref OutputStruct output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(upsertInfo.RecordInfo.IsNull());
            dst = src;
            return true;
        }

        public override bool ConcurrentWriter(ref KeyStruct key, ref InputStruct input, ref ValueStruct src, ref ValueStruct dst, ref OutputStruct output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(upsertInfo.RecordInfo.IsNull());
            Interlocked.Increment(ref _concurrentWriterCallCount);
            return false;
        }

        // RMW functions
        public override bool InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            return true;
        }

        public override bool InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            Interlocked.Increment(ref _inPlaceUpdaterCallCount);
            return false;
        }

        public override bool NeedCopyUpdate(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref OutputStruct output, ref RMWInfo rmwInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            return true;
        }

        public override bool CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue, ref OutputStruct output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Assert.IsFalse(rmwInfo.RecordInfo.IsNull());
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            return true;
        }
    }

    class RMWSimpleFunctions<Key, Value> : SimpleFunctions<Key, Value>
    {
        public RMWSimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }

        public override bool InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            output = input;
            return true;
        }

        /// <inheritdoc/>
        public override bool CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            base.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);
            output = newValue;
            return true;
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            output = value;
            return true;
        }
    }
}