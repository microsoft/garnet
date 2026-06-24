// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<KeyWithNamespaceStruct.Comparer, SpanByteRecordTriggers>>;
    using StructStoreFunctions = StoreFunctions<KeyWithNamespaceStruct.Comparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Tests covering basic operations on keys with namespaces
    /// </summary>
    [TestFixture]
    public sealed class NamespaceTests : TestBase
    {
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private ClientSession<KeyWithNamespaceStruct, InputStruct, OutputStruct, Empty, Functions, StructStoreFunctions, StructAllocator> session;
        private BasicContext<KeyWithNamespaceStruct, InputStruct, OutputStruct, Empty, Functions, StructStoreFunctions, StructAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);
        }

        private void Setup(KVSettings kvSettings, TestDeviceType deviceType, int latencyUs = DefaultLocalMemoryDeviceLatencyUs)
        {
            kvSettings.IndexSize = 1L << 13;

            string filename = Path.Join(MethodTestDir, TestContext.CurrentContext.Test.Name + deviceType.ToString() + ".log");
            log = CreateTestDevice(deviceType, filename, latencyUs: latencyUs);
            kvSettings.LogDevice = log;
            kvSettings.CheckpointDir = MethodTestDir;

            store = new(kvSettings
                , StoreFunctions.Create(KeyWithNamespaceStruct.Comparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<KeyWithNamespaceStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions());
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        private void TearDown(bool deleteDir)
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;

            // Do NOT clean up here unless specified, as tests use this TearDown() to prepare for recovery
            if (deleteDir)
                OnTearDown();
        }

        [Test]
        public void BasicOps([Values(0, 1, 4, (int)sbyte.MaxValue)] int namespaceSize, [Values] TestDeviceType deviceType)
        {
            const int KeyField1 = 13;
            const int KeyField2 = 14;

            const int ValField1 = 23;
            const int ValField2 = 24;

            Setup(new() { PageSize = 1L << 12, LogMemorySize = 1L << 13, SegmentSize = 1L << 22 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            byte[] ns1;
            if (namespaceSize == 0)
            {
                ns1 = null;
            }
            else
            {
                ns1 = new byte[namespaceSize];
                for (var i = 0; i < ns1.Length; i++)
                {
                    ns1[i] = (byte)(i + 1);
                }
            }

            // Upsert with namespace succeeds
            var key1 = new KeyWithNamespaceStruct { kfield1 = KeyField1, kfield2 = KeyField2, namespaceArr = ns1 };
            var value1 = new ValueStruct { vfield1 = ValField1, vfield2 = ValField2 };

            var upsertStatus = bContext.Upsert(key1, SpanByte.FromPinnedVariable(ref value1), Empty.Default);
            AssertCompleted(new(OperationStatus.NOTFOUND | OperationStatus.CREATED_RECORD), upsertStatus);

            // Reading same key succeeds
            var readSameStatus = bContext.Read(key1, ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), readSameStatus);
            ClassicAssert.IsTrue(value1.vfield1 == output.value.vfield1 && value1.vfield2 == output.value.vfield2);

            // Reading same key, different namespaces fails
            foreach (var otherNamespaceSize in new int[] { 0, 1, 4, sbyte.MaxValue })
            {
                byte[] ns1Other;
                if (otherNamespaceSize == 0)
                {
                    ns1Other = null;
                }
                else
                {
                    ns1Other = new byte[otherNamespaceSize];
                    for (var i = 0; i < ns1Other.Length; i++)
                    {
                        ns1Other[i] = (byte)(i + 1);
                    }
                }

                if (otherNamespaceSize == namespaceSize)
                {
                    if (otherNamespaceSize == 0)
                    {
                        continue;
                    }

                    for (var i = 0; i < ns1Other.Length; i++)
                    {
                        ns1Other[i] = (byte)~ns1Other[i];
                    }
                }

                var key1OtherNs = new KeyWithNamespaceStruct { kfield1 = KeyField1, kfield2 = KeyField2, namespaceArr = ns1Other };

                var readOtherStatus = bContext.Read(key1OtherNs, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), readOtherStatus);
            }

            // Reading same key, truncated namespace fails
            for (var truncatesNamespaceSize = namespaceSize - 1; truncatesNamespaceSize > 0; truncatesNamespaceSize--)
            {
                var ns1Other = new byte[truncatesNamespaceSize];
                ns1.AsSpan()[..ns1Other.Length].CopyTo(ns1Other);

                var key1OtherNs = new KeyWithNamespaceStruct { kfield1 = KeyField1, kfield2 = KeyField2, namespaceArr = ns1Other };

                var readOtherStatus = bContext.Read(key1OtherNs, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), readOtherStatus);
            }

            // Update same namespace succeeds
            var value2 = new ValueStruct { vfield1 = value1.vfield1 + 1, vfield2 = value1.vfield2 + 1 };
            var updateStatus = bContext.Upsert(key1, SpanByte.FromPinnedVariable(ref value2), Empty.Default);
            AssertCompleted(new(OperationStatus.INPLACE_UPDATED_RECORD), updateStatus);

            // Deletes same key, different namespace fail
            foreach (var otherNamespaceSize in new int[] { 0, 1, 4, sbyte.MaxValue })
            {
                byte[] ns1Other;
                if (otherNamespaceSize == 0)
                {
                    ns1Other = null;
                }
                else
                {
                    ns1Other = new byte[otherNamespaceSize];
                    for (var i = 0; i < ns1Other.Length; i++)
                    {
                        ns1Other[i] = (byte)(i + 1);
                    }
                }

                if (otherNamespaceSize == namespaceSize)
                {
                    if (otherNamespaceSize == 0)
                    {
                        continue;
                    }

                    for (var i = 0; i < ns1Other.Length; i++)
                    {
                        ns1Other[i] = (byte)~ns1Other[i];
                    }
                }

                var key1OtherNs = new KeyWithNamespaceStruct { kfield1 = KeyField1, kfield2 = KeyField2, namespaceArr = ns1Other };

                var delOtherStatus = bContext.Delete(key1OtherNs, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), delOtherStatus);
            }

            // Delete same key succeeds
            var delStatus = bContext.Delete(key1, Empty.Default);
            AssertCompleted(new(OperationStatus.INPLACE_UPDATED_RECORD), delStatus);
        }

        [Test]
        public async Task RecoveryAsync([Values(0, 1, 4, (int)sbyte.MaxValue)] int namespaceSize, [Values] TestDeviceType deviceType)
        {
            const int KeyField1 = 13;
            const int KeyField2 = 14;

            const int ValField1 = 23;
            const int ValField2 = 24;

            Setup(new() { PageSize = 1L << 12, LogMemorySize = 1L << 13, SegmentSize = 1L << 22 }, deviceType);

            byte[] ns1;
            if (namespaceSize == 0)
            {
                ns1 = null;
            }
            else
            {
                ns1 = new byte[namespaceSize];
                for (var i = 0; i < ns1.Length; i++)
                {
                    ns1[i] = (byte)(i + 1);
                }
            }

            // Upsert
            var key1 = new KeyWithNamespaceStruct { kfield1 = KeyField1, kfield2 = KeyField2, namespaceArr = ns1 };
            var value1 = new ValueStruct { vfield1 = ValField1, vfield2 = ValField2 };

            var upsertStatus = bContext.Upsert(key1, SpanByte.FromPinnedVariable(ref value1), Empty.Default);
            AssertCompleted(new(OperationStatus.NOTFOUND | OperationStatus.CREATED_RECORD), upsertStatus);

            // Checkpoint
            while (!store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot))
            {
                await Task.Yield();
            }
            await store.CompleteCheckpointAsync();

            // Recover
            TearDown(deleteDir: false);
            Setup(new() { PageSize = 1L << 12, LogMemorySize = 1L << 13, SegmentSize = 1L << 22 }, deviceType);
            _ = await store.RecoverAsync();

            // Read the upserted value
            InputStruct input = default;
            OutputStruct output = default;
            var readSameStatus = bContext.Read(key1, ref input, ref output);
            AssertCompleted(new(StatusCode.Found), readSameStatus);
            ClassicAssert.IsTrue(value1.vfield1 == output.value.vfield1 && value1.vfield2 == output.value.vfield2);

            // Reading same key, different namespaces fails
            foreach (var otherNamespaceSize in new int[] { 0, 1, 4, sbyte.MaxValue })
            {
                byte[] ns1Other;
                if (otherNamespaceSize == 0)
                {
                    ns1Other = null;
                }
                else
                {
                    ns1Other = new byte[otherNamespaceSize];
                    for (var i = 0; i < ns1Other.Length; i++)
                    {
                        ns1Other[i] = (byte)(i + 1);
                    }
                }

                if (otherNamespaceSize == namespaceSize)
                {
                    if (otherNamespaceSize == 0)
                    {
                        continue;
                    }

                    for (var i = 0; i < ns1Other.Length; i++)
                    {
                        ns1Other[i] = (byte)~ns1Other[i];
                    }
                }

                var key1OtherNs = new KeyWithNamespaceStruct { kfield1 = KeyField1, kfield2 = KeyField2, namespaceArr = ns1Other };

                var readOtherStatus = bContext.Read(key1OtherNs, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), readOtherStatus);
            }

            // Reading same key, truncated namespace fails
            for (var truncatesNamespaceSize = namespaceSize - 1; truncatesNamespaceSize > 0; truncatesNamespaceSize--)
            {
                var ns1Other = new byte[truncatesNamespaceSize];
                ns1.AsSpan()[..ns1Other.Length].CopyTo(ns1Other);

                var key1OtherNs = new KeyWithNamespaceStruct { kfield1 = KeyField1, kfield2 = KeyField2, namespaceArr = ns1Other };

                var readOtherStatus = bContext.Read(key1OtherNs, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), readOtherStatus);
            }

            // RMW
            input.ifield1 = 1;
            input.ifield2 = 2;

            var rmwAssert = bContext.RMW(key1, ref input, ref output, Empty.Default);
            AssertCompleted(new(OperationStatus.SUCCESS | OperationStatus.INPLACE_UPDATED_RECORD), rmwAssert);

            // Checkpoint
            while (!store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot))
            {
                await Task.Yield();
            }
            await store.CompleteCheckpointAsync();

            // Recover
            TearDown(deleteDir: false);
            Setup(new() { PageSize = 1L << 12, LogMemorySize = 1L << 13, SegmentSize = 1L << 22 }, deviceType);
            _ = await store.RecoverAsync();

            // Read the RMW'd value
            var reaUpdatedStatus = bContext.Read(key1, ref input, ref output);
            AssertCompleted(new(StatusCode.Found), reaUpdatedStatus);
            ClassicAssert.IsTrue((value1.vfield1 + input.ifield1) == output.value.vfield1 && (value1.vfield2 + input.ifield2) == output.value.vfield2);
        }

        private void AssertCompleted(Status expected, Status actual)
        {
            if (actual.IsPending)
                (actual, _) = CompletePendingResult();
            ClassicAssert.AreEqual(expected, actual);
        }

        private (Status status, OutputStruct output) CompletePendingResult()
        {
            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            return GetSinglePendingResult(completedOutputs);
        }
    }
}