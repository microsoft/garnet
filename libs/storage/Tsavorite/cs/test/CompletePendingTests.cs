// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    class CompletePendingTests
    {
        private TsavoriteKV<KeyStruct, ValueStruct> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = Devices.CreateLogDevice($"{TestUtils.MethodTestDir}/CompletePendingTests.log", preallocateFile: true, deleteOnClose: true);
            store = new TsavoriteKV<KeyStruct, ValueStruct>(128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        const int numRecords = 1000;

        static KeyStruct NewKeyStruct(int key) => new() { kfield1 = key, kfield2 = key + numRecords * 10 };
        static ValueStruct NewValueStruct(int key) => new() { vfield1 = key, vfield2 = key + numRecords * 10 };

        static InputStruct NewInputStruct(int key) => new() { ifield1 = key + numRecords * 30, ifield2 = key + numRecords * 40 };
        static ContextStruct NewContextStruct(int key) => new() { cfield1 = key + numRecords * 50, cfield2 = key + numRecords * 60 };

        static void VerifyStructs(int key, ref KeyStruct keyStruct, ref InputStruct inputStruct, ref OutputStruct outputStruct, ref ContextStruct contextStruct, bool useRMW)
        {
            Assert.AreEqual(key, keyStruct.kfield1);
            Assert.AreEqual(key + numRecords * 10, keyStruct.kfield2);
            Assert.AreEqual(key + numRecords * 30, inputStruct.ifield1);
            Assert.AreEqual(key + numRecords * 40, inputStruct.ifield2);

            // RMW causes the InPlaceUpdater to be called, which adds input fields to the value.
            Assert.AreEqual(key + (useRMW ? inputStruct.ifield1 : 0), outputStruct.value.vfield1);
            Assert.AreEqual(key + numRecords * 10 + (useRMW ? inputStruct.ifield2 : 0), outputStruct.value.vfield2);
            Assert.AreEqual(key + numRecords * 50, contextStruct.cfield1);
            Assert.AreEqual(key + numRecords * 60, contextStruct.cfield2);
        }

        class ProcessPending
        {
            // Get the first chunk of outputs as a group, testing realloc.
            private int deferredPendingMax = CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kInitialAlloc + 1;
            private int deferredPending = 0;
            internal Dictionary<int, long> keyAddressDict = new();
            private bool isFirst = true;

            internal bool IsFirst()
            {
                var temp = isFirst;
                isFirst = false;
                return temp;
            }

            internal bool DeferPending()
            {
                if (deferredPending < deferredPendingMax)
                {
                    ++deferredPending;
                    return true;
                }
                return false;
            }

            internal void Process(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs, List<(KeyStruct, long)> rmwCopyUpdatedAddresses)
            {
                var useRMW = rmwCopyUpdatedAddresses is not null;
                Assert.AreEqual(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kInitialAlloc *
                                CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kReallocMultuple, completedOutputs.vector.Length);
                Assert.AreEqual(deferredPending, completedOutputs.maxIndex);
                Assert.AreEqual(-1, completedOutputs.currentIndex);

                var count = 0;
                for (; completedOutputs.Next(); ++count)
                {
                    ref var result = ref completedOutputs.Current;
                    VerifyStructs((int)result.Key.kfield1, ref result.Key, ref result.Input, ref result.Output, ref result.Context, useRMW);
                    if (!useRMW)
                        Assert.AreEqual(keyAddressDict[(int)result.Key.kfield1], result.RecordMetadata.Address);
                    else if (keyAddressDict[(int)result.Key.kfield1] != result.RecordMetadata.Address)
                        rmwCopyUpdatedAddresses.Add((result.Key, result.RecordMetadata.Address));
                }
                completedOutputs.Dispose();
                Assert.AreEqual(deferredPending + 1, count);
                Assert.AreEqual(-1, completedOutputs.maxIndex);
                Assert.AreEqual(-1, completedOutputs.currentIndex);

                deferredPending = 0;
                deferredPendingMax /= 2;
            }

            internal void VerifyNoDeferredPending()
            {
                Assert.AreEqual(0, deferredPendingMax);  // This implicitly does a null check as well as ensures processing actually happened
                Assert.AreEqual(0, deferredPending);
            }

            internal static void VerifyOneNotFound(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs, ref KeyStruct keyStruct)
            {
                Assert.IsTrue(completedOutputs.Next());
                Assert.IsFalse(completedOutputs.Current.Status.Found);
                Assert.AreEqual(keyStruct, completedOutputs.Current.Key);
                Assert.IsFalse(completedOutputs.Next());
                completedOutputs.Dispose();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public async ValueTask ReadAndCompleteWithPendingOutput([Values] bool useRMW, [Values] bool isAsync)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, ContextStruct, FunctionsWithContext<ContextStruct>>(new FunctionsWithContext<ContextStruct>());
            Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it

            ProcessPending processPending = new();

            for (var key = 0; key < numRecords; ++key)
            {
                var keyStruct = NewKeyStruct(key);
                var valueStruct = NewValueStruct(key);
                processPending.keyAddressDict[key] = store.Log.TailAddress;
                session.Upsert(ref keyStruct, ref valueStruct);
            }

            // Flush to make reads or RMWs go pending.
            store.Log.FlushAndEvict(wait: true);

            List<(KeyStruct key, long address)> rmwCopyUpdatedAddresses = new();

            for (var key = 0; key < numRecords; ++key)
            {
                var keyStruct = NewKeyStruct(key);
                var inputStruct = NewInputStruct(key);
                var contextStruct = NewContextStruct(key);
                OutputStruct outputStruct = default;

                if ((key % (numRecords / 10)) == 0)
                {
                    var ksUnfound = keyStruct;
                    ksUnfound.kfield1 += numRecords * 10;
                    if (session.Read(ref ksUnfound, ref inputStruct, ref outputStruct, contextStruct).IsPending)
                    {
                        CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs;
                        if (isAsync)
                            completedOutputs = await session.CompletePendingWithOutputsAsync();
                        else
                            session.CompletePendingWithOutputs(out completedOutputs, wait: true);
                        ProcessPending.VerifyOneNotFound(completedOutputs, ref ksUnfound);
                    }
                }

                // We don't use context (though we verify it), and Read does not use input.
                var status = useRMW
                    ? session.RMW(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct)
                    : session.Read(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct);
                if (status.IsPending)
                {
                    if (processPending.IsFirst())
                    {
                        session.CompletePending(wait: true);        // Test that this does not instantiate CompletedOutputIterator
                        Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it
                        continue;
                    }

                    if (!processPending.DeferPending())
                    {
                        CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs;
                        if (isAsync)
                            completedOutputs = await session.CompletePendingWithOutputsAsync();
                        else
                            session.CompletePendingWithOutputs(out completedOutputs, wait: true);
                        processPending.Process(completedOutputs, useRMW ? rmwCopyUpdatedAddresses : null);
                    }
                    continue;
                }
                Assert.IsTrue(status.Found);
            }
            processPending.VerifyNoDeferredPending();

            // If we are using RMW, then all records were pending and updated their addresses, and we skipped the first one in the loop above.
            if (useRMW)
                Assert.AreEqual(numRecords - 1, rmwCopyUpdatedAddresses.Count);

            foreach (var (key, address) in rmwCopyUpdatedAddresses)
            {
                // ConcurrentReader does not verify the input struct.
                InputStruct inputStruct = default;
                OutputStruct outputStruct = default;
                ReadOptions readOptions = default;

                // This should not be pending since we've not flushed.
                var localKey = key;
                var status = session.Read(ref localKey, ref inputStruct, ref outputStruct, ref readOptions, out RecordMetadata recordMetadata);
                Assert.IsFalse(status.IsPending);
                Assert.AreEqual(address, recordMetadata.Address);
            }
        }
    }
}