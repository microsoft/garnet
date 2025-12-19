// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.readaddress
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    public struct KeyStruct(long first)
    {
        public long key = first;

        public override readonly string ToString() => key.ToString();

        internal struct Comparer : IKeyComparer<KeyStruct>
        {
            public readonly long GetHashCode64(ref KeyStruct key) => Utility.GetHashCode(key.key);

            public readonly bool Equals(ref KeyStruct k1, ref KeyStruct k2) => k1.key == k2.key;
        }
    }

    public struct ValueStruct(long value)
    {
        public long value = value;

        public override readonly string ToString() => value.ToString();
    }
}

namespace Tsavorite.test.readaddress
{
    using StructAllocator = BlittableAllocator<KeyStruct, ValueStruct, StoreFunctions<KeyStruct, ValueStruct, KeyStruct.Comparer, DefaultRecordDisposer<KeyStruct, ValueStruct>>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct, ValueStruct, KeyStruct.Comparer, DefaultRecordDisposer<KeyStruct, ValueStruct>>;

    [AllureNUnit]
    [TestFixture]
    internal class ReadAddressTests : AllureTestBase
    {
        const int NumKeys = 1000;
        const int KeyMod = 100;
        const int MaxLap = NumKeys / KeyMod;
        const int DeleteLap = MaxLap / 2;
        const int DefaultKeyToScan = 42;

        private static int LapOffset(int lap) => lap * NumKeys * 100;

        public struct Output
        {
            public long value;
            public long address;

            public override readonly string ToString() => $"val {value}; addr {address}";
        }

        private static long SetReadOutput(long key, long value) => (key << 32) | value;

        public enum UseReadCache { NoReadCache, ReadCache }

        internal class Functions : SessionFunctionsBase<KeyStruct, ValueStruct, ValueStruct, Output, Empty>
        {
            internal long lastWriteAddress = Constants.kInvalidAddress;
            readonly bool useReadCache;
            internal ReadCopyOptions readCopyOptions = ReadCopyOptions.None;
            bool preserveCopyUpdaterSource;

            internal Functions(bool preserveCopyUpdaterSource = false)
            {
                this.preserveCopyUpdaterSource = preserveCopyUpdaterSource;
                foreach (var arg in TestContext.CurrentContext.Test.Arguments)
                {
                    if (arg is UseReadCache urc)
                    {
                        useReadCache = urc == UseReadCache.ReadCache;
                        continue;
                    }
                }
            }

            public override bool ConcurrentReader(ref KeyStruct key, ref ValueStruct input, ref ValueStruct value, ref Output output, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                output.value = SetReadOutput(key.key, value.value);
                output.address = readInfo.Address;
                return true;
            }

            public override bool SingleReader(ref KeyStruct key, ref ValueStruct input, ref ValueStruct value, ref Output output, ref ReadInfo readInfo)
            {
                output.value = SetReadOutput(key.key, value.value);
                output.address = readInfo.Address;
                return true;
            }

            // Return false to force a chain of values.
            public override bool ConcurrentWriter(ref KeyStruct key, ref ValueStruct input, ref ValueStruct src, ref ValueStruct dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => false;

            public override bool InPlaceUpdater(ref KeyStruct key, ref ValueStruct input, ref ValueStruct value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => false;

            // Record addresses
            public override bool SingleWriter(ref KeyStruct key, ref ValueStruct input, ref ValueStruct src, ref ValueStruct dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                dst = src;
                output.address = upsertInfo.Address;
                lastWriteAddress = upsertInfo.Address;
                return true;
            }

            public override bool InitialUpdater(ref KeyStruct key, ref ValueStruct input, ref ValueStruct value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                lastWriteAddress = rmwInfo.Address;
                output.address = rmwInfo.Address;
                output.value = value.value = input.value;
                return true;
            }

            public override bool CopyUpdater(ref KeyStruct key, ref ValueStruct input, ref ValueStruct oldValue, ref ValueStruct newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                lastWriteAddress = rmwInfo.Address;
                output.address = rmwInfo.Address;
                output.value = newValue.value = input.value;
                rmwInfo.PreserveCopyUpdaterSourceRecord = preserveCopyUpdaterSource;
                return true;
            }

            public override void ReadCompletionCallback(ref KeyStruct key, ref ValueStruct input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (status.Found)
                {
                    if (useReadCache && readCopyOptions.CopyTo == ReadCopyTo.ReadCache)
                        ClassicAssert.AreEqual(Constants.kInvalidAddress, recordMetadata.Address, $"key {key}");
                    else
                        ClassicAssert.AreEqual(output.address, recordMetadata.Address, $"key {key}");  // Should agree with what SingleWriter set
                }
            }

            public override void RMWCompletionCallback(ref KeyStruct key, ref ValueStruct input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (status.Found)
                    ClassicAssert.AreEqual(output.address, recordMetadata.Address);
            }
        }

        private class TestStore : IDisposable
        {
            internal TsavoriteKV<KeyStruct, ValueStruct, StructStoreFunctions, StructAllocator> store;
            internal IDevice logDevice;
            private readonly bool flush;

            internal long[] insertAddresses = new long[NumKeys];

            internal TestStore(bool useReadCache, ReadCopyOptions readCopyOptions, bool flush)
            {
                DeleteDirectory(MethodTestDir, wait: true);
                logDevice = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"));
                this.flush = flush;

                store = new(new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = logDevice,
                    ReadCacheEnabled = useReadCache,
                    ReadCopyOptions = readCopyOptions,
                    // Use small-footprint values
                    PageSize = 1L << 12, // (4K pages)
                    MemorySize = 1L << 20, // (1M memory for main log)

                    CheckpointDir = Path.Join(MethodTestDir, "chkpt")
                }, StoreFunctions<KeyStruct, ValueStruct>.Create(new KeyStruct.Comparer())
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
            }

            internal async ValueTask Flush()
            {
                if (flush)
                {
                    if (!store.UseReadCache)
                        _ = await store.TakeFullCheckpointAsync(CheckpointType.FoldOver);
                    store.Log.FlushAndEvict(wait: true);
                }
            }

            internal async Task Populate(bool useRMW, bool preserveCopyUpdaterSource = false)
            {
                var functions = new Functions(preserveCopyUpdaterSource);
                using var session = store.NewSession<ValueStruct, Output, Empty, Functions>(functions);
                var bContext = session.BasicContext;

                var prevLap = 0;
                for (int ii = 0; ii < NumKeys; ii++)
                {
                    // lap is used to illustrate the changing values
                    var lap = ii / KeyMod;

                    if (lap != prevLap)
                    {
                        await Flush();
                        prevLap = lap;
                    }

                    var key = new KeyStruct(ii % KeyMod);
                    var value = new ValueStruct(key.key + LapOffset(lap));

                    var status = useRMW
                        ? bContext.RMW(ref key, ref value)
                        : bContext.Upsert(ref key, ref value);

                    if (status.IsPending)
                        await bContext.CompletePendingAsync();

                    insertAddresses[ii] = functions.lastWriteAddress;
                    //ClassicAssert.IsTrue(session.ctx.HasNoPendingRequests);

                    // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                    if (lap == DeleteLap)
                        _ = bContext.Delete(ref key);
                }

                await Flush();
            }

            internal bool ProcessChainRecord(Status status, RecordMetadata recordMetadata, int lap, ref Output actualOutput)
            {
                var recordInfo = recordMetadata.RecordInfo;
                ClassicAssert.GreaterOrEqual(lap, 0);
                long expectedValue = SetReadOutput(DefaultKeyToScan, LapOffset(lap) + DefaultKeyToScan);

                ClassicAssert.AreEqual(lap == DeleteLap, recordInfo.Tombstone, $"lap({lap}) == deleteLap({DeleteLap}) != Tombstone ({recordInfo.Tombstone})");
                if (!recordInfo.Tombstone)
                    ClassicAssert.AreEqual(expectedValue, actualOutput.value, $"lap({lap})");

                // Check for end of loop
                return recordInfo.PreviousAddress >= store.Log.BeginAddress;
            }

            internal static void ProcessNoKeyRecord(bool useRMW, Status status, RecordInfo recordInfo, ref Output actualOutput, int keyOrdinal)
            {
                if (status.Found)
                {
                    var keyToScan = keyOrdinal % KeyMod;
                    var lap = keyOrdinal / KeyMod;
                    long expectedValue = SetReadOutput(keyToScan, LapOffset(lap) + keyToScan);
                    if (!recordInfo.Tombstone)
                        ClassicAssert.AreEqual(expectedValue, actualOutput.value, $"keyToScan {keyToScan}, lap({lap})");
                }
            }

            public void Dispose()
            {
                store?.Dispose();
                store = null;
                logDevice?.Dispose();
                logDevice = null;
                DeleteDirectory(MethodTestDir);
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk)]
        [Category("TsavoriteKV"), Category("Read")]
        public void VersionedReadTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk);
            testStore.Populate(updateOp == UpdateOp.RMW).GetAwaiter().GetResult();
            using var session = testStore.store.NewSession<ValueStruct, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Output);
                var input = default(ValueStruct);
                var key = new KeyStruct(DefaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = MaxLap - 1; /* tested in loop */; --lap)
                {
                    // We need a non-AtAddress read to start the loop of returning the previous address to read at.
                    var status = readAtAddress == 0
                        ? bContext.Read(ref key, ref input, ref output, ref readOptions, out _)
                        : bContext.ReadAtAddress(readAtAddress, ref key, ref input, ref output, ref readOptions, out _);

                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        struct IterateKeyTestScanIteratorFunctions : IScanIteratorFunctions<KeyStruct, ValueStruct>
        {
            readonly TestStore testStore;
            internal int numRecords;
            internal int stopAt;

            internal IterateKeyTestScanIteratorFunctions(TestStore ts) => testStore = ts;

            public readonly bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public bool SingleReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                Output output = new() { address = recordMetadata.Address, value = SetReadOutput(key.key, value.value) };
                int lap = MaxLap - ++numRecords;
                ClassicAssert.AreEqual(lap != 0, testStore.ProcessChainRecord(new(StatusCode.Found), recordMetadata, lap, ref output), $"lap ({lap}) == 0 != ProcessChainRecord(...)");
                ClassicAssert.AreEqual(numRecords, numberOfRecords, "mismatched record count");
                return stopAt != numRecords;
            }

            public readonly void OnException(Exception exception, long numberOfRecords) { }

            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(ReadTestCategory)]
        public void IterateKeyTests([Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            // Upsert does not provide the preserveCopyUpdaterSource option, so we cannot check it in NoFlush; the record will always be elided.
            if (flushMode == FlushMode.NoFlush && updateOp == UpdateOp.Upsert)
                Assert.Ignore("Cannot test NoFlush with Upsert");

            using var testStore = new TestStore(useReadCache: false, ReadCopyOptions.None, flushMode != FlushMode.NoFlush);
            testStore.Populate(useRMW: updateOp == UpdateOp.RMW, preserveCopyUpdaterSource: true).GetAwaiter().GetResult();

            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var key = new KeyStruct(DefaultKeyToScan);
                IterateKeyTestScanIteratorFunctions scanFunctions = new(testStore);
                ClassicAssert.IsTrue(testStore.store.Log.IterateKeyVersions(ref scanFunctions, ref key));
                ClassicAssert.AreEqual(MaxLap, scanFunctions.numRecords);
            }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(ReadTestCategory)]
        public void IterateKeyStopTests([Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            // Upsert does not provide the preserveCopyUpdaterSource option, so we cannot check it in NoFlush; the record will always be elided.
            if (flushMode == FlushMode.NoFlush && updateOp == UpdateOp.Upsert)
                Assert.Ignore("Cannot test NoFlush with Upsert");

            using var testStore = new TestStore(useReadCache: false, ReadCopyOptions.None, flushMode != FlushMode.NoFlush);
            testStore.Populate(updateOp == UpdateOp.RMW, preserveCopyUpdaterSource: true).GetAwaiter().GetResult();

            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var key = new KeyStruct(DefaultKeyToScan);
                IterateKeyTestScanIteratorFunctions scanFunctions = new(testStore) { stopAt = 4 };
                ClassicAssert.IsFalse(testStore.store.Log.IterateKeyVersions(ref scanFunctions, ref key));
                ClassicAssert.AreEqual(scanFunctions.stopAt, scanFunctions.numRecords);
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk)]
        [Category("TsavoriteKV"), Category("Read")]
        public void ReadAtAddressTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk);
            testStore.Populate(updateOp == UpdateOp.RMW).GetAwaiter().GetResult();
            using var session = testStore.store.NewSession<ValueStruct, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Output);
                var input = default(ValueStruct);
                var key = new KeyStruct(DefaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = MaxLap - 1; /* tested in loop */; --lap)
                {
                    var status = readAtAddress == 0
                        ? bContext.Read(ref key, ref input, ref output, ref readOptions, out recordMetadata)
                        : bContext.ReadAtAddress(readAtAddress, ref input, ref output, ref readOptions, out recordMetadata);
                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // Test is similar to others but tests the Overload where RadCopy*.None is set -- probably don't need all combinations of test but doesn't hurt
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk)]
        [Category("TsavoriteKV"), Category("Read")]
        public async Task ReadAtAddressCopyOptNoRcTest(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk);
            await testStore.Populate(updateOp == UpdateOp.RMW);
            using var session = testStore.store.NewSession<ValueStruct, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(ValueStruct);
                var key = new KeyStruct(DefaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = MaxLap - 1; /* tested in loop */; --lap)
                {
                    Output output = new();
                    Status status = readAtAddress == 0
                        ? bContext.Read(ref key, ref input, ref output, ref readOptions, out recordMetadata)
                        : bContext.ReadAtAddress(readAtAddress, ref input, ref output, ref readOptions, out recordMetadata);
                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk)]
        [Category("TsavoriteKV"), Category("Read")]
        public async ValueTask ReadNoKeyTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk);
            await testStore.Populate(updateOp == UpdateOp.RMW);
            using var session = testStore.store.NewSession<ValueStruct, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var output = default(Output);
                var input = default(ValueStruct);

                for (int ii = 0; ii < NumKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(NumKeys);

                    ReadOptions readOptions = new()
                    {
                        CopyOptions = session.functions.readCopyOptions
                    };
                    var status = bContext.ReadAtAddress(testStore.insertAddresses[keyOrdinal], ref input, ref output, ref readOptions, out RecordMetadata recordMetadata);
                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }

                    TestStore.ProcessNoKeyRecord(updateOp == UpdateOp.RMW, status, recordMetadata.RecordInfo, ref output, keyOrdinal);
                }

                await testStore.Flush().AsTask();
            }
        }

        internal struct ReadCopyOptionsMerge
        {
            internal ReadCopyOptions store, Session, Read, Expected;
        }

        [Test]
        [Category("TsavoriteKV"), Category("Read")]
        public void ReadCopyOptionssMergeTest()
        {
            ReadCopyOptionsMerge[] merges =
            [
                new()
                {
                    store = ReadCopyOptions.None,
                    Session = default,
                    Read = default,
                    Expected = ReadCopyOptions.None
                },
                new()
                {
                    store = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                    Session = default,
                    Read = default,
                    Expected = new(ReadCopyFrom.Device, ReadCopyTo.MainLog)
                },
                new()
                {
                    store = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                    Session = ReadCopyOptions.None,
                    Read = default,
                    Expected = ReadCopyOptions.None
                },
                new()
                {
                    store = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                    Session = default,
                    Read = ReadCopyOptions.None,
                    Expected = ReadCopyOptions.None
                },
                new()
                {
                    store = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                    Session = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache),
                    Read = default,
                    Expected = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache)
                },
                new()
                {
                    store = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                    Session = default,
                    Read = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache),
                    Expected = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache)
                },
                new()
                {
                    store = ReadCopyOptions.None,
                    Session = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                    Read = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache),
                    Expected = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache)
                },
            ];

            for (var ii = 0; ii < merges.Length; ++ii)
            {
                var merge = merges[ii];
                var options = ReadCopyOptions.Merge(ReadCopyOptions.Merge(merge.store, merge.Session), merge.Read);
                ClassicAssert.AreEqual(merge.Expected, options, $"iter {ii}");
            }
        }
    }
}