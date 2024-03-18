// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.readaddress
{
    [TestFixture]
    internal class ReadAddressTests
    {
        const int numKeys = 1000;
        const int keyMod = 100;
        const int maxLap = numKeys / keyMod;
        const int deleteLap = maxLap / 2;
        const int defaultKeyToScan = 42;

        private static int LapOffset(int lap) => lap * numKeys * 100;

        public struct Key
        {
            public long key;

            public Key(long first) => key = first;

            public override string ToString() => key.ToString();

            internal class Comparer : ITsavoriteEqualityComparer<Key>
            {
                public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.key);

                public bool Equals(ref Key k1, ref Key k2) => k1.key == k2.key;
            }
        }

        public struct Value
        {
            public long value;

            public Value(long value) => this.value = value;

            public override string ToString() => value.ToString();
        }

        public struct Output
        {
            public long value;
            public long address;

            public override string ToString() => $"val {value}; addr {address}";
        }

        private static long SetReadOutput(long key, long value) => (key << 32) | value;

        public enum UseReadCache { NoReadCache, ReadCache }

        internal class Functions : FunctionsBase<Key, Value, Value, Output, Empty>
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

            public override bool ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Output output, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                output.value = SetReadOutput(key.key, value.value);
                output.address = readInfo.Address;
                return true;
            }

            public override bool SingleReader(ref Key key, ref Value input, ref Value value, ref Output output, ref ReadInfo readInfo)
            {
                output.value = SetReadOutput(key.key, value.value);
                output.address = readInfo.Address;
                return true;
            }

            // Return false to force a chain of values.
            public override bool ConcurrentWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => false;

            public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => false;

            // Record addresses
            public override bool SingleWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                dst = src;
                output.address = upsertInfo.Address;
                lastWriteAddress = upsertInfo.Address;
                return true;
            }

            public override bool InitialUpdater(ref Key key, ref Value input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                lastWriteAddress = rmwInfo.Address;
                output.address = rmwInfo.Address;
                output.value = value.value = input.value;
                return true;
            }

            public override bool CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                lastWriteAddress = rmwInfo.Address;
                output.address = rmwInfo.Address;
                output.value = newValue.value = input.value;
                rmwInfo.PreserveCopyUpdaterSourceRecord = preserveCopyUpdaterSource;
                return true;
            }

            public override void ReadCompletionCallback(ref Key key, ref Value input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (status.Found)
                {
                    if (useReadCache && readCopyOptions.CopyTo == ReadCopyTo.ReadCache)
                        Assert.AreEqual(Constants.kInvalidAddress, recordMetadata.Address, $"key {key}");
                    else
                        Assert.AreEqual(output.address, recordMetadata.Address, $"key {key}");  // Should agree with what SingleWriter set
                }
            }

            public override void RMWCompletionCallback(ref Key key, ref Value input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (status.Found)
                    Assert.AreEqual(output.address, recordMetadata.Address);
            }
        }

        private class TestStore : IDisposable
        {
            internal TsavoriteKV<Key, Value> store;
            internal IDevice logDevice;
            internal string testDir;
            private readonly bool flush;

            internal long[] InsertAddresses = new long[numKeys];

            internal TestStore(bool useReadCache, ReadCopyOptions readCopyOptions, bool flush, ConcurrencyControlMode concurrencyControlMode)
            {
                testDir = MethodTestDir;
                DeleteDirectory(testDir, wait: true);
                logDevice = Devices.CreateLogDevice($"{testDir}/hlog.log");
                this.flush = flush;

                var logSettings = new LogSettings
                {
                    LogDevice = logDevice,
                    ObjectLogDevice = new NullDevice(),
                    ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null,
                    ReadCopyOptions = readCopyOptions,
                    // Use small-footprint values
                    PageSizeBits = 12, // (4K pages)
                    MemorySizeBits = 20 // (1M memory for main log)
                };

                store = new TsavoriteKV<Key, Value>(
                    size: 1L << 20,
                    logSettings: logSettings,
                    checkpointSettings: new CheckpointSettings { CheckpointDir = $"{testDir}/chkpt" },
                    serializerSettings: null,
                    comparer: new Key.Comparer(),
                    concurrencyControlMode: concurrencyControlMode
                    );
            }

            internal async ValueTask Flush()
            {
                if (flush)
                {
                    if (!store.UseReadCache)
                        await store.TakeFullCheckpointAsync(CheckpointType.FoldOver);
                    store.Log.FlushAndEvict(wait: true);
                }
            }

            internal async Task Populate(bool useRMW, bool useAsync, bool preserveCopyUpdaterSource = false)
            {
                var functions = new Functions(preserveCopyUpdaterSource);
                using var session = store.NewSession<Value, Output, Empty, Functions>(functions);

                var prevLap = 0;
                for (int ii = 0; ii < numKeys; ii++)
                {
                    // lap is used to illustrate the changing values
                    var lap = ii / keyMod;

                    if (lap != prevLap)
                    {
                        await Flush();
                        prevLap = lap;
                    }

                    var key = new Key(ii % keyMod);
                    var value = new Value(key.key + LapOffset(lap));

                    var status = useRMW
                        ? useAsync
                            ? (await session.RMWAsync(ref key, ref value, serialNo: lap)).Complete().status
                            : session.RMW(ref key, ref value, serialNo: lap)
                        : session.Upsert(ref key, ref value, serialNo: lap);

                    if (status.IsPending)
                        await session.CompletePendingAsync();

                    InsertAddresses[ii] = functions.lastWriteAddress;
                    //Assert.IsTrue(session.ctx.HasNoPendingRequests);

                    // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                    if (lap == deleteLap)
                        session.Delete(ref key, serialNo: lap);
                }

                await Flush();
            }

            internal bool ProcessChainRecord(Status status, RecordMetadata recordMetadata, int lap, ref Output actualOutput)
            {
                var recordInfo = recordMetadata.RecordInfo;
                Assert.GreaterOrEqual(lap, 0);
                long expectedValue = SetReadOutput(defaultKeyToScan, LapOffset(lap) + defaultKeyToScan);

                Assert.AreEqual(lap == deleteLap, recordInfo.Tombstone, $"lap({lap}) == deleteLap({deleteLap}) != Tombstone ({recordInfo.Tombstone})");
                if (!recordInfo.Tombstone)
                    Assert.AreEqual(expectedValue, actualOutput.value, $"lap({lap})");

                // Check for end of loop
                return recordInfo.PreviousAddress >= store.Log.BeginAddress;
            }

            internal static void ProcessNoKeyRecord(bool useRMW, Status status, RecordInfo recordInfo, ref Output actualOutput, int keyOrdinal)
            {
                if (status.Found)
                {
                    var keyToScan = keyOrdinal % keyMod;
                    var lap = keyOrdinal / keyMod;
                    long expectedValue = SetReadOutput(keyToScan, LapOffset(lap) + keyToScan);
                    if (!recordInfo.Tombstone)
                        Assert.AreEqual(expectedValue, actualOutput.value, $"keyToScan {keyToScan}, lap({lap})");
                }
            }

            public void Dispose()
            {
                store?.Dispose();
                store = null;
                logDevice?.Dispose();
                logDevice = null;
                DeleteDirectory(testDir);
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [Category("TsavoriteKV"), Category("Read")]
        public void VersionedReadSyncTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk, concurrencyControlMode);
            testStore.Populate(updateOp == UpdateOp.RMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.store.NewSession<Value, Output, Empty, Functions>(new Functions());

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Output);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    // We need a non-AtAddress read to start the loop of returning the previous address to read at.
                    var status = readAtAddress == 0
                        ? session.Read(ref key, ref input, ref output, ref readOptions, out _, serialNo: maxLap + 1)
                        : session.ReadAtAddress(readAtAddress, ref key, ref input, ref output, ref readOptions, out _, serialNo: maxLap + 1);

                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        struct IterateKeyTestScanIteratorFunctions : IScanIteratorFunctions<Key, Value>
        {
            readonly TestStore testStore;
            internal int numRecords;
            internal int stopAt;

            internal IterateKeyTestScanIteratorFunctions(TestStore ts) => testStore = ts;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref Key key, ref Value value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public bool SingleReader(ref Key key, ref Value value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                Output output = new() { address = recordMetadata.Address, value = SetReadOutput(key.key, value.value) };
                int lap = maxLap - ++numRecords;
                Assert.AreEqual(lap != 0, testStore.ProcessChainRecord(new(StatusCode.Found), recordMetadata, lap, ref output), $"lap ({lap}) == 0 != ProcessChainRecord(...)");
                Assert.AreEqual(numRecords, numberOfRecords, "mismatched record count");
                return stopAt != numRecords;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(ReadTestCategory)]
        public void IterateKeyTests([Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            // Upsert does not provide the preserveCopyUpdaterSource option, so we cannot check it in NoFlush; the record will always be elided.
            if (flushMode == FlushMode.NoFlush && updateOp == UpdateOp.Upsert)
                Assert.Ignore("Cannot test NoFlush with Upsert");

            using var testStore = new TestStore(useReadCache: false, ReadCopyOptions.None, flushMode != FlushMode.NoFlush, concurrencyControlMode);
            testStore.Populate(useRMW: updateOp == UpdateOp.RMW, useAsync: false, preserveCopyUpdaterSource: true).GetAwaiter().GetResult();

            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var key = new Key(defaultKeyToScan);
                IterateKeyTestScanIteratorFunctions scanFunctions = new(testStore);
                Assert.IsTrue(testStore.store.Log.IterateKeyVersions(ref scanFunctions, ref key));
                Assert.AreEqual(maxLap, scanFunctions.numRecords);
            }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(ReadTestCategory)]
        public void IterateKeyStopTests([Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            // Upsert does not provide the preserveCopyUpdaterSource option, so we cannot check it in NoFlush; the record will always be elided.
            if (flushMode == FlushMode.NoFlush && updateOp == UpdateOp.Upsert)
                Assert.Ignore("Cannot test NoFlush with Upsert");

            using var testStore = new TestStore(useReadCache: false, ReadCopyOptions.None, flushMode != FlushMode.NoFlush, concurrencyControlMode);
            testStore.Populate(updateOp == UpdateOp.RMW, useAsync: false, preserveCopyUpdaterSource: true).GetAwaiter().GetResult();

            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var key = new Key(defaultKeyToScan);
                IterateKeyTestScanIteratorFunctions scanFunctions = new(testStore) { stopAt = 4 };
                Assert.IsFalse(testStore.store.Log.IterateKeyVersions(ref scanFunctions, ref key));
                Assert.AreEqual(scanFunctions.stopAt, scanFunctions.numRecords);
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [Category("TsavoriteKV"), Category("Read")]
        public async Task VersionedReadAsyncTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk, concurrencyControlMode);
            await testStore.Populate(updateOp == UpdateOp.RMW, useAsync: true);
            using var session = testStore.store.NewSession<Value, Output, Empty, Functions>(new Functions());

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    // We need a non-AtAddress read to start the loop of returning the previous address to read at.
                    var readAsyncResult = readAtAddress == 0
                        ? await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1)
                        : await session.ReadAtAddressAsync(readAtAddress, ref key, ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [Category("TsavoriteKV"), Category("Read")]
        public void ReadAtAddressSyncTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk, concurrencyControlMode);
            testStore.Populate(updateOp == UpdateOp.RMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.store.NewSession<Value, Output, Empty, Functions>(new Functions());

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Output);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var status = readAtAddress == 0
                        ? session.Read(ref key, ref input, ref output, ref readOptions, out recordMetadata, serialNo: maxLap + 1)
                        : session.ReadAtAddress(readAtAddress, ref input, ref output, ref readOptions, out recordMetadata, serialNo: maxLap + 1);
                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [Category("TsavoriteKV"), Category("Read")]
        public async Task ReadAtAddressAsyncTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk, concurrencyControlMode);
            await testStore.Populate(updateOp == UpdateOp.RMW, useAsync: true);
            using var session = testStore.store.NewSession<Value, Output, Empty, Functions>(new Functions());

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = readAtAddress == 0
                        ? await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1)
                        : await session.ReadAtAddressAsync(readAtAddress, ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // Test is similar to others but tests the Overload where RadCopy*.None is set -- probably don't need all combinations of test but doesn't hurt
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [Category("TsavoriteKV"), Category("Read")]
        public async Task ReadAtAddressAsyncCopyOptNoRcTest(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk, concurrencyControlMode);
            await testStore.Populate(updateOp == UpdateOp.RMW, useAsync: true);
            using var session = testStore.store.NewSession<Value, Output, Empty, Functions>(new Functions());

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { CopyOptions = session.functions.readCopyOptions };
                long readAtAddress = 0;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = readAtAddress == 0
                        ? await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1)
                        : await session.ReadAtAddressAsync(readAtAddress, ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readAtAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [Category("TsavoriteKV"), Category("Read")]
        public void ReadNoKeySyncTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk, concurrencyControlMode);
            testStore.Populate(updateOp == UpdateOp.RMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.store.NewSession<Value, Output, Empty, Functions>(new Functions());

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var output = default(Output);
                var input = default(Value);

                for (int ii = 0; ii < numKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(numKeys);

                    ReadOptions readOptions = new()
                    {
                        CopyOptions = session.functions.readCopyOptions
                    };
                    var status = session.ReadAtAddress(testStore.InsertAddresses[keyOrdinal], ref input, ref output, ref readOptions, out RecordMetadata recordMetadata, serialNo: maxLap + 1);
                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }

                    TestStore.ProcessNoKeyRecord(updateOp == UpdateOp.RMW, status, recordMetadata.RecordInfo, ref output, keyOrdinal);
                }

                testStore.Flush().AsTask().GetAwaiter().GetResult();
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.NoFlush, ConcurrencyControlMode.None)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.NoReadCache, ReadCopyFrom.Device, ReadCopyTo.MainLog, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.LockTable)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.Upsert, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [TestCase(UseReadCache.ReadCache, ReadCopyFrom.None, ReadCopyTo.None, UpdateOp.RMW, FlushMode.OnDisk, ConcurrencyControlMode.RecordIsolation)]
        [Category("TsavoriteKV"), Category("Read")]
        public async Task ReadNoKeyAsyncTests(UseReadCache urc, ReadCopyFrom readCopyFrom, ReadCopyTo readCopyTo, UpdateOp updateOp, FlushMode flushMode, ConcurrencyControlMode concurrencyControlMode)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            var readCopyOptions = new ReadCopyOptions(readCopyFrom, readCopyTo);
            using var testStore = new TestStore(useReadCache, readCopyOptions, flushMode == FlushMode.OnDisk, concurrencyControlMode);
            await testStore.Populate(updateOp == UpdateOp.RMW, useAsync: true);
            using var session = testStore.store.NewSession<Value, Output, Empty, Functions>(new Functions());

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var input = default(Value);
                RecordMetadata recordMetadata = default;

                for (int ii = 0; ii < numKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(numKeys);

                    ReadOptions readOptions = new()
                    {
                        CopyOptions = session.functions.readCopyOptions
                    };

                    var readAsyncResult = await session.ReadAtAddressAsync(testStore.InsertAddresses[keyOrdinal], ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    TestStore.ProcessNoKeyRecord(updateOp == UpdateOp.RMW, status, recordMetadata.RecordInfo, ref output, keyOrdinal);
                }
            }

            await testStore.Flush();
        }

        internal struct ReadCopyOptionsMerge
        {
            internal ReadCopyOptions store, Session, Read, Expected;
        }

        [Test]
        [Category("TsavoriteKV"), Category("Read")]
        public void ReadCopyOptionssMergeTest()
        {
            ReadCopyOptionsMerge[] merges = new ReadCopyOptionsMerge[]
            {
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
            };

            for (var ii = 0; ii < merges.Length; ++ii)
            {
                var merge = merges[ii];
                var options = ReadCopyOptions.Merge(ReadCopyOptions.Merge(merge.store, merge.Session), merge.Read);
                Assert.AreEqual(merge.Expected, options, $"iter {ii}");
            }
        }
    }
}