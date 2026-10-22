// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public partial class V5ImportEncodingTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        void Start(int logs, bool recover)
        {
            GarnetServerOptions options = TestUtils.GetGarnetServerOptions(TestUtils.MethodTestDir,
                TestUtils.MethodTestDir, TestUtils.EndPoint, enableCluster: false, enableAOF: true,
                tryRecover: recover, sublogCount: logs, aofPageSize: "32m", aofMemorySize: "64m");
            server = new GarnetServer(options);
            server.Start();
        }

        static void Check(IDatabase db, string key, long tag, string value, long expiration)
        {
            RedisResult[] result = (RedisResult[])db.Execute("GETWITHETAG", key);
            Assert.That((long)result[0], Is.EqualTo(tag));
            Assert.That((string)result[1], Is.EqualTo(value));
            Assert.That((long)db.Execute("PEXPIRETIME", key), Is.EqualTo(expiration));
        }

        string SourceKey()
        {
            GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
            for (int index = 0; ; index++)
            {
                string source = "source" + index;
                if (log.GetPhysicalSublogIdx(Encoding.UTF8.GetBytes(source)) == log.GetPhysicalSublogIdx("destination"u8))
                    return source;
            }
        }

        unsafe void CheckExpirationTicks(long expected)
        {
            StoreWrapper wrapper = server.Provider.StoreWrapper;
            FunctionsState state = wrapper.CreateFunctionsState();
            using var session = wrapper.store.NewSession<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(new UnifiedSessionFunctions(state));
            var context = session.BasicContext;
            UnifiedInput input = new(RespCommand.RENAME);
            UnifiedOutput output = new();
            try
            {
                Status status = context.Read((FixedSpanByteKey)"destination"u8, ref input, ref output);
                Assert.That(status.Found, Is.True);
                fixed (byte* pointer = output.SpanByteAndMemory.ReadOnlySpan)
                {
                    LogRecord record = new(pointer, state.transientObjectIdMap);
                    Assert.That(record.Expiration, Is.EqualTo(expected));
                }
            }
            finally
            {
                output.Dispose();
            }
        }

        unsafe List<(int Sublog, byte[] Entry)> Encode(int logs, long tag, long expiration, string value,
            bool commit, bool enclosingRename, bool originalControl, string destination = "destination")
        {
            GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
            long[] begin = new long[logs];
            for (int index = 0; index < logs; index++)
                begin[index] = log.TailAddress[index];
            byte[] key = Encoding.UTF8.GetBytes(destination);
            byte[] source = Encoding.UTF8.GetBytes(SourceKey());
            int physical = log.GetPhysicalSublogIdx(key);
            int replay = log.GetReplayTaskIdx(key);
            BitVector[] vectors = new BitVector[logs];
            for (int index = 0; index < logs; index++)
                vectors[index] = new BitVector(AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes);
            _ = vectors[physical].SetBit(replay);
            const int session = 123456;
            long version = server.Provider.StoreWrapper.store.CurrentVersion + 1;
            LightEpoch epoch = new();
            epoch.Resume();
            try
            {
                if (!originalControl)
                    log.EnqueueTxn(AofEntryType.TxnStart, version, session, 1UL << physical, vectors, 1);
                if (!originalControl)
                    log.Enqueue(AofEntryType.StoreDelete, version, session, key, default, epoch, out _);
                byte[] valueBytes = Encoding.UTF8.GetBytes(value);
                byte[] tagBytes = Encoding.ASCII.GetBytes(tag.ToString(System.Globalization.CultureInfo.InvariantCulture));
                fixed (byte* valuePointer = valueBytes)
                fixed (byte* tagPointer = tagBytes)
                {
                    if (tag != 0 && !originalControl)
                    {
                        StringInput input = new(RespCommand.SETIFGREATER, arg1: expiration);
                        input.header.flags |= RespInputFlags.Deterministic;
                        input.parseState.InitializeWithArguments(PinnedSpanByte.FromPinnedPointer(valuePointer, valueBytes.Length),
                            PinnedSpanByte.FromPinnedPointer(tagPointer, tagBytes.Length));
                        log.Enqueue(AofEntryType.StoreRMW, version, session, key, ref input, epoch, out _);
                    }
                    else
                    {
                        StringInput input = new(RespCommand.SET, arg1: expiration);
                        input.header.flags |= RespInputFlags.Deterministic;
                        log.Enqueue(AofEntryType.StoreUpsert, version, session, key, valueBytes, ref input, epoch, out _);
                    }
                }
                // The enclosing rename contributes its source deletion without adding nested markers.
                if (enclosingRename && !originalControl)
                {
                    Assert.That(log.GetPhysicalSublogIdx(source), Is.EqualTo(physical), "fixture keys must share a sublog");
                    log.Enqueue(AofEntryType.StoreDelete, version, session, source, default, epoch, out _);
                }
                if (commit && !originalControl)
                    log.EnqueueTxn(AofEntryType.TxnCommit, version, session, 1UL << physical, vectors, 1);
            }
            finally
            {
                epoch.Suspend();
                epoch.Dispose();
            }
            log.Commit(spinWait: true);
            List<(int, byte[])> entries = new();
            for (int index = 0; index < logs; index++)
            {
                using TsavoriteLogScanIterator scan = log.Scan(index, begin[index], log.TailAddress[index]);
                while (scan.GetNext(out byte[] entry, out int length, out _))
                {
                    Assert.That(entry[0], Is.EqualTo(5));
                    entries.Add((index, entry.AsSpan(0, length).ToArray()));
                }
            }
            Assert.That(entries.Count, Is.GreaterThanOrEqualTo(originalControl ? 1 : commit ? 3 : 2));
            return entries;
        }

        [Test]
        public unsafe void ExistingV5ReaderAcceptsCandidate([Values(1, 2)] int logs,
            [Values(0L, 1L, 3L, long.MaxValue)] long tag, [Values] bool expiry,
            [Values(32, 2097152)] int length, [Values] bool replica, [Values] bool commit)
        {
            Start(logs, false);
            string value = new('v', length);
            long expiration = expiry ? DateTime.UtcNow.AddMinutes(5).Ticks : 0;
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                IDatabase db = redis.GetDatabase();
                db.Execute("SETIFGREATER", "destination", "old", long.MaxValue);
                db.KeyExpire("destination", TimeSpan.FromMinutes(10));
                long oldExpiration = (long)db.Execute("PEXPIRETIME", "destination");
                List<(int Sublog, byte[] Entry)> entries = Encode(logs, tag, expiration, value, commit, false, false);
                Assert.That(entries.Any(entry => (entry.Entry[1] & AofHeader.ChunkedRecordFlag) != 0),
                    Is.EqualTo(length > TsavoriteLog.MinPartialAllocSize));
                if (replica)
                {
                    AofProcessor processor = new(server.Provider.StoreWrapper);
                    try
                    {
                        foreach ((int sublog, byte[] entry) in entries)
                        {
                            if ((AofEntryType)entry[2] == AofEntryType.TxnCommit)
                                Check(db, "destination", long.MaxValue, "old", oldExpiration);
                            fixed (byte* pointer = entry)
                                processor.ProcessAofRecordInternal(sublog, pointer, entry.Length, true, out _);
                        }
                    }
                    finally
                    {
                        processor.Dispose();
                    }
                    Check(db, "destination", commit ? tag : long.MaxValue, commit ? value : "old",
                        commit ? (expiry ? (expiration - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMillisecond : -1) : oldExpiration);
                    if (commit)
                        CheckExpirationTicks(expiration);
                }
            }
            server.Dispose(false);
            server = null;
            Start(logs, true);
            using ConnectionMultiplexer recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase recoveredDb = recovered.GetDatabase();
            if (commit)
            {
                Check(recoveredDb, "destination", tag, value, expiry ? (expiration - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMillisecond : -1);
                CheckExpirationTicks(expiration);
            }
            else
            {
                RedisResult[] result = (RedisResult[])recoveredDb.Execute("GETWITHETAG", "destination");
                Assert.That((long)result[0], Is.EqualTo(long.MaxValue));
                Assert.That((string)result[1], Is.EqualTo("old"));
                Assert.That(recoveredDb.KeyTimeToLive("destination"), Is.Not.Null);
            }
        }

        [Test]
        public unsafe void ExistingRenameGroupingAndCandidateBody([Values(1, 2)] int logs,
            [Values] bool expiry, [Values(32, 2097152)] int length)
        {
            Start(logs, false);
            GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
            string source = SourceKey();
            string value = new('v', length);
            long expiration = expiry ? DateTime.UtcNow.AddMinutes(5).Ticks : 0;
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                IDatabase db = redis.GetDatabase();
                db.Execute("SETWITHETAG", source, value);
                long[] begin = Enumerable.Range(0, logs).Select(index => log.TailAddress[index]).ToArray();
                Assert.That(db.KeyRename(source, "destination"), Is.True);
                log.Commit(spinWait: true);
                List<AofHeader> headers = new();
                for (int index = 0; index < logs; index++)
                {
                    using TsavoriteLogScanIterator scan = log.Scan(index, begin[index], log.TailAddress[index]);
                    while (scan.GetNext(out byte[] entry, out _, out _))
                    {
                        fixed (byte* pointer = entry)
                            headers.Add(*(AofHeader*)pointer);
                    }
                }
                Assert.That(headers.Count(header => header.opType == AofEntryType.TxnStart), Is.EqualTo(1));
                Assert.That(headers.Count(header => header.opType == AofEntryType.TxnCommit), Is.EqualTo(1));
                Assert.That(headers.Any(header => header.opType == AofEntryType.StoreRMW), Is.True);
                Assert.That(headers.Select(header => header.sessionID).Distinct().Count(), Is.EqualTo(1));
                db.StringSet(source, "source-before-replay");
                db.Execute("SETIFGREATER", "destination", "old", long.MaxValue);
                List<(int Sublog, byte[] Entry)> entries = Encode(logs, 3, expiration, value, true, true, false);
                Assert.That(entries.Count(entry => (AofEntryType)entry.Entry[2] == AofEntryType.TxnStart), Is.EqualTo(1));
                Assert.That(entries.Count(entry => (AofEntryType)entry.Entry[2] == AofEntryType.TxnCommit), Is.EqualTo(1));
                AofProcessor processor = new(server.Provider.StoreWrapper);
                try
                {
                    foreach ((int sublog, byte[] entry) in entries)
                    {
                        fixed (byte* pointer = entry)
                            processor.ProcessAofRecordInternal(sublog, pointer, entry.Length, true, out _);
                    }
                }
                finally
                {
                    processor.Dispose();
                }
                Check(db, "destination", 3, value, expiry ? (expiration - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMillisecond : -1);
                Assert.That(db.KeyExists(source), Is.False);
            }
            server.Dispose(false);
            server = null;
            Start(logs, true);
            using ConnectionMultiplexer recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Check(recovered.GetDatabase(), "destination", 3, value, expiry ? (expiration - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMillisecond : -1);
            Assert.That(recovered.GetDatabase().KeyExists(source), Is.False);
        }

        [Test]
        public void OriginalPlainValueLogLosesImportedTag()
        {
            Start(1, false);
            _ = Encode(1, 3, 0, "value", true, false, true);
            server.Dispose(false);
            server = null;
            Start(1, true);
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Check(redis.GetDatabase(), "destination", 0, "value", -1);
        }
    }
}