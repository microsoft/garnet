// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class RecordImportEtagTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        unsafe void ImportRecord(string source, string destination, bool renameInput = false)
        {
            StoreWrapper storeWrapper = server.Provider.StoreWrapper;
            FunctionsState state = storeWrapper.CreateFunctionsState();
            using var session = storeWrapper.store.NewSession<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(new UnifiedSessionFunctions(state));
            var context = session.BasicContext;
            UnifiedInput readInput = new(RespCommand.RENAME);
            UnifiedOutput output = new();
            try
            {
                Status readStatus = context.Read((FixedSpanByteKey)Encoding.UTF8.GetBytes(source), ref readInput, ref output);
                Assert.That(readStatus.Found, Is.True);
                fixed (byte* recordPtr = output.SpanByteAndMemory.ReadOnlySpan)
                {
                    LogRecord record = new(recordPtr, state.transientObjectIdMap);
                    UnifiedInput input = new();
                    if (renameInput)
                    {
                        input.header.cmd = RespCommand.RENAME;
                        input.parseState.InitializeWithArgument(PinnedSpanByte.FromPinnedPointer(recordPtr, 0));
                    }
                    Status status = context.Upsert((FixedSpanByteKey)Encoding.UTF8.GetBytes(destination), ref input, in record);
                    Assert.That(status.IsCompletedSuccessfully, Is.True);
                }
            }
            finally
            {
                output.Dispose();
            }
        }

        static void AssertTaggedValue(IDatabase database, string key, long tag, string value)
        {
            RedisResult[] result = (RedisResult[])database.Execute("GETWITHETAG", key);
            Assert.That((long)result[0], Is.EqualTo(tag));
            Assert.That((string)result[1], Is.EqualTo(value));
        }

        [Test]
        public void ImportPreservesTagAndExpiry([Values] bool replace, [Values] bool expiry, [Values(32, 8192)] int valueLength)
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            string value = new('v', valueLength);
            database.Execute("SETWITHETAG", "source", "first");
            database.Execute("SETWITHETAG", "source", "second");
            if (expiry)
                database.Execute("SETWITHETAG", "source", value, "EX", 300);
            else
                database.Execute("SETWITHETAG", "source", value);
            long expiration = (long)database.Execute("PEXPIRETIME", "source");
            if (replace)
                database.StringSet("destination", new string('x', valueLength * 2), TimeSpan.FromMinutes(10));

            ImportRecord("source", "destination");

            AssertTaggedValue(database, "destination", 3, value);
            Assert.That(database.KeyTimeToLive("destination").HasValue, Is.EqualTo(expiry));
            Assert.That((long)database.Execute("PEXPIRETIME", "destination"), Is.EqualTo(expiration));
            RedisResult[] stale = (RedisResult[])database.Execute("SETIFMATCH", "destination", "stale", 2);
            Assert.That((long)stale[0], Is.EqualTo(3));
            Assert.That((string)stale[1], Is.EqualTo(value));
            RedisResult[] matching = (RedisResult[])database.Execute("SETIFMATCH", "destination", "updated", 3);
            Assert.That((long)matching[0], Is.EqualTo(4));
            Assert.That(matching[1].IsNull, Is.True);
            AssertTaggedValue(database, "destination", 4, "updated");
            database.StringSet("destination", "plain");
            AssertTaggedValue(database, "destination", 4, "plain");
        }

        [Test]
        public void RenamePreservesTagAndExpiry([Values] bool replace, [Values] bool expiry)
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            database.Execute("SETWITHETAG", "source", "first");
            database.Execute("SETWITHETAG", "source", "value");
            if (expiry)
                database.KeyExpire("source", TimeSpan.FromMinutes(5));
            if (replace)
                database.StringSet("destination", new string('x', 128));

            Assert.That(database.KeyRename("source", "destination"), Is.True);
            AssertTaggedValue(database, "destination", 2, "value");
            Assert.That(database.KeyExists("source"), Is.False);
            Assert.That(database.KeyTimeToLive("destination").HasValue, Is.EqualTo(expiry));
        }

        [Test]
        public void UntaggedImportClearsDestinationTagAndExpiry()
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            database.StringSet("source", "plain");
            database.Execute("SETWITHETAG", "destination", new string('x', 128), "EX", 300);

            ImportRecord("source", "destination");

            AssertTaggedValue(database, "destination", 0, "plain");
            Assert.That(database.KeyTimeToLive("destination"), Is.Null);
        }

        GarnetServer CreateAofServer(int sublogCount, bool recover)
        {
            GarnetServerOptions options = TestUtils.GetGarnetServerOptions(TestUtils.MethodTestDir,
                TestUtils.MethodTestDir, TestUtils.EndPoint, enableCluster: false, enableAOF: true,
                tryRecover: recover, sublogCount: sublogCount, aofPageSize: "32m", aofMemorySize: "64m");
            return new GarnetServer(options);
        }

        static unsafe AofHeader ReadHeader(byte[] entry)
        {
            fixed (byte* pointer = entry)
                return *(AofHeader*)pointer;
        }

        List<byte[]> ReadImportedEntries(int sublogCount)
        {
            GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
            List<byte[]> entries = new();
            for (int sublog = 0; sublog < sublogCount; sublog++)
            {
                using TsavoriteLogScanIterator scan = log.Scan(sublog, log.BeginAddress[sublog], log.TailAddress[sublog]);
                while (scan.GetNext(out byte[] entry, out int length, out _))
                {
                    if (ReadHeader(entry).opType == AofEntryType.UnifiedStoreStringRecordUpsert)
                        entries.Add(entry.AsSpan(0, length).ToArray());
                }
            }
            return entries;
        }

        static unsafe void ReplayEntry(AofProcessor processor, byte[] entry, int length)
        {
            fixed (byte* pointer = entry)
                processor.ProcessAofRecordInternal(0, pointer, length, asReplica: false, out _);
        }

        static unsafe void ReplayReplicaEntry(AofProcessor processor, byte[] entry)
        {
            fixed (byte* pointer = entry)
                processor.ProcessAofRecordInternal(0, pointer, entry.Length, asReplica: true, out _);
        }

        static unsafe byte[] TransactionMarker(AofHeader header, AofEntryType operation, long sequenceNumber)
        {
            header.opType = operation;
            header.HeaderType = AofHeaderType.ShardedLogTransactionHeader;
            AofShardedLogTransactionHeader marker = new()
            {
                shardedHeader = new AofShardedHeader { basicHeader = header, sequenceNumber = sequenceNumber },
                participantCount = 1,
            };
            marker.replayTaskAccessVector[0] = 1;
            return new ReadOnlySpan<byte>(&marker, AofShardedLogTransactionHeader.TotalSize).ToArray();
        }

        [Test]
        public unsafe void ReplicaTransactionRejectsMalformedRecordBeforeApplying([Values] bool chunked,
            [Values("header", "count", "argument")] string corruption, [Values(-1, int.MaxValue)] int invalidLength)
        {
            server.Dispose();
            server = CreateAofServer(2, recover: false);
            server.Start();
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            database.Execute("SETWITHETAG", "source", new string('v', chunked ? 2 * TsavoriteLog.MinPartialAllocSize : 32));
            ImportRecord("source", "destination", renameInput: true);
            database.Execute("COMMITAOF");
            List<byte[]> entries = ReadImportedEntries(2);
            Assert.That(entries, Is.Not.Empty);
            Assert.That(ReadHeader(entries[0]).IsChunked, Is.EqualTo(chunked));
            List<byte[]> malformedEntries = entries.ConvertAll(entry => (byte[])entry.Clone());
            byte[] finalEntry = malformedEntries[^1];
            int headerSize = default(UnifiedInput).header.SpanByte.TotalSize;
            int inputLength = default(UnifiedInput).SerializedLength + sizeof(int) + 2 * sizeof(long);
            int prefixOffset = corruption switch
            {
                "header" => 0,
                "count" => headerSize + sizeof(long),
                _ => headerSize + sizeof(long) + sizeof(int),
            };
            fixed (byte* pointer = finalEntry)
                *(int*)(pointer + finalEntry.Length - inputLength + prefixOffset) = invalidLength;

            database.KeyDelete("destination");
            database.StringSet("destination", "unchanged");
            AofProcessor processor = new(server.Provider.StoreWrapper);
            try
            {
                AofHeader header = ReadHeader(entries[0]);
                ReplayReplicaEntry(processor, TransactionMarker(header, AofEntryType.TxnStart, 100));
                foreach (byte[] entry in entries)
                    ReplayReplicaEntry(processor, entry);
                foreach (byte[] entry in malformedEntries)
                    ReplayReplicaEntry(processor, entry);
                byte[] commit = TransactionMarker(header, AofEntryType.TxnCommit, 200);
                GarnetException exception = Assert.Throws<GarnetException>(() => ReplayReplicaEntry(processor, commit));
                Assert.That(exception.StackTrace, Does.Contain("RequiresOrderedReplay"));
            }
            finally
            {
                processor.Dispose();
            }
            AssertTaggedValue(database, "destination", 0, "unchanged");
        }

        [Test]
        public unsafe void RecordUpsertRejectsMalformedInputBeforeApplying([Values] bool chunked,
            [Values("truncated", "header", "count")] string corruption)
        {
            server.Dispose();
            server = CreateAofServer(1, recover: false);
            server.Start();
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            database.Execute("SETWITHETAG", "source", new string('v', chunked ? 2 * TsavoriteLog.MinPartialAllocSize : 32));
            ImportRecord("source", "destination");
            database.Execute("COMMITAOF");
            List<byte[]> entries = ReadImportedEntries(1);
            Assert.That(entries, Is.Not.Empty);
            byte[] finalEntry = entries[^1];
            if (corruption != "truncated")
            {
                int inputLength = default(UnifiedInput).SerializedLength + 2 * sizeof(long);
                int prefixOffset = corruption == "header" ? 0 : default(UnifiedInput).header.SpanByte.TotalSize + sizeof(long);
                fixed (byte* pointer = finalEntry)
                    *(int*)(pointer + finalEntry.Length - inputLength + prefixOffset) = int.MaxValue;
            }
            database.KeyDelete("destination");
            database.StringSet("destination", "unchanged");
            AofProcessor processor = new(server.Provider.StoreWrapper);
            try
            {
                if (chunked)
                {
                    for (int index = 0; index < entries.Count - 1; index++)
                        ReplayEntry(processor, entries[index], entries[index].Length);
                    int finalLength = corruption == "truncated" ? finalEntry.Length - 1 : finalEntry.Length;
                    Assert.Throws<GarnetException>(() => ReplayEntry(processor, finalEntry, finalLength));
                    AssertTaggedValue(database, "destination", 0, "unchanged");

                    AofChunkedRecordReader reader = new();
                    ChunkedAccumulator completed = null;
                    foreach (byte[] entry in entries)
                    {
                        Assert.That(ReadHeader(entry).IsChunked, Is.True);
                        fixed (byte* pointer = entry)
                        {
                            if (reader.ReadChunk(pointer, entry.Length, out ChunkedAccumulator accumulator))
                                completed = accumulator;
                        }
                    }
                    Assert.That(completed, Is.Not.Null);
                    if (corruption == "truncated")
                        completed.inputOffset--;
                    Assert.Throws<GarnetException>(() => processor.ProcessAofRecordInternal(0, completed, asReplica: false));
                    reader.DiscardInProgressAccumulations();
                }
                else
                {
                    Assert.That(entries.Count, Is.EqualTo(1));
                    Assert.That(ReadHeader(entries[0]).IsChunked, Is.False);
                    int entryLength = corruption == "truncated" ? entries[0].Length - 1 : entries[0].Length;
                    Assert.Throws<GarnetException>(() => ReplayEntry(processor, entries[0], entryLength));
                }
                AssertTaggedValue(database, "destination", 0, "unchanged");
            }
            finally
            {
                processor.Dispose();
            }
        }

        [Test]
        public void ImportedTagSurvivesRecovery([Values(1, 2)] int sublogCount, [Values] bool replace,
            [Values] bool expiry, [Values(32, 2097152)] int valueLength, [Values] bool rename)
        {
            server.Dispose();
            server = CreateAofServer(sublogCount, recover: false);
            server.Start();
            string value = new('v', valueLength);
            long expiration;
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                IDatabase database = redis.GetDatabase();
                database.Execute("SETWITHETAG", "source", "first");
                database.Execute("SETWITHETAG", "source", "second");
                database.Execute("SETWITHETAG", expiry ? ["source", value, "EX", 300] : ["source", value]);
                expiration = (long)database.Execute("PEXPIRETIME", "source");
                if (replace)
                    database.Execute("SETWITHETAG", "destination", new string('x', valueLength * 2), "EX", 600);
                if (rename)
                    Assert.That(database.KeyRename("source", "destination"), Is.True);
                else
                    ImportRecord("source", "destination");
                database.KeyDelete("source");
                database.Execute("COMMITAOF");
            }

            List<byte[]> importedEntries = ReadImportedEntries(sublogCount);
            Assert.That(importedEntries, Is.Not.Empty);
            foreach (byte[] entry in importedEntries)
                Assert.That(ReadHeader(entry).IsChunked, Is.EqualTo(valueLength > TsavoriteLog.MinPartialAllocSize));

            server.Dispose(false);
            server = CreateAofServer(sublogCount, recover: true);
            server.Start();
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                IDatabase database = redis.GetDatabase();
                AssertTaggedValue(database, "destination", 3, value);
                Assert.That((long)database.Execute("PEXPIRETIME", "destination"), Is.EqualTo(expiration));
                RedisResult[] stale = (RedisResult[])database.Execute("SETIFMATCH", "destination", "stale", 2);
                Assert.That((long)stale[0], Is.EqualTo(3));
                Assert.That((string)stale[1], Is.EqualTo(value));
                RedisResult[] matching = (RedisResult[])database.Execute("SETIFMATCH", "destination", "updated", 3);
                Assert.That((long)matching[0], Is.EqualTo(4));
                Assert.That(matching[1].IsNull, Is.True);
                database.Execute("COMMITAOF");
            }

            server.Dispose(false);
            server = CreateAofServer(sublogCount, recover: true);
            server.Start();
            using ConnectionMultiplexer recoveredRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            AssertTaggedValue(recoveredRedis.GetDatabase(), "destination", 4, "updated");
        }
    }
}