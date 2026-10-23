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
    public partial class RecordImportEtagTests : TestBase
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

        unsafe void ImportRecord(string source, string destination)
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

        [Test]
        public void LargerImportPreservesTagAndExpiry([Values] bool tagged, [Values] bool expiry)
        {
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase database = redis.GetDatabase();
            string value = new('v', 8192);
            if (tagged)
                database.Execute("SETIFGREATER", "source", value, 3);
            else
                database.StringSet("source", value);
            if (expiry)
                database.KeyExpire("source", TimeSpan.FromMinutes(5));
            long expiration = (long)database.Execute("PEXPIRETIME", "source");
            database.Execute("SETIFGREATER", "destination", "old", long.MaxValue);
            database.KeyExpire("destination", TimeSpan.FromMinutes(10));

            ImportRecord("source", "destination");

            AssertTaggedValue(database, "destination", tagged ? 3 : 0, value);
            Assert.That((long)database.Execute("PEXPIRETIME", "destination"), Is.EqualTo(expiration));
        }

        GarnetServer CreateAofServer(int sublogCount, bool recover)
        {
            GarnetServerOptions options = TestUtils.GetGarnetServerOptions(TestUtils.MethodTestDir,
                TestUtils.MethodTestDir, TestUtils.EndPoint, enableCluster: false, enableAOF: true,
                tryRecover: recover, sublogCount: sublogCount, aofPageSize: "32m", aofMemorySize: "64m");
            options.FailOnRecoveryError = true;
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
                AofChunkedRecordReader reader = new();
                List<byte[]> chunks = new();
                using TsavoriteLogScanIterator scan = log.Scan(sublog, log.BeginAddress[sublog], log.TailAddress[sublog]);
                while (scan.GetNext(out byte[] entry, out int length, out _))
                {
                    AofHeader header = ReadHeader(entry);
                    if (header.opType != AofEntryType.StoreRMW)
                        continue;
                    unsafe
                    {
                        fixed (byte* pointer = entry)
                        {
                            if (header.IsChunked)
                            {
                                chunks.Add(entry.AsSpan(0, length).ToArray());
                                if (!reader.ReadChunk(pointer, length, out ChunkedAccumulator completed))
                                    continue;
                                fixed (byte* inputPointer = completed.input)
                                {
                                    StringInput input = default;
                                    input.DeserializeFrom(inputPointer);
                                    if (input.header.cmd == RespCommand.SETIFGREATER)
                                        entries.AddRange(chunks);
                                }
                                chunks.Clear();
                                completed.ReturnValueChunks();
                                continue;
                            }
                            byte* inputPtr = AofHeader.SkipHeader(pointer);
                            inputPtr += PinnedSpanByte.FromLengthPrefixedPinnedPointer(inputPtr).TotalSize;
                            StringInput ordinaryInput = default;
                            ordinaryInput.DeserializeFrom(inputPtr);
                            if (ordinaryInput.header.cmd != RespCommand.SETIFGREATER)
                                continue;
                        }
                    }
                    entries.Add(entry.AsSpan(0, length).ToArray());
                }
                reader.DiscardInProgressAccumulations();
            }
            return entries;
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