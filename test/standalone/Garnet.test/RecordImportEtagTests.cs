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
                    UnifiedInput input = new(RespCommand.SET, RespInputFlags.HasEtag, record.Expiration)
                    {
                        arg2 = record.ETag
                    };
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
        public unsafe void InputsConditionallySerializeCompatibleEtag([Values(0L, 3L, long.MaxValue)] long etag,
            [Values] bool unified)
        {
            byte[] argument = Encoding.ASCII.GetBytes("value");
            fixed (byte* argumentPointer = argument)
            {
                SessionParseState parseState = default;
                ReadOnlySpan<PinnedSpanByte> args = [PinnedSpanByte.FromPinnedPointer(argumentPointer, argument.Length)];
                parseState.InitializeWithArguments(args);
                var input = new StringInput(RespCommand.SET, ref parseState, arg1: 123,
                    flags: RespInputFlags.HasEtag)
                { arg2 = etag };
                var unifiedInput = new UnifiedInput(RespCommand.SET, ref parseState, arg1: 123,
                    flags: RespInputFlags.HasEtag)
                { arg2 = etag };
                int taggedLength = input.SerializedLength;
                Assert.That(unifiedInput.SerializedLength, Is.EqualTo(taggedLength));
                byte[] buffer = new byte[taggedLength];
                fixed (byte* pointer = buffer)
                {
                    Assert.That(unified ? unifiedInput.CopyTo(pointer, buffer.Length) : input.CopyTo(pointer, buffer.Length),
                        Is.EqualTo(taggedLength));
                    StringInput decoded = default;
                    UnifiedInput unifiedDecoded = default;
                    Assert.That(decoded.DeserializeFrom(pointer), Is.EqualTo(taggedLength));
                    Assert.That(unifiedDecoded.DeserializeFrom(pointer), Is.EqualTo(taggedLength));
                    Assert.That(unifiedDecoded.HasEtag, Is.True);
                    Assert.That(unifiedDecoded.arg2, Is.EqualTo(etag));
                    Assert.That(unifiedDecoded.arg1, Is.EqualTo(123));
                    Assert.That(unifiedDecoded.parseState.GetArgSliceByRef(0).ToString(), Is.EqualTo("value"));
                    Assert.That(decoded.HasEtag, Is.True);
                    Assert.That(decoded.arg2, Is.EqualTo(etag));
                    Assert.That(decoded.arg1, Is.EqualTo(123));
                    Assert.That(decoded.parseState.GetArgSliceByRef(0).ToString(), Is.EqualTo("value"));

                    input.header.flags = 0;
                    unifiedInput.header.flags = 0;
                    Assert.That(input.SerializedLength, Is.EqualTo(taggedLength - sizeof(long)));
                    Assert.That(unifiedInput.SerializedLength, Is.EqualTo(input.SerializedLength));
                    Assert.That(unified ? unifiedInput.CopyTo(pointer, buffer.Length) : input.CopyTo(pointer, buffer.Length),
                        Is.EqualTo(input.SerializedLength));
                    Assert.That(decoded.DeserializeFrom(pointer), Is.EqualTo(input.SerializedLength));
                    Assert.That(unifiedDecoded.DeserializeFrom(pointer), Is.EqualTo(input.SerializedLength));
                    Assert.That(unifiedDecoded.HasEtag, Is.False);
                    Assert.That(unifiedDecoded.arg2, Is.Zero);
                    Assert.That(unifiedDecoded.arg1, Is.EqualTo(123));
                    Assert.That(unifiedDecoded.parseState.GetArgSliceByRef(0).ToString(), Is.EqualTo("value"));
                    Assert.That(decoded.HasEtag, Is.False);
                    Assert.That(decoded.arg2, Is.Zero);
                    Assert.That(decoded.arg1, Is.EqualTo(123));
                    Assert.That(decoded.parseState.GetArgSliceByRef(0).ToString(), Is.EqualTo("value"));
                }
            }
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

        // Returns the AOF entries logged for record upserts
        unsafe List<byte[]> ReadImportedEntries(int sublogCount, bool rename = false)
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
                    var header = ReadHeader(entry);
                    Assert.That(header.opType, Is.Not.EqualTo(AofEntryType.UnifiedStoreStringUpsert));
                    if (!rename)
                    {
                        Assert.That(header.opType, Is.Not.EqualTo(AofEntryType.TxnStart));
                        Assert.That(header.opType, Is.Not.EqualTo(AofEntryType.TxnCommit));
                    }
                    if (header.opType != AofEntryType.StoreUpsert)
                        continue;
                    fixed (byte* pointer = entry)
                    {
                        StringInput input = default;
                        if (header.IsChunked)
                        {
                            chunks.Add(entry.AsSpan(0, length).ToArray());
                            if (!reader.ReadChunk(pointer, length, out var completed))
                                continue;
                            try
                            {
                                fixed (byte* inputPointer = completed.input)
                                    input.DeserializeFrom(inputPointer);
                                if (input.header.cmd == RespCommand.SET && input.HasEtag)
                                {
                                    Assert.That(input.parseState.Count, Is.Zero);
                                    entries.AddRange(chunks);
                                }
                            }
                            finally
                            {
                                completed.ReturnValueChunks();
                            }
                            chunks.Clear();
                        }
                        else
                        {
                            byte* inputPointer = AofHeader.SkipHeader(pointer);
                            inputPointer += PinnedSpanByte.FromLengthPrefixedPinnedPointer(inputPointer).TotalSize;
                            inputPointer += PinnedSpanByte.FromLengthPrefixedPinnedPointer(inputPointer).TotalSize;
                            input.DeserializeFrom(inputPointer);
                            if (input.header.cmd == RespCommand.SET && input.HasEtag)
                            {
                                Assert.That(input.parseState.Count, Is.Zero);
                                entries.Add(entry.AsSpan(0, length).ToArray());
                            }
                        }
                    }
                }
                reader.DiscardInProgressAccumulations();
            }
            return entries;
        }
        [Test]
        public void ImportReplacesDestinationOnRecovery([Values(1, 2)] int sublogCount,
            [Values] bool tagged, [Values] bool objectDestination, [Values(32, 8192)] int valueLength)
        {
            server.Dispose();
            server = CreateAofServer(sublogCount, recover: false);
            server.Start();
            string value = new('v', valueLength);
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                IDatabase database = redis.GetDatabase();
                if (tagged)
                    database.Execute("SETIFGREATER", "source", value, 3);
                else
                    database.StringSet("source", value);
                if (objectDestination)
                    database.HashSet("destination", "field", "old");
                else
                    database.Execute("SETIFGREATER", "destination", new string('x', 128), long.MaxValue);
                database.KeyExpire("destination", TimeSpan.FromMinutes(10));
                ImportRecord("source", "destination");
                database.Execute("COMMITAOF");
            }

            Assert.That(ReadImportedEntries(sublogCount), Has.Count.EqualTo(1));

            server.Dispose(false);
            server = CreateAofServer(sublogCount, recover: true);
            server.Start();
            using ConnectionMultiplexer recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase recoveredDatabase = recovered.GetDatabase();
            AssertTaggedValue(recoveredDatabase, "destination", tagged ? 3 : 0, value);
            Assert.That(recoveredDatabase.KeyTimeToLive("destination"), Is.Null);
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
                {
                    database.Execute("SETIFGREATER", "destination", new string('x', valueLength * 2), long.MaxValue);
                    database.KeyExpire("destination", TimeSpan.FromMinutes(10));
                }
                if (rename)
                    Assert.That(database.KeyRename("source", "destination"), Is.True);
                else
                    ImportRecord("source", "destination");
                database.KeyDelete("source");
                database.Execute("COMMITAOF");
            }

            List<byte[]> importedEntries = ReadImportedEntries(sublogCount, rename);
            Assert.That(importedEntries, Is.Not.Empty);
            if (valueLength <= TsavoriteLog.MinPartialAllocSize)
                Assert.That(importedEntries, Has.Count.EqualTo(1));
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