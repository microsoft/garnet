// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    public partial class RecordImportEtagTests
    {
        List<(int Sublog, byte[] Entry)> Capture(long[] begin)
        {
            GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
            log.Commit(spinWait: true);
            List<(int, byte[])> entries = new();
            for (int index = 0; index < begin.Length; index++)
            {
                using TsavoriteLogScanIterator scan = log.Scan(index, begin[index], log.TailAddress[index]);
                while (scan.GetNext(out byte[] entry, out int length, out _))
                {
                    Assert.That(entry[0], Is.EqualTo(5));
                    Assert.That(Enum.IsDefined(typeof(AofEntryType), ReadHeader(entry).opType), Is.True);
                    entries.Add((index, entry.AsSpan(0, length).ToArray()));
                }
            }
            return entries;
        }

        void InitializeOldDestination(IDatabase db, bool oldObject, string destination = "destination")
        {
            db.KeyDelete(destination);
            if (oldObject)
                db.HashSet(destination, "field", "old");
            else
                db.Execute("SETIFGREATER", destination, "old", long.MaxValue);
            db.KeyExpire(destination, TimeSpan.FromMinutes(10));
        }

        [Test]
        public unsafe void ActualImportLogPreservesMetadataOnReplay([Values(1, 2)] int logs, [Values(32, 2097152)] int length,
            [Values] bool rename, [Values] bool expiry, [Values] bool oldObject, [Values] bool tagged, [Values] bool commit)
        {
            server.Dispose();
            server = CreateAofServer(logs, false);
            server.Start();
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase();
            GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
            string destination = Enumerable.Range(0, 100).Select(index => "destination" + index)
                .First(candidate => log.GetPhysicalSublogIdx(Encoding.UTF8.GetBytes(candidate)) == logs - 1);
            string source = Enumerable.Range(0, 100).Select(index => "source" + index)
                .First(candidate => log.GetPhysicalSublogIdx(Encoding.UTF8.GetBytes(candidate)) == logs - 1);
            string value = new('v', length);
            long importedTag = tagged ? 3 : 0;
            if (tagged)
                db.Execute("SETIFGREATER", source, value, importedTag);
            else
                db.StringSet(source, value);
            if (expiry)
                db.KeyExpire(source, TimeSpan.FromMinutes(5));
            long expiration = (long)db.Execute("PEXPIRETIME", source);
            InitializeOldDestination(db, oldObject, destination);
            if (length > TsavoriteLog.MinPartialAllocSize)
                db.KeyDelete(destination);
            long[] begin = Enumerable.Range(0, logs).Select(index => log.TailAddress[index]).ToArray();
            if (rename)
                Assert.That(db.KeyRename(source, destination), Is.True);
            else
                ImportRecord(source, destination);
            List<(int Sublog, byte[] Entry)> entries = Capture(begin);
            Assert.That(entries.All(entry => entry.Sublog == logs - 1), Is.True);
            Assert.That(entries.Count(entry => ReadHeader(entry.Entry).opType == AofEntryType.TxnStart), Is.EqualTo(1));
            Assert.That(entries.Count(entry => ReadHeader(entry.Entry).opType == AofEntryType.TxnCommit), Is.EqualTo(1));
            Assert.That(entries.Select(entry => ReadHeader(entry.Entry).sessionID).Distinct().Count(), Is.EqualTo(1));
            Assert.That(entries.Any(entry => ReadHeader(entry.Entry).IsChunked), Is.EqualTo(length > TsavoriteLog.MinPartialAllocSize));
            AssertTaggedValue(db, destination, importedTag, value);
            InitializeOldDestination(db, oldObject, destination);
            long oldExpiration = (long)db.Execute("PEXPIRETIME", destination);
            if (!commit)
                entries.RemoveAll(entry => ReadHeader(entry.Entry).opType == AofEntryType.TxnCommit);
            AofProcessor processor = new(server.Provider.StoreWrapper);
            Task replay = null;
            try
            {
                replay = Task.Run(() =>
                {
                    foreach ((int sublog, byte[] entry) in entries)
                    {
                        fixed (byte* pointer = entry)
                            processor.ProcessAofRecordInternal(sublog, pointer, entry.Length, asReplica: true, out _);
                    }
                });
                for (int index = 0; index < 64; index++)
                {
                    RedisResult raw;
                    try
                    {
                        raw = db.Execute("GETWITHETAG", destination);
                    }
                    catch (RedisServerException exception) when (oldObject && exception.Message.StartsWith("WRONGTYPE", StringComparison.Ordinal))
                    {
                        continue;
                    }
                    if (raw.IsNull)
                        continue;
                    RedisResult[] result = (RedisResult[])raw;
                    long tag = (long)result[0];
                    Assert.That(tag == long.MaxValue || tag == importedTag, Is.True);
                    Assert.That((string)result[1], Is.EqualTo(tag == importedTag ? value : "old"));
                }
                replay.GetAwaiter().GetResult();
                if (commit)
                    AssertTaggedValue(db, destination, importedTag, value);
                else if (oldObject)
                    Assert.That((string)db.HashGet(destination, "field"), Is.EqualTo("old"));
                else
                    AssertTaggedValue(db, destination, long.MaxValue, "old");
                Assert.That((long)db.Execute("PEXPIRETIME", destination), Is.EqualTo(commit ? expiration : oldExpiration));
            }
            finally
            {
                replay?.GetAwaiter().GetResult();
                processor.Dispose();
            }
        }

        [Test]
        public unsafe void UserDeleteSetIfGreaterTransactionKeepsItsOutcome([Values(1, 2)] int logs, [Values] bool oldObject)
        {
            server.Dispose();
            server = CreateAofServer(logs, false);
            server.Start();
            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase();
            InitializeOldDestination(db, oldObject);
            GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
            long[] begin = Enumerable.Range(0, logs).Select(index => log.TailAddress[index]).ToArray();
            ITransaction transaction = db.CreateTransaction();
            Task<bool> delete = transaction.KeyDeleteAsync("destination");
            Task<RedisResult> set = transaction.ExecuteAsync("SETIFGREATER", "destination", "new", 3);
            Assert.That(transaction.Execute(), Is.True);
            Assert.That(delete.Result, Is.True);
            Assert.That((long)((RedisResult[])set.Result)[0], Is.EqualTo(3));
            AssertTaggedValue(db, "destination", 3, "new");
            List<(int Sublog, byte[] Entry)> entries = Capture(begin);
            InitializeOldDestination(db, oldObject);
            AofProcessor processor = new(server.Provider.StoreWrapper);
            try
            {
                foreach ((int sublog, byte[] entry) in entries)
                {
                    fixed (byte* pointer = entry)
                        processor.ProcessAofRecordInternal(sublog, pointer, entry.Length, true, out _);
                }
                AssertTaggedValue(db, "destination", 3, "new");
                Assert.That(db.KeyTimeToLive("destination"), Is.Null);
                Assert.That(db.KeyType("destination"), Is.EqualTo(RedisType.String));
            }
            finally
            {
                processor.Dispose();
            }
        }

        [Test]
        public void CheckpointRecoveryContinuesAfterTruncatedImportTransaction([Values(1, 2)] int logs, [Values] bool abort)
        {
            server.Dispose();
            server = CreateAofServer(logs, false);
            server.Start();
            ConfigurationOptions config = TestUtils.GetConfig();
            config.AllowAdmin = true;
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(config))
            {
                IDatabase db = redis.GetDatabase();
                db.Execute("SETIFGREATER", "source", "imported", 3);
                GarnetLog log = server.Provider.StoreWrapper.appendOnlyFile.Log;
                long[] begin = Enumerable.Range(0, logs).Select(index => log.TailAddress[index]).ToArray();
                ImportRecord("source", "destination");
                AofHeader importedCommit = ReadHeader(Capture(begin).Single(entry => ReadHeader(entry.Entry).opType == AofEntryType.TxnCommit).Entry);
                db.Execute("SAVE");

                int physical = log.GetPhysicalSublogIdx("destination"u8);
                BitVector[] vectors = new BitVector[log.Size];
                for (int index = 0; index < vectors.Length; index++)
                    vectors[index] = new BitVector(AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes);
                _ = vectors[physical].SetBit(log.GetReplayTaskIdx("destination"u8));
                // A checkpoint can truncate the start of an older imported transaction.
                log.EnqueueTxn(abort ? AofEntryType.TxnAbort : AofEntryType.TxnCommit, importedCommit.storeVersion,
                    importedCommit.sessionID, 1UL << physical, vectors, 1);
                db.Execute("SETWITHETAG", "destination", "after-checkpoint");
                AssertTaggedValue(db, "destination", 4, "after-checkpoint");
                db.Execute("COMMITAOF");
            }
            server.Dispose(false);
            server = CreateAofServer(logs, true);
            server.Start();
            using ConnectionMultiplexer recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            AssertTaggedValue(recovered.GetDatabase(), "destination", 4, "after-checkpoint");
        }

        [Test]
        public void ConcurrentImportWriterCheckpointRecovery([Values(1, 2)] int logs)
        {
            const int length = 32;
            server.Dispose();
            server = CreateAofServer(logs, false);
            server.Start();
            long expectedTag;
            string expectedValue;
            long expectedExpiration;
            ConfigurationOptions config = TestUtils.GetConfig();
            config.AllowAdmin = true;
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(config))
            {
                IDatabase db = redis.GetDatabase();
                db.Execute("SETIFGREATER", "source", new string('v', length), 3);
                db.Execute("SETIFGREATER", "destination", "old", 1000);
                Task writer = Task.Run(() =>
                {
                    for (int index = 0; index < 32; index++)
                        db.Execute("SETWITHETAG", "destination", "writer" + index);
                });
                Task checkpoint = Task.Run(() => db.Execute("SAVE"));
                for (int index = 0; index < 32; index++)
                    ImportRecord("source", "destination");
                Task.WaitAll(writer, checkpoint);
                RedisResult[] expected = (RedisResult[])db.Execute("GETWITHETAG", "destination");
                expectedTag = (long)expected[0];
                expectedValue = (string)expected[1];
                expectedExpiration = (long)db.Execute("PEXPIRETIME", "destination");
                db.Execute("COMMITAOF");
            }
            server.Dispose(false);
            server = CreateAofServer(logs, true);
            server.Start();
            using ConnectionMultiplexer recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            AssertTaggedValue(recovered.GetDatabase(), "destination", expectedTag, expectedValue);
            Assert.That((long)recovered.GetDatabase().Execute("PEXPIRETIME", "destination"), Is.EqualTo(expectedExpiration));
        }
    }
}