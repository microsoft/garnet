// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Tests for how Vector Set operations take the Vector Set lock and the store sessions they use, against
    /// concurrent deletes, other readers, and index growth.
    /// </summary>
    [TestFixture]
    public class VectorSetLockingTests : TestBase
    {
        /// <summary>
        /// How long an operation that should not wait on another one is given before it is judged blocked.
        /// </summary>
        private static readonly TimeSpan NotBlockedTimeout = TimeSpan.FromSeconds(10);

        private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableVectorSetPreview: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A read-only Vector Set command holds the Vector Set lock shared while it uses the index, so another reader of
        /// the same set proceeds while the first one is still holding it.
        /// </summary>
        [Test]
        public async Task ReadOnlyCommandsShareTheVectorSetLockAsync([Values("VCARD", "VEMB", "VSIM")] string command)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const ExceptionInjectionType Pause = ExceptionInjectionType.VectorSet_Pause_After_Index_Read;
            const string Key = nameof(ReadOnlyCommandsShareTheVectorSetLockAsync);

            using var redisHolder = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisOther = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var dbHolder = redisHolder.GetDatabase(0);
            var dbOther = redisOther.GetDatabase(0);

            AddMembers(dbHolder, Key, count: 4);

            ExceptionInjectionHelper.EnableException(Pause);
            try
            {
                // The first reader parks holding the Vector Set lock it took to read the index.
                var holder = dbHolder.ExecuteAsync(command, ReadOnlyCommandArguments(command, Key));
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

                var other = dbOther.ExecuteAsync("VCARD", Key);
                var otherRanAlongside = await CompletesWithinAsync(other, NotBlockedTimeout);

                var holderResult = await ReleaseParkedAndAwaitAsync(holder);
                var otherResult = await other.WaitAsync(WaitTimeout);

                ClassicAssert.IsTrue(otherRanAlongside,
                    $"VCARD waited for a {command} that holds the Vector Set lock: read-only commands must take it shared");
                AssertReadOnlyCommandResult(command, holderResult, expectedCard: 4);
                ClassicAssert.AreEqual(4, (int)otherResult);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Pause);
            }
        }

        /// <summary>
        /// DEL of a Vector Set must not free its DiskANN index while another operation holds the Vector Set lock and is
        /// about to use that index. The index is dropped once the operation releases the lock.
        /// </summary>
        [Test]
        public async Task DeleteDoesNotDropIndexInUseAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();
#if DEBUG
            const ExceptionInjectionType Pause = ExceptionInjectionType.VectorSet_Pause_After_Index_Read;
            const ExceptionInjectionType Fail = ExceptionInjectionType.VectorSet_Fail_After_Index_Read;
            const string Key = nameof(DeleteDoesNotDropIndexInUseAsync);

            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;

            using var redisReader = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisDel = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var dbReader = redisReader.GetDatabase(0);
            var dbDel = redisDel.GetDatabase(0);

            AddMembers(dbDel, Key, count: 4);
            vectorManager.WaitForQuiescence();
            var dropsBefore = vectorManager.Service.DropIndexCalls;

            ExceptionInjectionHelper.EnableException(Pause);
            try
            {
                // The reader parks holding the Vector Set lock, with the index pointer read and not yet used.
                var reader = dbReader.ExecuteAsync("VCARD", Key);
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

                ClassicAssert.IsTrue(await dbDel.KeyDeleteAsync(Key).WaitAsync(WaitTimeout), "DEL did not remove the Vector Set");
                var droppedUnderReader = vectorManager.Service.DropIndexCalls != dropsBefore;

                if (droppedUnderReader)
                {
                    // The index the parked reader is about to use has been freed; fail the reader rather than let it
                    // call into freed native memory.
                    ExceptionInjectionHelper.EnableException(Fail);
                }

                RedisResult readerResult = null;
                try
                {
                    readerResult = await ReleaseParkedAndAwaitAsync(reader);
                }
                catch (RedisException) when (droppedUnderReader)
                {
                }

                ClassicAssert.IsFalse(droppedUnderReader,
                    "DEL dropped the DiskANN index while a reader held the Vector Set lock and was about to use it");
                ClassicAssert.AreEqual(4, (int)readerResult, "the reader that read the index before DEL must see the whole set");

                vectorManager.WaitForQuiescence();
                ClassicAssert.AreEqual(dropsBefore + 1, vectorManager.Service.DropIndexCalls,
                    "the index must be dropped exactly once, after the reader releases the Vector Set lock");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Pause);
                ExceptionInjectionHelper.DisableException(Fail);
            }
#else
            await Task.CompletedTask;
#endif
        }

        /// <summary>
        /// DEL of a Vector Set whose index record is superseded rather than elided leaves that record sealed in the log,
        /// where page eviction visits it again. The deleted record must no longer carry the index pointer, or eviction
        /// requests a second drop of an index DEL already dropped.
        /// </summary>
        [Test]
        public void DeletedIndexRecordIsNotDroppedAgainOnEviction()
        {
#if DEBUG
            const string Key = nameof(DeletedIndexRecordIsNotDroppedAgainOnEviction);
            var keyBytes = Encoding.ASCII.GetBytes(Key);

            // A small hash index, so that a key sharing the index record's hash chain is quick to find
            const string IndexSize = "1k";
            const long IndexBuckets = 1024 / 64;

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableVectorSetPreview: true, indexSize: IndexSize);
            server.Start();

            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;
            var store = server.Provider.StoreWrapper.store;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            AddMembers(db, Key, count: 2);
            vectorManager.WaitForQuiescence();
            var dropsBefore = vectorManager.Service.DropIndexCalls;

            // A later record in the index record's hash chain keeps DEL from eliding it, so DEL supersedes it instead.
            ClassicAssert.IsTrue(db.StringSet(TestUtils.FindKeyInSameHashChain(Key, IndexBuckets), "value"));
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            _ = db.KeyDelete(Key);
            ClassicAssert.IsFalse(db.KeyExists(Key), "DEL did not remove the Vector Set");

            var deletedRecords = 0;
            var deletedRecordWithIndex = LogAddress.kInvalidAddress;
            using (var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress, DiskScanBufferingMode.NoBuffering, includeClosedRecords: true))
            {
                while (iter.GetNext())
                {
                    // Only records that eviction visits
                    if (iter.Info.Invalid || iter.Info.Tombstone || iter.RecordType != VectorManager.RecordType || !iter.Key.SequenceEqual(keyBytes))
                        continue;

                    deletedRecords++;

                    VectorManager.ReadIndex(iter.ValueSpan, out _, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);
                    if (indexPtr != 0)
                        deletedRecordWithIndex = iter.CurrentAddress;
                }
            }

            ClassicAssert.AreEqual(1, deletedRecords, "DEL elided the index record rather than superseding it");
            ClassicAssert.AreEqual(LogAddress.kInvalidAddress, deletedRecordWithIndex,
                $"the deleted index record at {deletedRecordWithIndex} still carries the index pointer that DEL dropped");

            // Evicting the sealed record must not request another drop.
            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            vectorManager.WaitForQuiescence();
            ClassicAssert.AreEqual(dropsBefore + 1, vectorManager.Service.DropIndexCalls, "the deleted index must be dropped exactly once");
#else
            Assert.Ignore("Relies on DiskANNService call counters, only enabled in DEBUG builds");
#endif
        }

        /// <summary>
        /// A transaction of read-only Vector Set commands runs them on the sessions Vector Sets use for index and element
        /// data. Those sessions must be enrolled in the transaction: an unenrolled session waits out PREPARE_GROW like
        /// any session outside a transaction, while an index grow waits in PREPARE_GROW for the transaction to finish.
        /// </summary>
        [Test]
        public async Task ReadOnlyCommandsInTransactionProceedDuringPrepareGrowAsync([Values("VCARD", "VEMB", "VSIM")] string command)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const ExceptionInjectionType Pause = ExceptionInjectionType.Transaction_Pause_After_Lock_Acquisition;
            const string Key = nameof(ReadOnlyCommandsInTransactionProceedDuringPrepareGrowAsync);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            AddMembers(db, Key, count: 4);
            server.Provider.StoreWrapper.DefaultDatabase.VectorManager.WaitForQuiescence();

            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            var prepareGrow = new HoldPrepareGrowStateMachine();
            var registered = false;

            ExceptionInjectionHelper.EnableException(Pause);
            try
            {
                // The transaction parks in EXEC with its keys locked, before running the queued command.
                var transaction = db.CreateTransaction();
                var queued = transaction.ExecuteAsync(command, ReadOnlyCommandArguments(command, Key));
                var exec = transaction.ExecuteAsync();
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

                // Hold the store in PREPARE_GROW, as an index grow does while it waits for transactions to finish.
                registered = driver.Register(prepareGrow);
                ClassicAssert.IsTrue(registered, "Could not start the PREPARE_GROW state machine");
                ClassicAssert.IsTrue(prepareGrow.Entered.Wait(WaitTimeout), "The state machine never reached PREPARE_GROW");

                bool ranDuringPrepareGrow;
                ExceptionInjectionHelper.SuspendParking();
                try
                {
                    ranDuringPrepareGrow = await CompletesWithinAsync(exec, NotBlockedTimeout);

                    prepareGrow.Release();
                    ClassicAssert.IsTrue(await exec.WaitAsync(WaitTimeout), "EXEC failed");
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }

                ClassicAssert.IsTrue(ranDuringPrepareGrow,
                    $"{command} in a transaction waited out PREPARE_GROW: a session it uses is not enrolled in the transaction");
                AssertReadOnlyCommandResult(command, await queued.WaitAsync(WaitTimeout), expectedCard: 4);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Pause);
                prepareGrow.Release();

                if (registered)
                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => driver.SystemState.Phase == Phase.REST, WaitTimeout), "The state machine never returned to REST");
            }
        }

        /// <summary>
        /// A Lua script run as a transaction enrolls the sessions Vector Sets use for index and element data, as EXEC does, so
        /// a read-only Vector Set command in the script proceeds while an index grow waits in PREPARE_GROW for the transaction.
        /// </summary>
        [Test]
        public async Task ReadOnlyCommandsInLuaTransactionProceedDuringPrepareGrowAsync([Values("VCARD", "VEMB", "VSIM")] string command)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const ExceptionInjectionType Pause = ExceptionInjectionType.Transaction_Pause_After_Lock_Acquisition;
            const string Key = nameof(ReadOnlyCommandsInLuaTransactionProceedDuringPrepareGrowAsync);
            const string DeclaredKey = "declared";

            // The default index size of TestUtils.CreateGarnetServer, 1m, in buckets of 64 bytes
            const long IndexBuckets = (1L << 20) / 64;

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableVectorSetPreview: true, enableLua: true, luaTransactionMode: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            AddMembers(db, Key, count: 4);
            server.Provider.StoreWrapper.DefaultDatabase.VectorManager.WaitForQuiescence();

            // A script runs as a transaction only if it declares a key, which the transaction locks exclusively, and a Vector
            // Set command's index read does not go through the transaction's locks, so it would wait on that lock forever.
            // The script declares another key instead, in another bucket, and is passed the set's key as an argument.
            ClassicAssert.AreNotEqual(SpanByteComparer.StaticGetHashCode64(Encoding.ASCII.GetBytes(Key)) & (IndexBuckets - 1),
                SpanByteComparer.StaticGetHashCode64(Encoding.ASCII.GetBytes(DeclaredKey)) & (IndexBuckets - 1), "The declared key shares the set's bucket");

            var arguments = new object[] { command }.Concat(ReadOnlyCommandArguments(command, Key)).Select(static arg => (RedisValue)arg.ToString()).ToArray();
            var script = $"return redis.call({string.Join(", ", Enumerable.Range(1, arguments.Length).Select(static i => $"ARGV[{i}]"))})";

            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            var prepareGrow = new HoldPrepareGrowStateMachine();
            var registered = false;

            ExceptionInjectionHelper.EnableException(Pause);
            try
            {
                // The script parks with its key locked, before it runs.
                var run = db.ScriptEvaluateAsync(script, [DeclaredKey], arguments);
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

                // Hold the store in PREPARE_GROW, as an index grow does while it waits for transactions to finish.
                registered = driver.Register(prepareGrow);
                ClassicAssert.IsTrue(registered, "Could not start the PREPARE_GROW state machine");
                ClassicAssert.IsTrue(prepareGrow.Entered.Wait(WaitTimeout), "The state machine never reached PREPARE_GROW");

                bool ranDuringPrepareGrow;
                RedisResult result;
                ExceptionInjectionHelper.SuspendParking();
                try
                {
                    ranDuringPrepareGrow = await CompletesWithinAsync(run, NotBlockedTimeout);

                    prepareGrow.Release();
                    result = await run.WaitAsync(WaitTimeout);
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }

                ClassicAssert.IsTrue(ranDuringPrepareGrow,
                    $"{command} in a Lua transaction waited out PREPARE_GROW: a session it uses is not enrolled in the transaction");
                AssertReadOnlyCommandResult(command, result, expectedCard: 4);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Pause);
                prepareGrow.Release();

                if (registered)
                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => driver.SystemState.Phase == Phase.REST, WaitTimeout), "The state machine never returned to REST");
            }
        }

        /// <summary>
        /// While a checkpoint waits for a transaction, the sessions the transaction has locked keys through stay pinned to
        /// its version. That is only safe for locked keys, whose hash chains no other session can add a next-version record
        /// to. Vector Set commands read element data on another session, through keys the transaction has not locked, so a
        /// pinned element session meets a next-version record at the head of an element's hash chain that it can never get
        /// past while the checkpoint waits for the transaction. That session must not be pinned.
        /// </summary>
        [Test]
        public async Task ReadOnlyCommandInTransactionReadsElementsDuringCheckpointAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const ExceptionInjectionType Pause = ExceptionInjectionType.Transaction_Pause_After_Lock_Acquisition;
            const string Key = nameof(ReadOnlyCommandInTransactionReadsElementsDuringCheckpointAsync);
            const int Members = 4;

            // A small hash index, so that a key sharing an element record's hash chain is quick to find
            const string IndexSize = "1k";
            const long IndexBuckets = 1024 / 64;

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableVectorSetPreview: true, indexSize: IndexSize);
            server.Start();

            var store = server.Provider.StoreWrapper.store;
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisOther = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var dbOther = redisOther.GetDatabase(0);

            AddMembers(db, Key, count: Members);
            server.Provider.StoreWrapper.DefaultDatabase.VectorManager.WaitForQuiescence();

            var (member, collidingKey) = FindKeyInMemberIdChain(store, Key, Members, IndexBuckets);

            ExceptionInjectionHelper.EnableException(Pause);
            try
            {
                // The transaction parks in EXEC with its key locked and its version verified, before running VEMB.
                var transaction = db.CreateTransaction();
                var queued = transaction.ExecuteAsync("VEMB", Key, $"member{member}");
                var exec = transaction.ExecuteAsync();
                await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

                // The checkpoint moves the store to the next version, and waits in IN_PROGRESS for the transaction.
                ClassicAssert.AreEqual("Background saving started", (string)await dbOther.ExecuteAsync("BGSAVE").WaitAsync(WaitTimeout));
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => driver.SystemState.Phase == Phase.IN_PROGRESS, WaitTimeout), "The checkpoint never reached IN_PROGRESS");

                // A record in the next version now heads the hash chain of the element record VEMB reads first.
                ClassicAssert.IsTrue(await dbOther.StringSetAsync(collidingKey, "value").WaitAsync(WaitTimeout));
                ClassicAssert.AreEqual(Phase.IN_PROGRESS, driver.SystemState.Phase, "The checkpoint did not wait for the transaction");

                bool ranDuringCheckpoint;
                ExceptionInjectionHelper.SuspendParking();
                try
                {
                    ranDuringCheckpoint = await CompletesWithinAsync(exec, NotBlockedTimeout);
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }

                ClassicAssert.IsTrue(ranDuringCheckpoint, "EXEC did not finish while a checkpoint waited for it");
                Assert.DoesNotThrowAsync(() => exec,
                    "EXEC failed while a checkpoint waited for it: VEMB read element data on a session pinned to the transaction's version");
                ClassicAssert.IsTrue(await exec, "EXEC was aborted");
                CollectionAssert.AreEqual(new[] { member + 1.0, 2.0, 3.0 }, ((RedisResult[])await queued).Select(static r => Math.Round((double)r, 3)).ToArray());

                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => driver.SystemState.Phase == Phase.REST, WaitTimeout), "The checkpoint never completed");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Pause);
            }
        }

        /// <summary>
        /// Picks a member whose internal id record, which VEMB reads first, is in another hash bucket than the Vector Set's
        /// index record, which a transaction on the Vector Set locks, and finds a key that shares that record's hash chain.
        /// </summary>
        private static (int Member, string CollidingKey) FindKeyInMemberIdChain<TStoreFunctions, TAllocator>(
            TsavoriteKV<TStoreFunctions, TAllocator> store, string key, int members, long indexBuckets)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var keyBytes = Encoding.ASCII.GetBytes(key);

            ulong context = 0;
            using (var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress, DiskScanBufferingMode.NoBuffering))
            {
                while (iter.GetNext())
                {
                    if (!iter.Info.Tombstone && iter.RecordType == VectorManager.RecordType && iter.Key.SequenceEqual(keyBytes))
                        VectorManager.ReadIndex(iter.ValueSpan, out context, out _, out _, out _, out _, out _, out _, out _, out _);
                }
            }

            ClassicAssert.AreNotEqual(0UL, context, "The Vector Set's index record was not found");

            var indexRecordBucket = SpanByteComparer.StaticGetHashCode64(keyBytes) & (indexBuckets - 1);

            Span<byte> namespaceBytes = stackalloc byte[sizeof(uint)];
            VectorManager.StoreContextInNamespace(context | DiskANNService.InternalIdMap, ref namespaceBytes);

            for (var member = 0; member < members; member++)
            {
                var memberIdHash = GarnetKeyComparer.StaticGetHashCode64(new VectorElementKey(namespaceBytes, Encoding.ASCII.GetBytes($"member{member}")));
                if ((memberIdHash & (indexBuckets - 1)) != indexRecordBucket)
                    return (member, TestUtils.FindKeyInSameHashChain(memberIdHash, indexBuckets, prefix: "collides"));
            }

            Assert.Fail("Every member's internal id record is in the index record's hash bucket");
            return default;
        }

        private static void AddMembers(IDatabase db, string key, int count)
        {
            for (var i = 0; i < count; i++)
                ClassicAssert.AreEqual(1, (int)db.Execute("VADD", key, "VALUES", "3", $"{i + 1}", "2", "3", $"member{i}", "NOQUANT"));
        }

        private static object[] ReadOnlyCommandArguments(string command, string key)
            => command switch
            {
                "VCARD" => [key],
                "VEMB" => [key, "member0"],
                "VSIM" => [key, "VALUES", "3", "1", "2", "3", "COUNT", "4"],
                _ => throw new ArgumentOutOfRangeException(nameof(command), command, null),
            };

        private static void AssertReadOnlyCommandResult(string command, RedisResult result, int expectedCard)
        {
            switch (command)
            {
                case "VCARD":
                    ClassicAssert.AreEqual(expectedCard, (int)result);
                    break;
                case "VEMB":
                    CollectionAssert.AreEqual(new[] { 1.0, 2.0, 3.0 }, ((RedisResult[])result).Select(static r => Math.Round((double)r, 3)).ToArray());
                    break;
                case "VSIM":
                    ClassicAssert.AreEqual(expectedCard, ((RedisResult[])result).Length);
                    break;
            }
        }

        private static async Task<bool> CompletesWithinAsync(Task task, TimeSpan timeout)
            => await Task.WhenAny(task, Task.Delay(timeout)) == task;

        /// <summary>
        /// Releases the operation parked at an injection point without re-arming the point, and waits for that
        /// operation to finish, so it runs straight through the point if it passes it again.
        /// </summary>
        private static async Task<RedisResult> ReleaseParkedAndAwaitAsync(Task<RedisResult> parked)
        {
            ExceptionInjectionHelper.SuspendParking();
            try
            {
                return await parked.WaitAsync(WaitTimeout);
            }
            finally
            {
                ExceptionInjectionHelper.ResumeParking();
            }
        }

        /// <summary>
        /// Moves the store into PREPARE_GROW and holds it there until released, then returns it to REST. PREPARE_GROW is
        /// the phase an index grow waits in for running transactions to finish; sessions that are not enrolled in a
        /// transaction wait for the phase to end before running an operation.
        /// </summary>
        private sealed class HoldPrepareGrowStateMachine : IStateMachine
        {
            private readonly ManualResetEventSlim release = new(false);

            /// <summary>Set once PREPARE_GROW is in effect for every session.</summary>
            internal readonly ManualResetEventSlim Entered = new(false);

            internal void Release() => release.Set();

            /// <inheritdoc/>
            public SystemState NextState(SystemState start)
            {
                var next = start;
                next.Phase = start.Phase == Phase.REST ? Phase.PREPARE_GROW : Phase.REST;
                return next;
            }

            /// <inheritdoc/>
            public void GlobalBeforeEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver)
            {
                // The driver leaves PREPARE_GROW for REST only after PREPARE_GROW has fully taken effect, and it holds no
                // epoch here, so parking keeps PREPARE_GROW published without stalling the epoch.
                if (nextState.Phase == Phase.REST)
                {
                    Entered.Set();
                    _ = release.Wait(WaitTimeout);
                }
            }

            /// <inheritdoc/>
            public void GlobalAfterEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver) { }
        }
    }
}