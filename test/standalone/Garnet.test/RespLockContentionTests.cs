// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// A session whose command finds a key latched by another session parks until the latch is released, holding no
    /// thread, and then runs the command again from its parse or continues it from the progress it recorded.
    /// </summary>
    /// <remarks>
    /// Each test holds a key with a stored procedure that locks it and then waits, inside its main phase, until the test
    /// lets it go. A command that meets the key parks behind it, which the test observes through the store's
    /// <see cref="LockReleaseSignal"/> listing a waiter.
    /// </remarks>
    [TestFixture, NonParallelizable]
    public class RespLockContentionTests : TestBase
    {
        static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(30);

        GarnetServer server;
        LockHolder holderA, holderB;
        Gate slowSetGate;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);

            holderA = new LockHolder();
            holderB = new LockHolder();
            slowSetGate = new Gate();
            server.Register.NewTransactionProc("HOLDA", () => new HoldLocks(holderA), new RespCommandsInfo { Arity = -2 });
            server.Register.NewTransactionProc("HOLDB", () => new HoldLocks(holderB), new RespCommandsInfo { Arity = -2 });
            server.Register.NewTransactionProc("LOCKAFTERREAD", () => new LockAfterRead(), new RespCommandsInfo { Arity = 3 });
            _ = server.Register.NewCommand("SLOWSET", CommandType.ReadModifyWrite, new SlowSet(slowSetGate), new RespCommandsInfo { Arity = 3 });

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            // Lets go of anything still held, so no network thread is left waiting inside a procedure or an update
            holderA.Release.Release(100);
            holderB.Release.Release(100);
            slowSetGate.Release.Release(100);

            server.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A read that meets a latched key part way through its reply is run again from its parse: the reply carries
        /// the values once, and the replies of the commands before it go out while it waits.
        /// </summary>
        [Test]
        public void AReadThatMeetsALatchedKeyIsRunAgainWithItsReplyStartedOver()
        {
            using var holder = Session();
            using var client = Session();

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.SendPipeline(["SET", "lc:a", "1"], ["MGET", "lc:a", "lc:held"], ["PING"]);
            WaitParked();

            ClassicAssert.AreEqual("+OK\r\n", client.ReadReply(), "The reply of the command before the parked one should go out");
            ClassicAssert.IsFalse(client.WaitForReply(TimeSpan.FromMilliseconds(200)), "Nothing more should arrive while parked");

            holderA.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder.ReadReply());

            ClassicAssert.AreEqual("*2\r\n$1\r\n1\r\n$4\r\nheld\r\n", client.ReadReply());
            ClassicAssert.AreEqual("+PONG\r\n", client.ReadReply());
        }

        /// <summary>
        /// MSET that meets a latched key after setting the keys before it continues from that key, so a write another
        /// client makes to an earlier key while it waits stands.
        /// </summary>
        [Test]
        public void MsetThatMeetsALatchedKeyContinuesFromThatKey()
        {
            using var holder = Session();
            using var client = Session();
            using var other = Session();

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.Send("MSET", "lc:a", "1", "lc:held", "2", "lc:c", "3");
            WaitParked();

            ClassicAssert.AreEqual("$1\r\n1\r\n", other.Execute("GET", "lc:a"), "MSET should have set the key before the latched one");
            ClassicAssert.AreEqual("+OK\r\n", other.Execute("SET", "lc:a", "X"));

            holderA.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder.ReadReply());
            ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());

            ClassicAssert.AreEqual("$1\r\nX\r\n", other.Execute("GET", "lc:a"), "MSET should not have set the earlier key again");
            ClassicAssert.AreEqual("$1\r\n2\r\n", other.Execute("GET", "lc:held"));
            ClassicAssert.AreEqual("$1\r\n3\r\n", other.Execute("GET", "lc:c"));
        }

        /// <summary>
        /// DEL that meets a latched key after deleting the keys before it continues from that key, counting what it
        /// deleted before it waited, and leaves a key another client creates again while it waits.
        /// </summary>
        [Test]
        public void DelThatMeetsALatchedKeyContinuesFromThatKeyAndCountsEveryKey()
        {
            using var holder = Session();
            using var client = Session();
            using var other = Session();

            foreach (var key in (string[])["lc:a", "lc:b", "lc:c", "lc:d"])
                ClassicAssert.AreEqual("+OK\r\n", other.Execute("SET", key, "v"));

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.Send("DEL", "lc:a", "lc:b", "lc:held", "lc:c", "lc:d");
            WaitParked();

            ClassicAssert.AreEqual(":0\r\n", other.Execute("EXISTS", "lc:a"), "DEL should have deleted the key before the latched one");
            ClassicAssert.AreEqual("+OK\r\n", other.Execute("SET", "lc:a", "again"));

            holderA.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder.ReadReply());
            ClassicAssert.AreEqual(":5\r\n", client.ReadReply());

            ClassicAssert.AreEqual("$5\r\nagain\r\n", other.Execute("GET", "lc:a"), "DEL should not have deleted the earlier key again");
            ClassicAssert.AreEqual(":0\r\n", other.Execute("EXISTS", "lc:held", "lc:c", "lc:d"));
        }

        /// <summary>
        /// EXEC that meets a latched key as it locks its keys waits without holding the transaction open, runs once the
        /// key is released, and leaves nothing behind that would keep a checkpoint waiting.
        /// </summary>
        [Test]
        public void ExecThatMeetsALatchedKeyRunsOnceReleasedAndLeavesNoTransactionOpen()
        {
            using var holder = Session();
            using var client = Session();

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.SendPipeline(["MULTI"], ["SET", "lc:held", "mine"], ["INCR", "lc:n"], ["EXEC"]);
            ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());
            ClassicAssert.AreEqual("+QUEUED\r\n", client.ReadReply());
            ClassicAssert.AreEqual("+QUEUED\r\n", client.ReadReply());
            WaitParked();

            holderA.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder.ReadReply());
            ClassicAssert.AreEqual("*2\r\n+OK\r\n:1\r\n", client.ReadReply());

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SAVE"), "A checkpoint should not wait on the transaction that parked");
            ClassicAssert.AreEqual("$4\r\nmine\r\n", client.Execute("GET", "lc:held"));
        }

        /// <summary>
        /// EXEC that parks on one latched key, is run again, and parks on another, lists its watched keys to lock once.
        /// </summary>
        /// <remarks>
        /// The transaction holds only the first key when EXEC first parks, and takes the second while it waits, so EXEC
        /// runs again on the first release and parks on the second, whichever order it locks them in.
        /// </remarks>
        [Test]
        public void ExecRunAgainListsItsWatchedKeysToLockOnce()
        {
            using var holder1 = Session();
            using var holder2 = Session();
            using var client = Session();

            var clientId = ClientId(client);
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("WATCH", "lc:k1", "lc:k2"));

            Hold(holder1, "HOLDA", holderA, "lc:k1");

            client.SendPipeline(["MULTI"], ["SET", "lc:k1", "mine"], ["SET", "lc:k2", "mine"], ["EXEC"]);
            ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());
            ClassicAssert.AreEqual("+QUEUED\r\n", client.ReadReply());
            ClassicAssert.AreEqual("+QUEUED\r\n", client.ReadReply());
            WaitParked();

            Hold(holder2, "HOLDB", holderB, "lc:k2");

            // The release takes the parked EXEC off the list before the holder replies, so a waiter listed after the
            // reply is EXEC parked again
            holderA.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder1.ReadReply());
            WaitParked();

            // Two keys queued and two watched
            ClassicAssert.AreEqual(4, KeyEntries(clientId).Count, "EXEC run again should not list its watched keys again");

            holderB.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder2.ReadReply());
            ClassicAssert.AreEqual("*-1\r\n", client.ReadReply(), "The holders changed the watched keys, so EXEC should abort");
        }

        /// <summary>
        /// A stored procedure that meets a latched key as it locks its keys is run again from its prepare phase once the
        /// key is released, and runs its main and finalize phases once.
        /// </summary>
        [Test]
        public void AProcedureThatMeetsALatchedKeyIsRunAgainFromItsPreparePhase()
        {
            using var holder = Session();
            using var client = Session();

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.Send("HOLDA", "lc:held");
            WaitParked();
            ClassicAssert.AreEqual(2, Volatile.Read(ref holderA.Prepares));
            ClassicAssert.AreEqual(1, Volatile.Read(ref holderA.Mains));

            // One for the holder, and one for the procedure run again
            holderA.Release.Release(2);
            ClassicAssert.AreEqual("+OK\r\n", holder.ReadReply());
            ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());

            ClassicAssert.AreEqual(3, Volatile.Read(ref holderA.Prepares));
            ClassicAssert.AreEqual(2, Volatile.Read(ref holderA.Mains));
            ClassicAssert.AreEqual(2, Volatile.Read(ref holderA.Finalizes));
        }

        /// <summary>
        /// A stored procedure whose prepare phase catches every exception, and so catches the one that unwinds it to
        /// park on a latched key, still parks.
        /// </summary>
        [Test]
        public void AProcedureThatCatchesTheUnwindStillParks()
        {
            using var holder = Session();
            using var client = Session();

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.Send("LOCKAFTERREAD", "lc:held", "lc:held");
            WaitParked();

            holderA.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder.ReadReply());
            ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());
        }

        /// <summary>
        /// A stored procedure run again from its prepare phase watches only what that run reads, so a key its earlier run
        /// read, changed while it waits, does not fail it.
        /// </summary>
        [Test]
        public void AProcedureRunAgainIsNotFailedByAKeyItsEarlierRunRead()
        {
            using var holder = Session();
            using var client = Session();
            using var other = Session();

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.Send("LOCKAFTERREAD", "lc:read", "lc:held");
            WaitParked();
            ClassicAssert.AreEqual("+OK\r\n", other.Execute("SET", "lc:read", "changed"));

            holderA.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", holder.ReadReply());
            ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());
        }

        /// <summary>
        /// A connection torn down while its session is parked on a latched key leaves the session no longer listed as
        /// waiting on it, while the key is still held.
        /// </summary>
        /// <remarks>
        /// Disposes the connection's handler, as server shutdown does for each connection, but for this one alone: the
        /// holder's thread is inside its procedure, so its own connection cannot be torn down under it.
        /// </remarks>
        [Test]
        public void AConnectionTornDownWhileParkedIsNoLongerListed()
        {
            using var holder = Session();
            using var client = Session();

            var clientId = ClientId(client);

            Hold(holder, "HOLDA", holderA, "lc:held");

            client.Send("GET", "lc:held");
            WaitParked();

            Handler(clientId).Dispose();
            ClassicAssert.IsFalse(Signal.HasWaiters, "The torn-down session should no longer be listed while the key is still held");
        }

        /// <summary>
        /// A read parked on a key latched for a single update, whose release is not announced, runs once it is released.
        /// </summary>
        [Test]
        public void AReadParkedOnASingleUpdateRunsOnceTheUpdateCompletes()
        {
            using var updater = Session();
            using var client = Session();

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", "lc:held", "aa"));

            // Same length, so the update is made in place, holding the key's latch across the gate
            updater.Send("SLOWSET", "lc:held", "bb");
            ClassicAssert.IsTrue(slowSetGate.Entered.Wait(WaitLimit), "The update never reached its gate");

            client.Send("GET", "lc:held");
            WaitParked();

            slowSetGate.Release.Release();
            ClassicAssert.AreEqual("+OK\r\n", updater.ReadReply());
            ClassicAssert.AreEqual("$2\r\nbb\r\n", client.ReadReply());
        }

        static RawRespSession Session() => new() { ReceiveTimeout = WaitLimit };

        /// <summary>
        /// Has <paramref name="session"/> run procedure <paramref name="proc"/>, which locks <paramref name="keys"/>, sets
        /// each to "held", and waits until <paramref name="holder"/> is released, and returns once it holds them.
        /// </summary>
        static void Hold(RawRespSession session, string proc, LockHolder holder, params string[] keys)
        {
            session.Send([proc, .. keys]);
            ClassicAssert.IsTrue(holder.Entered.Wait(WaitLimit), $"{proc} never took its locks");
        }

        LockReleaseSignal Signal => server.Provider.StoreWrapper.lockReleaseSignal;

        void WaitParked()
            => ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Signal.HasWaiters, WaitLimit), "No session parked on the latched key");

        static long ClientId(RawRespSession session)
            => long.Parse(session.Execute("CLIENT", "ID").AsSpan(1).TrimEnd("\r\n"));

        /// <summary>
        /// The network handler of the connection whose session has id <paramref name="clientId"/>.
        /// </summary>
        INetworkHandler Handler(long clientId)
        {
            var servers = typeof(GarnetServer).GetField("servers", BindingFlags.Instance | BindingFlags.NonPublic);
            var handlers = typeof(GarnetServerBase).GetField("activeHandlers", BindingFlags.Instance | BindingFlags.NonPublic);
            ClassicAssert.IsNotNull(servers, "GarnetServer should still hold its listeners in a 'servers' field");
            ClassicAssert.IsNotNull(handlers, "GarnetServerBase should still hold its connections in an 'activeHandlers' field");

            var found = new List<INetworkHandler>();
            foreach (var listener in (IGarnetServer[])servers.GetValue(server))
                foreach (var handler in ((ConcurrentDictionary<INetworkHandler, byte>)handlers.GetValue(listener)).Keys)
                    if (handler.Session is RespServerSession resp && resp.Id == clientId)
                        found.Add(handler);

            ClassicAssert.AreEqual(1, found.Count, "Expected exactly one connection with the client's id");
            return found[0];
        }

        /// <summary>
        /// The keys to lock listed by the transaction of the session with id <paramref name="clientId"/>.
        /// </summary>
        TxnKeyEntries KeyEntries(long clientId)
        {
            var keyEntries = typeof(TransactionManager).GetField("keyEntries", BindingFlags.Instance | BindingFlags.NonPublic);
            ClassicAssert.IsNotNull(keyEntries, "TransactionManager should still hold its keys to lock in a 'keyEntries' field");
            return (TxnKeyEntries)keyEntries.GetValue(((RespServerSession)Handler(clientId).Session).txnManager);
        }

        sealed class LockHolder
        {
            public readonly SemaphoreSlim Entered = new(0);
            public readonly SemaphoreSlim Release = new(0);
            public int Prepares, Mains, Finalizes;
        }

        sealed class Gate
        {
            public readonly SemaphoreSlim Entered = new(0);
            public readonly SemaphoreSlim Release = new(0);

            public void Pass()
            {
                Entered.Release();
                _ = Release.Wait(WaitLimit);
            }
        }

        /// <summary>
        /// HOLD key [key ...]: locks the keys, sets each to "held", and waits until the test releases it.
        /// </summary>
        sealed class HoldLocks(LockHolder holder) : CustomTransactionProcedure
        {
            public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
            {
                _ = Interlocked.Increment(ref holder.Prepares);
                var offset = 0;
                for (var i = 0; i < procInput.parseState.Count; i++)
                    AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
                return true;
            }

            public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            {
                _ = Interlocked.Increment(ref holder.Mains);
                var held = CreateArgSlice("held");
                var offset = 0;
                for (var i = 0; i < procInput.parseState.Count; i++)
                    _ = api.SET(GetNextArg(ref procInput, ref offset), held);

                holder.Entered.Release();
                if (holder.Release.Wait(WaitLimit))
                    WriteSimpleString(ref output, "OK");
                else
                    WriteError(ref output, "ERR not released");
            }

            public override void Finalize<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
                => _ = Interlocked.Increment(ref holder.Finalizes);
        }

        /// <summary>
        /// LOCKAFTERREAD readkey lockkey: reads the first key in its prepare phase, catching any exception, and then
        /// locks the second.
        /// </summary>
        sealed class LockAfterRead : CustomTransactionProcedure
        {
            public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
            {
                var offset = 0;
                var readKey = GetNextArg(ref procInput, ref offset);
                var lockKey = GetNextArg(ref procInput, ref offset);
                try
                {
                    _ = api.GET(readKey, out PinnedSpanByte _);
                }
                catch
                {
                    return false;
                }
                AddKey(lockKey, LockType.Exclusive, StoreType.Main);
                return true;
            }

            public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
                => WriteSimpleString(ref output, "OK");
        }

        /// <summary>
        /// SLOWSET key value: sets the key, passing a gate while it holds the key's latch.
        /// </summary>
        sealed class SlowSet(Gate gate) : CustomRawStringFunctions
        {
            public override bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
                => throw new InvalidOperationException();

            public override bool NeedInitialUpdate(scoped ReadOnlySpan<byte> key, ref StringInput input, ref RespMemoryWriter writer)
                => true;

            public override int GetInitialLength(ref StringInput input)
                => GetFirstArg(ref input).Length;

            public override bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            {
                GetFirstArg(ref input).CopyTo(value);
                return true;
            }

            public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            {
                var newValue = GetFirstArg(ref input);
                if (newValue.Length > value.Length)
                    return false;

                gate.Pass();
                newValue.CopyTo(value);
                valueLength = newValue.Length;
                return true;
            }

            public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, ref RespMemoryWriter writer)
                => true;

            public override int GetLength(ReadOnlySpan<byte> value, ref StringInput input)
                => GetFirstArg(ref input).Length;

            public override bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            {
                GetFirstArg(ref input).CopyTo(newValue);
                return true;
            }
        }
    }
}