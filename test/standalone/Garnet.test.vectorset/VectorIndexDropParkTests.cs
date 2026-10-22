// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A Vector Set command that has to recreate a DiskANN index whose earlier index is still to be dropped cannot run
    /// until the drop is done. The drop is made by a task on the thread pool, so a command that waits for it on its own
    /// pool thread holds a thread the drop and every other session may need; such a command parks its session instead.
    /// </summary>
    [TestFixture]
    public class VectorIndexDropParkTests : TestBase
    {
        const ExceptionInjectionType Hold = ExceptionInjectionType.VectorSet_Hold_Index_Drop;
        const string Key = "drop-park";
        const string Pong = "+PONG\r\n";

        static readonly TimeSpan StallLimit = TimeSpan.FromSeconds(10);

        global::Garnet.GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: "16m", pageSize: "1m", enableVectorSetPreview: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            ExceptionInjectionHelper.DisableException(Hold);
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public void ACommandWaitingOnAnIndexDropDoesNotHoldAThread()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;

            using var writer = new RawRespSession();
            ClassicAssert.AreEqual(":1\r\n", writer.Execute("VADD", Key, "VALUES", "3", "1", "2", "3", "member", "NOQUANT"));

            // Evicting the index record requests a drop of its in-memory index, which the hold keeps pending
            ExceptionInjectionHelper.EnableException(Hold);
            StringAssert.StartsWith("+OK", writer.Execute("DEBUG", "FLUSHANDEVICT"));

            var held = Stopwatch.StartNew();
            while (ExceptionInjectionHelper.WaitingOnClear(Hold) == 0)
            {
                ClassicAssert.Less(held.Elapsed, StallLimit, "The index drop task never reached the hold");
                Thread.Sleep(10);
            }
            ClassicAssert.IsTrue(vectorManager.DropRequested(System.Text.Encoding.ASCII.GetBytes(Key)));

            using var reader = new RawRespSession();
            using var pinger = new RawRespSession();

            // With more than one free worker the claim is unfalsifiable: the other sessions are served by the spare
            using (ThreadPoolCap.ApplyFree(1))
            {
                ClassicAssert.AreEqual(Pong, reader.Execute("PING"));
                ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));

                // Has to recreate the index, so waits for the drop
                reader.Send("VCARD", Key);

                for (var i = 0; i < 20; i++)
                {
                    pinger.Send("PING");
                    ClassicAssert.IsTrue(pinger.WaitForReply(StallLimit),
                        $"PING {i} was not answered within {StallLimit.TotalSeconds:F0}s while another session waited on an " +
                        "index drop against a pool of one thread, which indicates the wait was holding it.");
                    ClassicAssert.AreEqual(Pong, pinger.ReadReply());
                }

                ClassicAssert.IsFalse(reader.WaitForReply(TimeSpan.FromMilliseconds(200)), "VCARD ran before the index was dropped");

                ExceptionInjectionHelper.DisableException(Hold);

                ClassicAssert.IsTrue(reader.WaitForReply(StallLimit), "VCARD did not run once the index was dropped");
                ClassicAssert.AreEqual(":1\r\n", reader.ReadReply());
                ClassicAssert.IsFalse(vectorManager.DropRequested(System.Text.Encoding.ASCII.GetBytes(Key)));
            }

            ClassicAssert.AreEqual(":1\r\n", writer.Execute("VADD", Key, "VALUES", "3", "4", "5", "6", "other", "NOQUANT"));
            ClassicAssert.AreEqual(":2\r\n", writer.Execute("VCARD", Key));
        }
    }
}