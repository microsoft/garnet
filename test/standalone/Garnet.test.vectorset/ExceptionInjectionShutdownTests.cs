// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// A thread parked in <see cref="ExceptionInjectionHelper.ResetAndWaitAsync"/> is only released by
    /// <see cref="ExceptionInjectionHelper.EnableException"/>, but the cleanup a test runs on its way out
    /// is <see cref="ExceptionInjectionHelper.DisableException"/>. A test that leaves between a waiter
    /// arriving and being re-enabled strands that waiter, and because the waiter is a server thread holding
    /// a pooled network buffer, <c>LimitedFixedBufferPool.Dispose</c> then spins forever on a reference that
    /// is never returned - hanging the whole test process instead of failing one test.
    /// </summary>
    [TestFixture]
    public class ExceptionInjectionShutdownTests : TestBase
    {
        private const ExceptionInjectionType Pause = ExceptionInjectionType.VectorSet_Pause_Before_Synthetic_Replication_Rmw;

        private global::Garnet.GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            ExceptionInjectionHelper.DisableException(Pause);

            server?.Dispose();
            server = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public void ServerDisposeCompletesWhileAnInjectionPointIsParked()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableVectorSetPreview: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ExceptionInjectionHelper.EnableException(Pause);

            // Parks inside ReplicateVectorSetAdd while holding a pooled network buffer.
            _ = Task.Run(() => db.Execute("VADD", ["parked-vs", "VALUES", "3", "1", "2", "3", "elem"]));

            // ResetAndWaitAsync clears the flag when it arrives, so this is the arrival signal.
            ClassicAssert.IsTrue(
                ExceptionInjectionHelper.WaitOnClearAsync(Pause).Wait(TimeSpan.FromSeconds(30)),
                "the server never reached the injection point");

            // What a failing test leaves behind: the flag is cleared, so nothing will ever release the waiter.
            ExceptionInjectionHelper.DisableException(Pause);

            // Hand ownership to the dispose task before starting it: if the waiter is stranded this call
            // never returns, and TearDown must not go on to block the run on the same disposal.
            var disposing = server;
            server = null;

            var dispose = Task.Run(() => disposing.Dispose(deleteDir: false));

            ClassicAssert.IsTrue(
                dispose.Wait(TimeSpan.FromSeconds(30)),
                "GarnetServer.Dispose() did not complete: a parked injection point kept a pooled buffer " +
                "checked out, so the buffer pool drain never finished");
        }

        /// <summary>
        /// Releasing only the waiters that are already parked leaves the same hang one moment later:
        /// disposal closes listeners before it drains handlers, so a request that is already in flight can
        /// reach a still-armed injection point after shutdown has begun and park there instead. Nothing
        /// re-enables it, so it strands its pooled buffer exactly as before.
        /// </summary>
        [Test]
        public void ArrivingAtAnInjectionPointDuringShutdownDoesNotPark()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            ExceptionInjectionHelper.EnableException(Pause);
            ExceptionInjectionHelper.SuspendParking();

            try
            {
                var arriving = Task.Run(() => ExceptionInjectionHelper.ResetAndWait(Pause));

                ClassicAssert.IsTrue(
                    arriving.Wait(TimeSpan.FromSeconds(30)),
                    "a caller reaching an injection point after shutdown began parked instead of proceeding, " +
                    "so it would still be holding its pooled buffer when the drain runs");
            }
            finally
            {
                ExceptionInjectionHelper.ResumeParking();
                ExceptionInjectionHelper.DisableException(Pause);
            }
        }

        /// <summary>
        /// The suspension must not outlive the shutdown that raised it, or a later test in the same process
        /// would find its injection points no longer pausing anything.
        /// </summary>
        [Test]
        public void ParkingResumesAfterShutdownCompletes()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            ExceptionInjectionHelper.SuspendParking();
            ExceptionInjectionHelper.ResumeParking();

            ExceptionInjectionHelper.EnableException(Pause);

            try
            {
                var parked = Task.Run(() => ExceptionInjectionHelper.ResetAndWait(Pause));

                ClassicAssert.IsTrue(
                    ExceptionInjectionHelper.WaitOnClearAsync(Pause).Wait(TimeSpan.FromSeconds(30)),
                    "the caller never reached the injection point");

                ClassicAssert.IsFalse(
                    parked.Wait(TimeSpan.FromSeconds(1)),
                    "the injection point did not pause: a suspension leaked past the shutdown that raised it");

                // Releasing it the normal way proves the rendezvous is otherwise intact.
                ExceptionInjectionHelper.EnableException(Pause);

                ClassicAssert.IsTrue(
                    parked.Wait(TimeSpan.FromSeconds(30)),
                    "re-enabling the injection point did not release the parked caller");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Pause);
            }
        }
    }
}