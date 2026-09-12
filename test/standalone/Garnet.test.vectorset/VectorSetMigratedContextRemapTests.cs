// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Contexts are chosen independently by each node, so a context that was free on the PRIMARY which
    /// picked it can be occupied on the node receiving the migration, and that migration is steered onto a
    /// context that is free locally. The context it is steered onto gets marked migrating, which is also how
    /// a migration that was interrupted and resumed looks, so a later incoming context whose own value
    /// happens to equal that one must not read that mark as its own reservation and adopt it.
    /// </summary>
    [TestFixture]
    public class VectorSetMigratedContextRemapTests : TestBase
    {
        private global::Garnet.GarnetServer server;

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
            server?.Dispose();
            server = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public void RemappedMigrationTargetIsNotAdoptedByALaterMigration()
        {
            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;

            // Occupy the low contexts, so the first incoming context cannot be adopted as-is
            vectorManager.AllocateTestContexts(4);

            var occupiedLocally = VectorManager.ContextStep;

            var firstLocal = vectorManager.ResolveMigratedContextForTest(occupiedLocally);
            ClassicAssert.AreNotEqual(occupiedLocally, firstLocal, "A context that is in use locally must be steered elsewhere");

            // The PRIMARY assigns contexts independently, so the value the first migration was steered onto
            // can equal the value a second, unrelated migration arrives with
            var secondLocal = vectorManager.ResolveMigratedContextForTest(firstLocal);

            ClassicAssert.AreNotEqual(firstLocal, secondLocal, "Two migrated Vector Sets were mapped onto a single context");

            // Resolution is stable, so replaying either migration's records keeps landing in the same place
            ClassicAssert.AreEqual(firstLocal, vectorManager.ResolveMigratedContextForTest(occupiedLocally));
            ClassicAssert.AreEqual(secondLocal, vectorManager.ResolveMigratedContextForTest(firstLocal));
        }
    }
}