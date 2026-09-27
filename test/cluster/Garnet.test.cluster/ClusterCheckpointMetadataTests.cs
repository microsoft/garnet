// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.IO;
using Garnet.cluster;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Covers the cluster override of <c>GetLogCheckpointMetadata</c>, which reads the same length-prefixed metadata
    /// the base class does but previously sliced by that length without validating it. The manager is reached through
    /// <see cref="ClusterFactory.CreateCheckpointManager"/> because the cluster type itself is internal; the factory
    /// returns it as its public base type with the override in place.
    /// </summary>
    [TestFixture]
    internal class ClusterCheckpointMetadataTests : TestBase
    {
        private string directory;

        [SetUp]
        public void Setup()
        {
            // MethodTestDir is already unique per test method; nesting further risks exceeding the path limit,
            // since the naming scheme appends a checkpoint directory and a token GUID beneath it.
            directory = TestUtils.MethodTestDir;
            TestUtils.DeleteDirectory(directory, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(directory, wait: true);
            TestUtils.OnTearDown();
        }

        private static DeviceLogCommitCheckpointManager CreateManager(string dir)
            => new ClusterFactory().CreateCheckpointManager(
                aofPhysicalSublogCount: 1,
                new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(dir),
                isMainStore: true);

        [Test]
        [Category("ClusterCheckpoint")]
        public void ClusterLogCheckpointMetadataRejectsInvalidLength()
        {
            // An empty commit writes a zero length prefix, which is what a truncated or partially written metadata
            // file also produces. Slicing by it would hand back empty metadata as though the checkpoint were valid.
            using var manager = CreateManager(directory);

            var token = Guid.NewGuid();
            manager.CommitLogCheckpointMetadata(token, []);

            var ex = Assert.Throws<TsavoriteException>(() => manager.GetLogCheckpointMetadata(token));
            StringAssert.Contains("truncated or corrupt", ex.Message);
            StringAssert.Contains(token.ToString(), ex.Message);
        }

        [Test]
        [Category("ClusterCheckpoint")]
        public void ClusterLogCheckpointMetadataReleasesDeviceOnInvalidLength()
        {
            // The validation throws between opening the metadata device and the end of the read, so the device has to
            // be released on the failure path. An undisposed device keeps the file open, which on Windows blocks the
            // delete below; reading the same token twice would also fail on the second open.
            using var manager = CreateManager(directory);

            var token = Guid.NewGuid();
            manager.CommitLogCheckpointMetadata(token, []);

            _ = Assert.Throws<TsavoriteException>(() => manager.GetLogCheckpointMetadata(token));
            _ = Assert.Throws<TsavoriteException>(() => manager.GetLogCheckpointMetadata(token));

            var metadataFile = Directory.GetFiles(directory, "info.dat*", SearchOption.AllDirectories);
            CollectionAssert.IsNotEmpty(metadataFile, "the metadata file under test was not created");
            Assert.DoesNotThrow(() => File.Delete(metadataFile[0]),
                "the metadata file is still open, so the failing read leaked its device");
        }

        [Test]
        [Category("ClusterCheckpoint")]
        public void ClusterLogCheckpointMetadataRoundTripsValidMetadata()
        {
            // Guards the validation against rejecting healthy metadata: the cluster override converts what it reads
            // into HybridLogRecoveryInfo, so a length check that was too strict would surface here rather than above.
            using var manager = CreateManager(directory);

            var token = Guid.NewGuid();
            var info = new HybridLogRecoveryInfo();
            info.Initialize(token, _version: 1);
            manager.CommitLogCheckpointMetadata(token, info.ToByteArray());

            byte[] roundTripped = null;
            Assert.DoesNotThrow(() => roundTripped = manager.GetLogCheckpointMetadata(token));
            CollectionAssert.IsNotEmpty(roundTripped, "valid metadata must survive the length validation");
        }
    }
}