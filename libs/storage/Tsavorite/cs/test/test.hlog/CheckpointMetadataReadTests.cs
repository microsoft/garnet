// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture(DeviceType.Native)]
    [TestFixture(DeviceType.RandomAccess)]
    [TestFixture(DeviceType.FileStream)]
    internal class CheckpointMetadataReadTests(DeviceType deviceType) : TestBase
    {
        public enum MetadataKind { Index, Log }

        private string directory;
        private DefaultCheckpointNamingScheme scheme;

        [SetUp]
        public void Setup()
        {
            directory = TestUtils.MethodTestDir;
            TestUtils.DeleteDirectory(directory, wait: true);
            scheme = new DefaultCheckpointNamingScheme(directory);
        }

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown(waitForDelete: true);

        private DeviceLogCommitCheckpointManager CreateManager()
            => new(new LocalStorageNamedDeviceFactoryCreator(deviceType: deviceType), scheme, removeOutdated: false);

        private static void WriteMetadata(DeviceLogCommitCheckpointManager manager, MetadataKind kind, Guid token, byte[] metadata)
        {
            switch (kind)
            {
                case MetadataKind.Index:
                    manager.CommitIndexCheckpoint(token, metadata);
                    break;
                case MetadataKind.Log:
                    manager.CommitLogCheckpointMetadata(token, metadata);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind));
            }
        }

        private static byte[] ReadMetadata(DeviceLogCommitCheckpointManager manager, MetadataKind kind, Guid token)
            => kind switch
            {
                MetadataKind.Index => manager.ReadIndexCheckpointMetadata(token),
                MetadataKind.Log => manager.ReadLogCheckpointMetadata(token),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };

        private string MetadataPath(MetadataKind kind, Guid token)
        {
            var descriptor = kind switch
            {
                MetadataKind.Index => scheme.IndexCheckpointMetadata(token),
                MetadataKind.Log => scheme.LogCheckpointMetadata(token),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            return Path.Combine(directory, descriptor.directoryName, descriptor.fileName + ".0");
        }

        [Test]
        public void MetadataRoundTripsWithoutModifyingFiles([Values] MetadataKind kind)
        {
            using var manager = CreateManager();
            var token = Guid.NewGuid();
            var metadata = new byte[8193];
            new Random(0).NextBytes(metadata);
            WriteMetadata(manager, kind, token, metadata);
            var path = MetadataPath(kind, token);
            var before = File.ReadAllBytes(path);

            CollectionAssert.AreEqual(metadata, ReadMetadata(manager, kind, token));
            CollectionAssert.AreEqual(before, File.ReadAllBytes(path));
        }

        [Test]
        public void MissingMetadataDoesNotCreateDirectories([Values] MetadataKind kind)
        {
            using var manager = CreateManager();

            _ = Assert.Catch<IOException>(() => ReadMetadata(manager, kind, Guid.NewGuid()));

            ClassicAssert.IsFalse(Directory.Exists(directory), "A failed metadata read must not create even the base directory");
        }

        [Test]
        public void MetadataDeletedAfterEnumerationIsNotRecreated([Values(MetadataKind.Index, MetadataKind.Log)] MetadataKind kind)
        {
            using var manager = CreateManager();
            var oldToken = Guid.NewGuid();
            var latestToken = Guid.NewGuid();
            byte[] metadata = [1, 2, 3, 4];
            WriteMetadata(manager, kind, oldToken, metadata);
            WriteMetadata(manager, kind, latestToken, metadata);

            var tokens = kind == MetadataKind.Log ? manager.GetLogCheckpointTokens() : manager.GetIndexCheckpointTokens();
            CollectionAssert.Contains(tokens.ToArray(), oldToken);
            manager.Purge(oldToken);

            _ = Assert.Catch<IOException>(() => ReadMetadata(manager, kind, oldToken));

            ClassicAssert.IsFalse(Directory.Exists(Path.GetDirectoryName(MetadataPath(kind, oldToken))),
                "A token deleted after discovery must stay deleted when its metadata is read");
            CollectionAssert.AreEqual(metadata, ReadMetadata(manager, kind, latestToken));
        }

        [Test]
        public void MissingMetadataFileDoesNotCreateSegment([Values] MetadataKind kind)
        {
            using var manager = CreateManager();
            var token = Guid.NewGuid();
            var path = MetadataPath(kind, token);
            Directory.CreateDirectory(Path.GetDirectoryName(path));

            _ = Assert.Throws<FileNotFoundException>(() => ReadMetadata(manager, kind, token));

            ClassicAssert.IsEmpty(Directory.GetFiles(directory, "*", SearchOption.AllDirectories));
        }

        [TestCase(0)]
        [TestCase(3)]
        [TestCase(7)]
        public void TruncatedMetadataIsReported(int fileLength)
        {
            using var manager = CreateManager();
            var token = Guid.NewGuid();
            WriteMetadata(manager, MetadataKind.Log, token, [1, 2, 3, 4]);
            var path = MetadataPath(MetadataKind.Log, token);
            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Write))
                stream.SetLength(fileLength);

            var ex = Assert.Throws<TsavoriteException>(() => manager.ReadLogCheckpointMetadata(token));

            StringAssert.Contains(token.ToString(), ex.Message);
            ClassicAssert.IsInstanceOf<EndOfStreamException>(ex.InnerException);
            ClassicAssert.AreEqual(fileLength, new FileInfo(path).Length);
        }

        [Test]
        public void LegacyMetadataReadsKeepSectorPadding()
        {
            using var manager = CreateManager();
            var token = Guid.NewGuid();
            byte[] metadata = [1, 2, 3, 4];
            manager.CommitIndexCheckpoint(token, metadata);
            manager.Commit(0, 100, metadata, 1, forceWriteMetadata: true);

            var index = manager.GetIndexCheckpointMetadata(token);
            var commit = manager.GetCommitMetadata(1);
            ClassicAssert.Greater(index.Length, metadata.Length);
            ClassicAssert.Greater(commit.Length, metadata.Length);
            CollectionAssert.AreEqual(metadata, index.AsSpan(0, metadata.Length).ToArray());
            CollectionAssert.AreEqual(metadata, commit.AsSpan(0, metadata.Length).ToArray());
            CollectionAssert.AreEqual(metadata, manager.ReadIndexCheckpointMetadata(token));
        }

        [Test]
        public void OpenReadAllowsDeletionWithoutDeletingOnClose()
        {
            var factory = new LocalStorageNamedDeviceFactory(baseName: directory, deviceType: deviceType, deleteOnClose: true);
            var descriptor = scheme.LogCheckpointMetadata(Guid.NewGuid());
            var path = Path.Combine(directory, descriptor.directoryName, descriptor.fileName + ".0");
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            byte[] metadata = [1, 2, 3, 4];
            File.WriteAllBytes(path, metadata);

            using (var stream = factory.OpenRead(descriptor))
            {
                ClassicAssert.IsFalse(stream.CanWrite);
                File.Delete(path);
                var result = new byte[metadata.Length];
                stream.ReadExactly(result);
                CollectionAssert.AreEqual(metadata, result);
            }
            ClassicAssert.IsFalse(File.Exists(path));

            File.WriteAllBytes(path, metadata);
            factory.OpenRead(descriptor).Dispose();
            CollectionAssert.AreEqual(metadata, File.ReadAllBytes(path), "Read streams must not inherit delete-on-close from write devices");
        }
    }
}
