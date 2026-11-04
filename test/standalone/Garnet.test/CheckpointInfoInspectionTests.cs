// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class CheckpointInfoInspectionTests : TestBase
    {
        private GarnetServer server;
        private ConnectionMultiplexer redis;
        private ILoggerFactory loggerFactory;
        private CapturingLogger logger;
        private InspectionDeviceFactoryCreator factory;
        private DefaultCheckpointNamingScheme scheme;
        private Guid token;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            logger = new CapturingLogger();
            loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Debug).AddProvider(new CapturingLoggerProvider(logger)));
            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir, logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint, enableCluster: true, enableAOF: true, disableStorageTier: true);
            factory = new InspectionDeviceFactoryCreator();
            options.DeviceFactoryCreator = factory;
            server = new GarnetServer(options, loggerFactory);
            server.Start();
            redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();
            ClassicAssert.AreEqual("OK", db.Execute("CLUSTER", "ADDSLOTSRANGE", 0, 16383).ToString());
            ClassicAssert.IsTrue(db.StringSet("checkpoint-key", "value"));
            ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString());
            ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString());

            scheme = new DefaultCheckpointNamingScheme(options.GetStoreCheckpointDirectory(0));
            token = Guid.Parse(Path.GetFileName(Directory.GetDirectories(Path.Combine(scheme.BaseName, scheme.LogCheckpointBasePath)).Single()));
            factory.DeviceOpens = 0;
        }

        [TearDown]
        public void TearDown()
        {
            redis?.Dispose();
            server?.Dispose();
            loggerFactory?.Dispose();
            TestUtils.OnTearDown();
        }

        private string Inspect(string section = "CINFO")
        {
            var response = section.Length == 0 ? redis.GetDatabase().Execute("INFO") : redis.GetDatabase().Execute("INFO", section);
            ClassicAssert.AreEqual(0, factory.DeviceOpens, "INFO must not use a create-capable device, including for the cookie");
            return response.ToString().Split('\n').Single(line => line.StartsWith("disk_checkpoint_entry:", StringComparison.Ordinal)).Trim();
        }

        private void AssertNoInspectionWarnings(int logStart)
            => ClassicAssert.IsEmpty(logger.Entries.Skip(logStart).Where(entry => entry.Level >= LogLevel.Warning &&
                entry.Message.Contains("checkpoint", StringComparison.OrdinalIgnoreCase)));

        [TestCase("CINFO")]
        [TestCase("ALL")]
        [TestCase("")]
        public void InfoReadsCheckpointWithoutCreatingDevices(string section)
        {
            StringAssert.Contains($"storeHlogToken={token}", Inspect(section));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void InfoSkipsCheckpointsDeletedBeforeReading(bool deleteAll)
        {
            var olderToken = Guid.NewGuid();
            using (var manager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactoryCreator(), scheme, removeOutdated: false))
            {
                using var reader = new StreamReader(new MemoryStream(manager.ReadLogCheckpointMetadata(token)));
                var info = new HybridLogRecoveryInfo();
                info.Initialize(reader);
                info.guid = olderToken;
                --info.version;
                --info.nextVersion;
                manager.CommitLogCheckpointMetadata(olderToken, info.ToByteArray());
            }

            factory.BeforeRead = (baseName, file) =>
            {
                if (file.directoryName.StartsWith(scheme.LogCheckpointBasePath, StringComparison.Ordinal) &&
                    (deleteAll || file.directoryName == scheme.LogCheckpointMetadata(token).directoryName))
                    Directory.Delete(Path.Combine(baseName, file.directoryName), recursive: true);
            };
            var logStart = logger.Entries.Count;

            var diskInfo = Inspect();

            if (deleteAll)
                ClassicAssert.AreEqual("disk_checkpoint_entry:(empty)", diskInfo);
            else
                StringAssert.Contains($"storeHlogToken={olderToken}", diskInfo);
            ClassicAssert.IsFalse(Directory.Exists(Path.Combine(scheme.BaseName, scheme.LogCheckpointBase(token).directoryName)));
            AssertNoInspectionWarnings(logStart);
            ClassicAssert.IsTrue(logger.Entries.Skip(logStart).Any(entry => entry.Level == LogLevel.Debug &&
                entry.Message.Contains("disappeared during inspection", StringComparison.Ordinal)));
        }

        [Test]
        public void InfoUsesCapturedMetadataWhenCheckpointIsDeletedAfterOpen()
        {
            var logReads = 0;
            factory.AfterOpen = (baseName, file) =>
            {
                if (file.directoryName != scheme.LogCheckpointMetadata(token).directoryName)
                    return;
                ++logReads;
                Directory.Delete(Path.Combine(baseName, file.directoryName), recursive: true);
            };
            var logStart = logger.Entries.Count;

            StringAssert.Contains($"storeHlogToken={token}", Inspect());

            ClassicAssert.AreEqual(1, logReads, "The selected checkpoint and its cookie must use the same captured metadata");
            ClassicAssert.IsFalse(Directory.Exists(Path.Combine(scheme.BaseName, scheme.LogCheckpointBase(token).directoryName)));
            AssertNoInspectionWarnings(logStart);

            factory.AfterOpen = null;
            ClassicAssert.AreEqual("disk_checkpoint_entry:(empty)", Inspect(), "The next inspection must not reuse stale disk metadata");
        }

        [Test]
        public void InfoSkipsIndexDeletedBeforeReading()
        {
            factory.BeforeRead = (baseName, file) =>
            {
                if (file.directoryName.StartsWith(scheme.IndexCheckpointBasePath, StringComparison.Ordinal))
                    Directory.Delete(Path.Combine(baseName, file.directoryName), recursive: true);
            };
            var logStart = logger.Entries.Count;

            var diskInfo = Inspect();

            StringAssert.Contains($"storeHlogToken={token}", diskInfo);
            StringAssert.Contains($"storeIndexToken={Guid.Empty}", diskInfo);
            ClassicAssert.IsEmpty(Directory.GetDirectories(Path.Combine(scheme.BaseName, scheme.IndexCheckpointBasePath)));
            AssertNoInspectionWarnings(logStart);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void InfoReportsUnreadableMetadata(bool accessDenied)
        {
            if (accessDenied)
                factory.BeforeRead = (_, _) => throw new UnauthorizedAccessException("Injected metadata read failure");
            else
            {
                var file = scheme.LogCheckpointMetadata(token);
                File.WriteAllBytes(Path.Combine(scheme.BaseName, file.directoryName, file.fileName + ".0"), []);
            }
            var logStart = logger.Entries.Count;

            ClassicAssert.AreEqual("disk_checkpoint_entry:(empty)", Inspect());

            ClassicAssert.IsTrue(logger.Entries.Skip(logStart).Any(entry => entry.Level == LogLevel.Warning &&
                entry.Message.Contains("Skipping unreadable HybridLog checkpoint", StringComparison.Ordinal)));
            ClassicAssert.IsFalse(logger.Entries.Skip(logStart).Any(entry =>
                entry.Message.Contains("disappeared during inspection", StringComparison.Ordinal)));
        }

        private sealed class InspectionDeviceFactoryCreator : INamedDeviceFactoryCreator
        {
            public Action<string, FileDescriptor> BeforeRead;
            public Action<string, FileDescriptor> AfterOpen;
            public int DeviceOpens;

            public INamedDeviceFactory Create(string baseName)
                => new InspectionDeviceFactory(baseName, new LocalStorageNamedDeviceFactory(baseName: baseName), this);
        }

        private sealed class InspectionDeviceFactory(string baseName, INamedDeviceFactory underlying, InspectionDeviceFactoryCreator owner)
            : INamedDeviceFactory
        {
            public IDevice Get(FileDescriptor fileInfo)
            {
                Interlocked.Increment(ref owner.DeviceOpens);
                return underlying.Get(fileInfo);
            }

            public Stream OpenRead(FileDescriptor fileInfo)
            {
                owner.BeforeRead?.Invoke(baseName, fileInfo);
                var stream = underlying.OpenRead(fileInfo);
                try
                {
                    owner.AfterOpen?.Invoke(baseName, fileInfo);
                    return stream;
                }
                catch
                {
                    stream.Dispose();
                    throw;
                }
            }

            public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);
            public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);
            public uint GetSectorSize(FileDescriptor fileInfo) => underlying.GetSectorSize(fileInfo);
        }
    }
}
