// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using CommandLine;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.devices;

namespace Garnet.test
{

    [TestFixture, NonParallelizable]

    public class GarnetServerConfigTests
    {
        [SetUp]
        public void Setup()
        { }

        [TearDown]
        public void TearDown()
        { }

        [Test]
        public void DefaultConfigurationOptionsCoverage()
        {
            string json;
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource);
            using (var stream = streamProvider.Read(ServerSettingsManager.DefaultOptionsEmbeddedFileName))
            {
                using (var streamReader = new StreamReader(stream))
                {
                    json = streamReader.ReadToEnd();
                }
            }
            // Deserialize default.conf to get all defined default options
            Dictionary<string, string> jsonSettings = new Dictionary<string, string>();
            try
            {
                jsonSettings = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
            }
            catch (Exception e)
            {
                Assert.Fail($"Unable to deserialize JSON from {ServerSettingsManager.DefaultOptionsEmbeddedFileName}. Exception: {e.Message}{Environment.NewLine}{e.StackTrace}");
            }

            // Check that all properties in Options have a default value in defaults.conf
            Assert.IsNotNull(jsonSettings);
            foreach (var property in typeof(Options).GetProperties().Where(pi =>
                         pi.GetCustomAttribute<OptionAttribute>() != null &&
                         pi.GetCustomAttribute<System.Text.Json.Serialization.JsonIgnoreAttribute>() == null))
            {
                Assert.Contains(property.Name, jsonSettings.Keys);
            }
        }

        [Test]
        public void ImportExportConfigLocal()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            string dir = TestUtils.MethodTestDir;
            string configPath = $"{dir}\\test1.conf";

            // No import path, no command line args
            // Check values match those on defaults.conf
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(null, out var options, out var invalidOptions);
            Assert.IsTrue(parseSuccessful);
            Assert.AreEqual(invalidOptions.Count, 0);
            Assert.AreEqual("32m", options.PageSize);
            Assert.AreEqual("16g", options.MemorySize);

            // No import path, include command line args, export to file
            // Check values from command line override values from defaults.conf
            var args = new string[] { "--config-export-path", configPath, "-p", "4m", "-m", "8g", "-s", "2g", "--recover", "--port", "53", "--reviv-obj-bin-record-count", "2", "--reviv-fraction", "0.5", "--extension-bin-paths", "../../../../../../test/Garnet.test,../../../../../../test/Garnet.test.cluster" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            Assert.IsTrue(parseSuccessful);
            Assert.AreEqual(invalidOptions.Count, 0);
            Assert.AreEqual("4m", options.PageSize);
            Assert.AreEqual("8g", options.MemorySize);
            Assert.AreEqual("2g", options.SegmentSize);
            Assert.AreEqual(53, options.Port);
            Assert.AreEqual(2, options.RevivObjBinRecordCount);
            Assert.AreEqual(0.5, options.RevivifiableFraction);
            Assert.IsTrue(options.Recover);
            Assert.IsTrue(File.Exists(configPath));
            Assert.AreEqual(2, options.ExtensionBinPaths.Count());

            // Import from previous export command, no command line args
            // Check values from import path override values from default.conf
            args = new string[] { "--config-import-path", configPath };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            Assert.IsTrue(parseSuccessful);
            Assert.AreEqual(invalidOptions.Count, 0);
            Assert.IsTrue(options.PageSize == "4m");
            Assert.IsTrue(options.MemorySize == "8g");

            // Import from previous export command, include command line args, export to file
            // Check values from import path override values from default.conf, and values from command line override values from default.conf and import path
            args = new string[] { "--config-import-path", configPath, "-p", "12m", "-s", "1g", "--recover", "false", "--port", "0", "--no-obj", "--aof" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            Assert.IsTrue(parseSuccessful);
            Assert.AreEqual(invalidOptions.Count, 0);
            Assert.AreEqual("12m", options.PageSize);
            Assert.AreEqual("8g", options.MemorySize);
            Assert.AreEqual("1g", options.SegmentSize);
            Assert.AreEqual(0, options.Port);
            Assert.IsFalse(options.Recover);
            Assert.IsTrue(options.DisableObjects);
            Assert.IsTrue(options.EnableAOF);

            // No import path, include command line args
            // Check that all invalid options flagged
            args = new string[] { "--bind", "1.1.1.257", "-m", "12mg", "--port", "-1", "--mutable-percent", "101", "--acl-file", "nx_dir/nx_file.txt", "--tls", "--reviv-fraction", "1.1", "--cert-file-name", "testcert.crt" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            Assert.IsFalse(parseSuccessful);
            Assert.IsNull(options);
            Assert.AreEqual(7, invalidOptions.Count);
            Assert.IsTrue(invalidOptions.Contains(nameof(Options.Address)));
            Assert.IsTrue(invalidOptions.Contains(nameof(Options.MemorySize)));
            Assert.IsTrue(invalidOptions.Contains(nameof(Options.Port)));
            Assert.IsTrue(invalidOptions.Contains(nameof(Options.MutablePercent)));
            Assert.IsTrue(invalidOptions.Contains(nameof(Options.AclFile)));
            Assert.IsTrue(invalidOptions.Contains(nameof(Options.RevivifiableFraction)));
            Assert.IsTrue(invalidOptions.Contains(nameof(Options.CertFileName)));

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void ImportExportRedisConfigLocal()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            string dir = TestUtils.MethodTestDir;
            string garnetConfigPath = $"{dir}\\test1.conf";
            string redisConfigPath = $"redis.conf";

            // Import from redis.conf file, no command line args
            // Check values from import path override values from default.conf
            var args = new[] { "--config-import-path", redisConfigPath, "--config-import-format", "RedisConf" };
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions);
            Assert.IsTrue(parseSuccessful);
            Assert.AreEqual(invalidOptions.Count, 0);
            Assert.AreEqual("127.0.0.1", options.Address);
            Assert.AreEqual(6379, options.Port);
            Assert.AreEqual("20gb", options.MemorySize);
            Assert.AreEqual("./garnet-log", options.FileLogger);
            Assert.AreEqual("./", options.CheckpointDir);
            Assert.IsTrue(options.EnableCluster);
            Assert.AreEqual("foobared", options.Password);
            Assert.AreEqual(4, options.ThreadPoolMinThreads);
            Assert.AreEqual(15000, options.ClusterTimeout);
            Assert.AreEqual(LogLevel.Information, options.LogLevel);
            Assert.AreEqual(5, options.ReplicaSyncDelayMs);
            Assert.IsTrue(options.EnableTLS);
            Assert.IsTrue(options.ClientCertificateRequired);
            Assert.AreEqual("testcert.pfx", options.CertFileName);
            Assert.AreEqual("placeholder", options.CertPassword);

            // Import from redis.conf file, include command line args
            // Check values from import path override values from default.conf, and values from command line override values from default.conf and import path
            args = new[] { "--config-import-path", redisConfigPath, "--config-import-format", "RedisConf", "--config-export-path", garnetConfigPath, "-p", "12m", "--tls", "false", "--minthreads", "6", "--client-certificate-required", "true" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            Assert.IsTrue(parseSuccessful);
            Assert.AreEqual(invalidOptions.Count, 0);
            Assert.AreEqual("12m", options.PageSize);
            Assert.AreEqual("20gb", options.MemorySize);
            Assert.AreEqual("1g", options.SegmentSize);
            Assert.AreEqual(6, options.ThreadPoolMinThreads);
            Assert.AreEqual(5, options.ReplicaSyncDelayMs);
            Assert.IsFalse(options.EnableTLS);
            Assert.IsTrue(options.ClientCertificateRequired);
            Assert.IsTrue(File.Exists(garnetConfigPath));

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [Test]
        public void ImportExportConfigAzure()
        {
            var AzureTestDirectory = $"{TestContext.CurrentContext.Test.MethodName.ToLowerInvariant()}";
            var configPath = $"{AzureTestDirectory}/test1.config";
            var AzureEmulatedStorageString = "UseDevelopmentStorage=true;";

            if (TestUtils.IsRunningAzureTests)
            {
                // Delete blob if exists
                var deviceFactory = new AzureStorageNamedDeviceFactory(AzureEmulatedStorageString, default);
                deviceFactory.Initialize(AzureTestDirectory);
                deviceFactory.Delete(new FileDescriptor { directoryName = "" });

                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(null, out var options, out var invalidOptions);
                Assert.IsTrue(parseSuccessful);
                Assert.AreEqual(invalidOptions.Count, 0);
                Assert.IsTrue(options.PageSize == "32m");
                Assert.IsTrue(options.MemorySize == "16g");

                var args = new string[] { "--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-export", "true", "--config-export-path", configPath, "-p", "4m", "-m", "8g" };
                parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
                Assert.IsTrue(parseSuccessful);
                Assert.AreEqual(invalidOptions.Count, 0);
                Assert.IsTrue(options.PageSize == "4m");
                Assert.IsTrue(options.MemorySize == "8g");

                args = new string[] { "--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-import", "true", "--config-import-path", configPath };
                parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
                Assert.IsTrue(parseSuccessful);
                Assert.AreEqual(invalidOptions.Count, 0);
                Assert.IsTrue(options.PageSize == "4m");
                Assert.IsTrue(options.MemorySize == "8g");

                // Delete blob
                deviceFactory.Initialize(AzureTestDirectory);
                deviceFactory.Delete(new FileDescriptor { directoryName = "" });
            }
        }
    }
}