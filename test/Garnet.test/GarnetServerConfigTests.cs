// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using CommandLine;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NUnit.Framework;
using NUnit.Framework.Legacy;
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
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.EmbeddedResource, null, Assembly.GetExecutingAssembly());
            using (var stream = streamProvider.Read(ServerSettingsManager.DefaultOptionsEmbeddedFileName))
            {
                using (var streamReader = new StreamReader(stream))
                {
                    json = streamReader.ReadToEnd();
                }
            }
            // Deserialize default.conf to get all defined default options
            Dictionary<string, string> jsonSettings = [];
            try
            {
                jsonSettings = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
            }
            catch (Exception e)
            {
                Assert.Fail($"Unable to deserialize JSON from {ServerSettingsManager.DefaultOptionsEmbeddedFileName}. Exception: {e.Message}{Environment.NewLine}{e.StackTrace}");
            }

            // Check that all properties in Options have a default value in defaults.conf
            ClassicAssert.IsNotNull(jsonSettings);
            foreach (var property in typeof(Options).GetProperties().Where(pi =>
                         pi.GetCustomAttribute<OptionAttribute>() != null &&
                         pi.GetCustomAttribute<System.Text.Json.Serialization.JsonIgnoreAttribute>() == null))
            {
                ClassicAssert.Contains(property.Name, jsonSettings.Keys);
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
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("32m", options.PageSize);
            ClassicAssert.AreEqual("16g", options.MemorySize);

            // No import path, include command line args, export to file
            // Check values from command line override values from defaults.conf
            static string GetFullExtensionBinPath(string testProjectName) => Path.GetFullPath(testProjectName, TestUtils.RootTestsProjectPath);
            var args = new string[] { "--config-export-path", configPath, "-p", "4m", "-m", "128m", "-s", "2g", "--recover", "--port", "53", "--reviv-obj-bin-record-count", "2", "--reviv-fraction", "0.5", "--extension-bin-paths", $"{GetFullExtensionBinPath("Garnet.test")},{GetFullExtensionBinPath("Garnet.test.cluster")}", "--loadmodulecs", $"{Assembly.GetExecutingAssembly().Location}" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("4m", options.PageSize);
            ClassicAssert.AreEqual("128m", options.MemorySize);
            ClassicAssert.AreEqual("2g", options.SegmentSize);
            ClassicAssert.AreEqual(53, options.Port);
            ClassicAssert.AreEqual(2, options.RevivObjBinRecordCount);
            ClassicAssert.AreEqual(0.5, options.RevivifiableFraction);
            ClassicAssert.IsTrue(options.Recover);
            ClassicAssert.IsTrue(File.Exists(configPath));
            ClassicAssert.AreEqual(2, options.ExtensionBinPaths.Count());
            ClassicAssert.AreEqual(1, options.LoadModuleCS.Count());
            ClassicAssert.AreEqual(Assembly.GetExecutingAssembly().Location, options.LoadModuleCS.First());

            // Import from previous export command, no command line args
            // Check values from import path override values from default.conf
            args = ["--config-import-path", configPath];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.IsTrue(options.PageSize == "4m");
            ClassicAssert.IsTrue(options.MemorySize == "128m");

            // Import from previous export command, include command line args, export to file
            // Check values from import path override values from default.conf, and values from command line override values from default.conf and import path
            args = ["--config-import-path", configPath, "-p", "12m", "-s", "1g", "--recover", "false", "--port", "0", "--no-obj", "--aof"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("12m", options.PageSize);
            ClassicAssert.AreEqual("128m", options.MemorySize);
            ClassicAssert.AreEqual("1g", options.SegmentSize);
            ClassicAssert.AreEqual(0, options.Port);
            ClassicAssert.IsFalse(options.Recover);
            ClassicAssert.IsTrue(options.DisableObjects);
            ClassicAssert.IsTrue(options.EnableAOF);

            // No import path, include command line args
            // Check that all invalid options flagged
            args = ["--bind", "1.1.1.257", "-m", "12mg", "--port", "-1", "--mutable-percent", "101", "--acl-file", "nx_dir/nx_file.txt", "--tls", "--reviv-fraction", "1.1", "--cert-file-name", "testcert.crt"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsFalse(parseSuccessful);
            ClassicAssert.IsNull(options);
            ClassicAssert.AreEqual(7, invalidOptions.Count);
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.Address)));
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.MemorySize)));
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.Port)));
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.MutablePercent)));
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.AclFile)));
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.RevivifiableFraction)));
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.CertFileName)));

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
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("127.0.0.1", options.Address);
            ClassicAssert.AreEqual(6379, options.Port);
            ClassicAssert.AreEqual("20gb", options.MemorySize);
            ClassicAssert.AreEqual("./garnet-log", options.FileLogger);
            ClassicAssert.AreEqual("./", options.CheckpointDir);
            ClassicAssert.IsTrue(options.EnableCluster);
            ClassicAssert.AreEqual("foobared", options.Password);
            ClassicAssert.AreEqual(4, options.ThreadPoolMinThreads);
            ClassicAssert.AreEqual(15000, options.ClusterTimeout);
            ClassicAssert.AreEqual(LogLevel.Information, options.LogLevel);
            ClassicAssert.AreEqual(5, options.ReplicaSyncDelayMs);
            ClassicAssert.IsTrue(options.EnableTLS);
            ClassicAssert.IsTrue(options.ClientCertificateRequired);
            ClassicAssert.AreEqual("testcert.pfx", options.CertFileName);
            ClassicAssert.AreEqual("placeholder", options.CertPassword);

            // Import from redis.conf file, include command line args
            // Check values from import path override values from default.conf, and values from command line override values from default.conf and import path
            args = ["--config-import-path", redisConfigPath, "--config-import-format", "RedisConf", "--config-export-path", garnetConfigPath, "-p", "12m", "--tls", "false", "--minthreads", "6", "--client-certificate-required", "true"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("12m", options.PageSize);
            ClassicAssert.AreEqual("20gb", options.MemorySize);
            ClassicAssert.AreEqual("1g", options.SegmentSize);
            ClassicAssert.AreEqual(6, options.ThreadPoolMinThreads);
            ClassicAssert.AreEqual(5, options.ReplicaSyncDelayMs);
            ClassicAssert.IsFalse(options.EnableTLS);
            ClassicAssert.IsTrue(options.ClientCertificateRequired);
            ClassicAssert.IsTrue(File.Exists(garnetConfigPath));

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [Test]
        public void ImportExportConfigAzure()
        {
            if (!TestUtils.IsRunningAzureTests)
            {
                Assert.Ignore("Azure tests are disabled.");
            }

            var AzureTestDirectory = $"{TestContext.CurrentContext.Test.MethodName.ToLowerInvariant()}";
            var configPath = $"{AzureTestDirectory}/test1.config";
            var AzureEmulatedStorageString = "UseDevelopmentStorage=true;";

            // Delete blob if exists
            var deviceFactory = new AzureStorageNamedDeviceFactory(AzureEmulatedStorageString, default);
            deviceFactory.Initialize(AzureTestDirectory);
            deviceFactory.Delete(new FileDescriptor { directoryName = "" });

            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(null, out var options, out var invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            ClassicAssert.IsTrue(options.PageSize == "32m");
            ClassicAssert.IsTrue(options.MemorySize == "16g");

            var args = new string[] { "--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-export", "true", "--config-export-path", configPath, "-p", "4m", "-m", "128m" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            ClassicAssert.IsTrue(options.PageSize == "4m");
            ClassicAssert.IsTrue(options.MemorySize == "128m");

            args = ["--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-import", "true", "--config-import-path", configPath];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            ClassicAssert.IsTrue(options.PageSize == "4m");
            ClassicAssert.IsTrue(options.MemorySize == "128m");

            args = ["--use-azure-storage", "--storage-string", AzureEmulatedStorageString];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            ClassicAssert.AreEqual(AzureEmulatedStorageString, options.AzureStorageConnectionString);

            // Delete blob
            deviceFactory.Initialize(AzureTestDirectory);
            deviceFactory.Delete(new FileDescriptor { directoryName = "" });
        }

        [Test]
        public void AzureStorageConfiguration()
        {
            // missing both storage-string and managed-identity
            var args = new string[] { "--use-azure-storage" };
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            Assert.Throws<InvalidAzureConfiguration>(() => options.GetServerOptions());

            // valid storage-string
            args = ["--use-azure-storage", "--storage-string", "UseDevelopmentStorage=true;"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            Assert.DoesNotThrow(() => options.GetServerOptions());

            // insecure service-uri
            args = ["--use-azure-storage", "--storage-service-uri", "http://demo.blob.core.windows.net"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsFalse(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 1);
            ClassicAssert.AreEqual(invalidOptions[0], nameof(Options.AzureStorageServiceUri));

            // secure service-uri but missing managed-identity
            args = ["--use-azure-storage", "--storage-service-uri", "https://demo.blob.core.windows.net"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            Assert.Throws<InvalidAzureConfiguration>(() => options.GetServerOptions());

            // secure service-uri with managed-identity
            args = ["--use-azure-storage", "--storage-service-uri", "https://demo.blob.core.windows.net", "--storage-managed-identity", "demo"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            var serverOptions = options.GetServerOptions();
            Assert.DoesNotThrow(() => options.GetServerOptions());

            // both storage-string and managed-identity
            args = ["--use-azure-storage", "--storage-string", "UseDevelopmentStorage", "--storage-managed-identity", "demo", "--storage-service-uri", "https://demo.blob.core.windows.net"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.IsEmpty(invalidOptions);
            Assert.Throws<InvalidAzureConfiguration>(() => options.GetServerOptions());
        }
    }
}