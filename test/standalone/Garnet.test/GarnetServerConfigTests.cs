// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using CommandLine;
using Garnet.common;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture, NonParallelizable]
    public class GarnetServerConfigTests : TestBase
    {
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
            Dictionary<string, object> jsonSettings = [];
            var jsonSerializerOptions = new JsonSerializerOptions
            {
                ReadCommentHandling = JsonCommentHandling.Skip,
                NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString,
                AllowTrailingCommas = true,
            };

            try
            {
                jsonSettings = JsonSerializer.Deserialize<Dictionary<string, object>>(json, jsonSerializerOptions);
            }
            catch (Exception e)
            {
                Assert.Fail($"Unable to deserialize JSON from {ServerSettingsManager.DefaultOptionsEmbeddedFileName}. Exception: {e.Message}{Environment.NewLine}{e.StackTrace}");
            }

            // Check that all properties in Options have a default value in defaults.conf
            ClassicAssert.IsNotNull(jsonSettings);
            foreach (var property in typeof(Options).GetProperties().Where(pi =>
                         pi.GetCustomAttribute<OptionAttribute>() != null &&
                         pi.GetCustomAttribute<JsonIgnoreAttribute>() == null))
            {
                ClassicAssert.Contains(property.Name, jsonSettings.Keys);
            }
        }

        [Test]
        public void OptionsDefaultAttributeUsage()
        {
            // Verify that there are no usages of the OptionsAttribute.Default property (all default values should be set in defaults.conf)
            // Note that this test will not fail if the user is setting the Default property to the type's default value (yet can still cause an issue if done).
            var propUsages = new List<string>();

            foreach (var prop in typeof(Options).GetProperties())
            {
                var ignoreAttr = prop.GetCustomAttributes(typeof(JsonIgnoreAttribute)).FirstOrDefault();
                if (ignoreAttr != null)
                    continue;

                var optionAttr = (OptionAttribute)prop.GetCustomAttributes(typeof(OptionAttribute)).FirstOrDefault();
                if (optionAttr == null)
                    continue;

                if (optionAttr.Default != default)
                {
                    propUsages.Add(prop.Name);
                }
            }

            ClassicAssert.IsEmpty(propUsages,
                $"Properties in {typeof(Options)} should not use {nameof(OptionAttribute)}.{nameof(OptionAttribute.Default)}. All default values should be specified in defaults.conf.");
        }

        [Test]
        public void RangeIndexPreviewRequiresMinimumAofPageSize()
        {
            // Range index preview needs an AOF page large enough for a migrated stream chunk (>= 512k).
            var args = new[] { "--enable-range-index-preview", "--aof-page-size", "256k" };
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful, "aof-page-size below 512k must be rejected when range index preview is enabled");

            // A page at the threshold is accepted.
            args = ["--enable-range-index-preview", "--aof-page-size", "512k"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful, "aof-page-size at 512k must be accepted when range index preview is enabled");

            // The constraint only applies when the preview is enabled.
            args = ["--aof-page-size", "256k"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful, "aof-page-size below 512k must be accepted when range index preview is disabled");
        }

        [Test]
        public void ImportExportConfigLocal()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            string dir = TestUtils.MethodTestDir;
            string configPath = $"{dir}\\test1.conf";

            // Help
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(["--help"], out var options, out var invalidOptions, out _, out var exitGracefully, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful);
            ClassicAssert.IsTrue(exitGracefully);

            // Version
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(["--version"], out options, out invalidOptions, out _, out exitGracefully, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful);
            ClassicAssert.IsTrue(exitGracefully);

            // No import path, no command line args
            // Check values match those on defaults.conf
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(null, out options, out invalidOptions, out var optionsJson, out exitGracefully, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("16m", options.PageSize);
            ClassicAssert.AreEqual("16g", options.LogMemorySize);
            var nonDefaultOptions = JsonSerializer.Deserialize<Dictionary<string, object>>(optionsJson);
            ClassicAssert.IsEmpty(nonDefaultOptions);

            // No import path, include command line args, export to file
            // Check values from command line override values from defaults.conf
            static string GetFullExtensionBinPath(string relativePath) => Path.GetFullPath(relativePath, TestUtils.RootTestsProjectPath);
            var binPaths = new[] { GetFullExtensionBinPath(Path.Combine("standalone", "Garnet.test")), GetFullExtensionBinPath(Path.Combine("cluster", "Garnet.test.cluster")) };
            var modules = new[] { Assembly.GetExecutingAssembly().Location };

            var args = new[] { "--config-export-path", configPath, "-p", "8m", "-m", "128m", "-s", "2g", "--index", "128m", "--recover", "--port", "53", "--reviv-fraction", "0.5", "--reviv-bin-record-counts", "1,2,3", "--extension-bin-paths", string.Join(',', binPaths), "--loadmodulecs", string.Join(',', modules) };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out optionsJson, out exitGracefully, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("8m", options.PageSize);
            ClassicAssert.AreEqual("128m", options.LogMemorySize);
            ClassicAssert.AreEqual("2g", options.SegmentSize);
            ClassicAssert.AreEqual(53, options.Port);
            ClassicAssert.AreEqual(0.5, options.RevivifiableFraction);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, options.RevivBinRecordCounts);
            ClassicAssert.IsTrue(options.Recover);
            ClassicAssert.IsTrue(File.Exists(configPath));
            CollectionAssert.AreEqual(binPaths, options.ExtensionBinPaths);
            CollectionAssert.AreEqual(modules, options.LoadModuleCS);

            // Validate non-default configuration options
            nonDefaultOptions = JsonSerializer.Deserialize<Dictionary<string, object>>(optionsJson);
            ClassicAssert.AreEqual(9, nonDefaultOptions.Count);
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.PageSize)));
            ClassicAssert.AreEqual("8m", ((JsonElement)nonDefaultOptions[nameof(Options.PageSize)]).GetString());
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.Port)));
            ClassicAssert.AreEqual(53, ((JsonElement)nonDefaultOptions[nameof(Options.Port)]).GetInt32());
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.RevivifiableFraction)));
            ClassicAssert.AreEqual(0.5, ((JsonElement)nonDefaultOptions[nameof(Options.RevivifiableFraction)]).GetDouble());
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.RevivBinRecordCounts)));
            ClassicAssert.AreEqual(new[] { 1, 2, 3 },
                ((JsonElement)nonDefaultOptions[nameof(Options.RevivBinRecordCounts)]).EnumerateArray()
                .Select(i => i.GetInt32()));
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.Recover)));
            ClassicAssert.AreEqual(true, ((JsonElement)nonDefaultOptions[nameof(Options.Recover)]).GetBoolean());
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.LoadModuleCS)));
            ClassicAssert.AreEqual(modules,
                ((JsonElement)nonDefaultOptions[nameof(Options.LoadModuleCS)]).EnumerateArray()
                .Select(m => m.GetString()));

            // Import from previous export command, no command line args
            // Check values from import path override values from default.conf
            args = ["--config-import-path", configPath];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out optionsJson, out exitGracefully, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.IsTrue(options.PageSize == "8m");
            ClassicAssert.IsTrue(options.LogMemorySize == "128m");
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, options.RevivBinRecordCounts);
            CollectionAssert.AreEqual(binPaths, options.ExtensionBinPaths);
            CollectionAssert.AreEqual(modules, options.LoadModuleCS);

            // Validate non-default configuration options
            nonDefaultOptions = JsonSerializer.Deserialize<Dictionary<string, object>>(optionsJson);
            ClassicAssert.AreEqual(9, nonDefaultOptions.Count);
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.PageSize)));
            ClassicAssert.AreEqual("8m", ((JsonElement)nonDefaultOptions[nameof(Options.PageSize)]).GetString());

            // Import from previous export command, include command line args, export to file
            // Check values from import path override values from default.conf, and values from command line override values from default.conf and import path
            binPaths = [GetFullExtensionBinPath(Path.Combine("standalone", "Garnet.test"))];
            args = ["--config-import-path", configPath, "-p", "12m", "-s", "1g", "--recover", "false", "--index", "256m", "--port", "0", "--no-obj", "--aof", "--reviv-bin-record-counts", "4,5", "--extension-bin-paths", string.Join(',', binPaths)];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out optionsJson, out exitGracefully, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("12m", options.PageSize);
            ClassicAssert.AreEqual("128m", options.LogMemorySize);
            ClassicAssert.AreEqual("1g", options.SegmentSize);
            ClassicAssert.AreEqual(0, options.Port);
            ClassicAssert.IsFalse(options.Recover);
            ClassicAssert.IsTrue(options.DisableObjects);
            ClassicAssert.IsTrue(options.EnableAOF);
            CollectionAssert.AreEqual(new[] { 4, 5 }, options.RevivBinRecordCounts);
            CollectionAssert.AreEqual(binPaths, options.ExtensionBinPaths);

            // Validate non-default configuration options
            nonDefaultOptions = JsonSerializer.Deserialize<Dictionary<string, object>>(optionsJson);
            ClassicAssert.AreEqual(10, nonDefaultOptions.Count);
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.PageSize)));
            ClassicAssert.AreEqual("12m", ((JsonElement)nonDefaultOptions[nameof(Options.PageSize)]).GetString());
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.Port)));
            ClassicAssert.AreEqual(0, ((JsonElement)nonDefaultOptions[nameof(Options.Port)]).GetInt32());
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.IndexMemorySize)));
            ClassicAssert.AreEqual("256m", ((JsonElement)nonDefaultOptions[nameof(Options.IndexMemorySize)]).GetString());
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.RevivBinRecordCounts)));
            ClassicAssert.AreEqual(new[] { 4, 5 },
                ((JsonElement)nonDefaultOptions[nameof(Options.RevivBinRecordCounts)]).EnumerateArray()
                .Select(i => i.GetInt32()));
            ClassicAssert.IsFalse(nonDefaultOptions.ContainsKey(nameof(Options.Recover)));
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.DisableObjects)));
            ClassicAssert.IsTrue(((JsonElement)nonDefaultOptions[nameof(Options.DisableObjects)]).GetBoolean());

            // No import path, include command line args
            // Check that all invalid options flagged
            args = ["--bind", "1.1.1.257 127.0.0.1 -::1", "-m", "12mg", "--port", "-1", "--mutable-percent", "101", "--acl-file", "nx_dir/nx_file.txt", "--tls", "--reviv-fraction", "1.1", "--cert-file-name", "testcert.crt"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out exitGracefully, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful);
            ClassicAssert.IsFalse(exitGracefully);
            ClassicAssert.IsNull(options);
            ClassicAssert.AreEqual(7, invalidOptions.Count);
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.Address)));
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.LogMemorySize)));
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

            var dir = TestUtils.MethodTestDir;
            var garnetConfigPath = $"{dir}\\test1.conf";
            var redisConfigPath = $"redis.conf";

            // Import from redis.conf file, no command line args
            // Check values from import path override values from default.conf
            var args = new[] { "--config-import-path", redisConfigPath, "--config-import-format", "RedisConf" };
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("127.0.0.1 -::1", options.Address);
            ClassicAssert.AreEqual(CommandLineBooleanOption.No, options.ProtectedMode);
            ClassicAssert.AreEqual(ConnectionProtectionOption.Local, options.EnableDebugCommand);
            ClassicAssert.AreEqual(ConnectionProtectionOption.Yes, options.EnableModuleCommand);
            ClassicAssert.AreEqual(6379, options.Port);
            ClassicAssert.AreEqual("20gb", options.LogMemorySize);
            ClassicAssert.AreEqual("./garnet-log", options.FileLogger);
            ClassicAssert.AreEqual("./", options.CheckpointDir);
            ClassicAssert.IsTrue(options.EnableCluster);
            ClassicAssert.AreEqual("foobared", options.Password);
            ClassicAssert.AreEqual(4, options.ThreadPoolMinThreads);
            ClassicAssert.AreEqual(15000, options.ClusterTimeout);
            ClassicAssert.AreEqual(LogLevel.Information, options.LogLevel);
            ClassicAssert.AreEqual(10, options.ReplicaSyncDelayMs);
            ClassicAssert.IsTrue(options.EnableTLS);
            ClassicAssert.IsTrue(options.ClientCertificateRequired);
            ClassicAssert.AreEqual("testcert.pfx", options.CertFileName);
            ClassicAssert.AreEqual("placeholder", options.CertPassword);
            ClassicAssert.AreEqual(10000, options.SlowLogThreshold);
            ClassicAssert.AreEqual(128, options.SlowLogMaxEntries);
            ClassicAssert.AreEqual(32, options.MaxDatabases);

            // Import from redis.conf file, include command line args
            // Check values from import path override values from default.conf, and values from command line override values from default.conf and import path
            args = ["--config-import-path", redisConfigPath, "--config-import-format", "RedisConf", "--config-export-path", garnetConfigPath, "-p", "12m", "--tls", "false", "--minthreads", "6", "--client-certificate-required", "true", "--max-databases", "64"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("12m", options.PageSize);
            ClassicAssert.AreEqual("20gb", options.LogMemorySize);
            ClassicAssert.AreEqual("1g", options.SegmentSize);
            ClassicAssert.AreEqual(6, options.ThreadPoolMinThreads);
            ClassicAssert.AreEqual(10, options.ReplicaSyncDelayMs);
            ClassicAssert.IsFalse(options.EnableTLS);
            ClassicAssert.IsTrue(options.ClientCertificateRequired);
            ClassicAssert.AreEqual("testcert.pfx", options.CertFileName);
            ClassicAssert.AreEqual("placeholder", options.CertPassword);
            ClassicAssert.AreEqual(10000, options.SlowLogThreshold);
            ClassicAssert.AreEqual(128, options.SlowLogMaxEntries);
            ClassicAssert.AreEqual(64, options.MaxDatabases);
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
            var deviceFactory = TestUtils.AzureStorageNamedDeviceFactoryCreator.Create(AzureTestDirectory);
            deviceFactory.Delete(new FileDescriptor { directoryName = "" });

            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(null, out var options, out var invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.IsTrue(options.PageSize == "16m");
            ClassicAssert.IsTrue(options.LogMemorySize == "16g");
            ClassicAssert.IsNull(options.AzureStorageServiceUri);
            ClassicAssert.IsNull(options.AzureStorageManagedIdentity);
            ClassicAssert.AreNotEqual(DeviceType.AzureStorage, options.GetDeviceType());

            var args = new[] { "--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-export", "true", "--config-export-path", configPath, "-p", "8m", "-m", "128m", "--storage-service-uri", "https://demo.blob.core.windows.net", "--storage-managed-identity", "demo" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.IsTrue(options.PageSize == "8m");
            ClassicAssert.IsTrue(options.LogMemorySize == "128m");
            ClassicAssert.IsTrue(options.AzureStorageServiceUri == "https://demo.blob.core.windows.net");
            ClassicAssert.IsTrue(options.AzureStorageManagedIdentity == "demo");

            args = ["--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-import", "true", "--config-import-path", configPath];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.IsTrue(options.PageSize == "8m");
            ClassicAssert.IsTrue(options.LogMemorySize == "128m");
            ClassicAssert.IsTrue(options.AzureStorageServiceUri == "https://demo.blob.core.windows.net");
            ClassicAssert.IsTrue(options.AzureStorageManagedIdentity == "demo");

            // Delete blob
            deviceFactory.Delete(new FileDescriptor { directoryName = "" });
        }

        [Test]
        public void AzureStorageConfiguration()
        {
            // missing both storage-string and storage-service-uri
            var args = new string[] { "--use-azure-storage", "true" };
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            Assert.Throws<InvalidAzureConfiguration>(() => options.GetServerOptions());

            // valid storage-string
            args = ["--use-azure-storage", "--storage-string", "UseDevelopmentStorage=true;"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            Assert.DoesNotThrow(() => options.GetServerOptions());

            // secure service-uri with managed-identity
            args = ["--use-azure-storage", "--storage-service-uri", "https://demo.blob.core.windows.net", "--storage-managed-identity", "demo"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            Assert.DoesNotThrow(() => options.GetServerOptions());

            // secure service-uri with workload-identity and no managed-identity
            args = ["--use-azure-storage", "--storage-service-uri", "https://demo.blob.core.windows.net"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            Assert.DoesNotThrow(() => options.GetServerOptions());

            // insecure service-uri with managed-identity
            args = ["--use-azure-storage", "--storage-service-uri", "http://demo.blob.core.windows.net", "--storage-managed-identity", "demo"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 1);
            ClassicAssert.AreEqual(invalidOptions[0], nameof(Options.AzureStorageServiceUri));

            // using both storage-string and managed-identity
            args = ["--use-azure-storage", "--storage-string", "UseDevelopmentStorage", "--storage-managed-identity", "demo", "--storage-service-uri", "https://demo.blob.core.windows.net"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            Assert.Throws<InvalidAzureConfiguration>(() => options.GetServerOptions());
        }

        [Test]
        public void LuaMemoryOptions()
        {
            // Command line
            {
                // Defaults to Native with no limit
                {
                    var args = new[] { "--lua" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Native, options.LuaMemoryManagementMode);
                    ClassicAssert.IsNull(options.LuaScriptMemoryLimit);
                }

                // Native with limit rejected
                {
                    var args = new[] { "--lua", "--lua-script-memory-limit", "10m" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Tracked with no limit works
                {
                    var args = new[] { "--lua", "--lua-memory-management-mode", "Tracked" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Tracked, options.LuaMemoryManagementMode);
                    ClassicAssert.IsNull(options.LuaScriptMemoryLimit);
                }

                // Tracked with limit works
                {
                    var args = new[] { "--lua", "--lua-memory-management-mode", "Tracked", "--lua-script-memory-limit", "10m" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Tracked, options.LuaMemoryManagementMode);
                    ClassicAssert.AreEqual("10m", options.LuaScriptMemoryLimit);
                }

                // Tracked with bad limit rejected
                {
                    var args = new[] { "--lua", "--lua-memory-management-mode", "Tracked", "--lua-script-memory-limit", "10Q" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Managed with no limit works
                {
                    var args = new[] { "--lua", "--lua-memory-management-mode", "Managed" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Managed, options.LuaMemoryManagementMode);
                    ClassicAssert.IsNull(options.LuaScriptMemoryLimit);
                }

                // Managed with limit works
                {
                    var args = new[] { "--lua", "--lua-memory-management-mode", "Managed", "--lua-script-memory-limit", "10m" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Managed, options.LuaMemoryManagementMode);
                    ClassicAssert.AreEqual("10m", options.LuaScriptMemoryLimit);
                }

                // Managed with bad limit rejected
                {
                    var args = new[] { "--lua", "--lua-memory-management-mode", "Managed", "--lua-script-memory-limit", "10Q" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }

            // Garnet.conf
            {
                // Defaults to Native with no limit
                {
                    const string JSON = @"{ ""EnableLua"": true }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Native, options.LuaMemoryManagementMode);
                    ClassicAssert.IsNull(options.LuaScriptMemoryLimit);
                }

                // Native with limit rejected
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaScriptMemoryLimit"": ""10m"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Tracked with no limit works
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaMemoryManagementMode"": ""Tracked"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Tracked, options.LuaMemoryManagementMode);
                    ClassicAssert.IsNull(options.LuaScriptMemoryLimit);
                }

                // Tracked with limit works
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaMemoryManagementMode"": ""Tracked"", ""LuaScriptMemoryLimit"": ""10m"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Tracked, options.LuaMemoryManagementMode);
                    ClassicAssert.AreEqual("10m", options.LuaScriptMemoryLimit);
                }

                // Tracked with bad limit rejected
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaMemoryManagementMode"": ""Tracked"", ""LuaScriptMemoryLimit"": ""10Q"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Managed with no limit works
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaMemoryManagementMode"": ""Managed"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Managed, options.LuaMemoryManagementMode);
                    ClassicAssert.IsNull(options.LuaScriptMemoryLimit);
                }

                // Managed with limit works
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaMemoryManagementMode"": ""Managed"", ""LuaScriptMemoryLimit"": ""10m"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaMemoryManagementMode.Managed, options.LuaMemoryManagementMode);
                    ClassicAssert.AreEqual("10m", options.LuaScriptMemoryLimit);
                }

                // Managed with bad limit rejected
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaMemoryManagementMode"": ""Managed"", ""LuaScriptMemoryLimit"": ""10Q"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void LuaTimeoutOptions()
        {
            // Command line args
            {
                // No value is accepted
                {
                    var args = new[] { "--lua" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(0, options.LuaScriptTimeoutMs);
                }

                // Positive accepted
                {
                    var args = new[] { "--lua", "--lua-script-timeout", "10" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(10, options.LuaScriptTimeoutMs);
                }

                // > 0 and < 10 rejected
                for (var ms = 1; ms < 10; ms++)
                {
                    var args = new[] { "--lua", "--lua-script-timeout", ms.ToString() };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Negative rejected
                {
                    var args = new[] { "--lua", "--lua-script-timeout", "-10" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }

            // Garnet.conf
            {
                // No value is accepted
                {
                    const string JSON = @"{ ""EnableLua"": true }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(0, options.LuaScriptTimeoutMs);
                }

                // Positive accepted
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaScriptTimeoutMs"": 10 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(10, options.LuaScriptTimeoutMs);
                }

                // > 0 and < 10 rejected
                for (var ms = 1; ms < 10; ms++)
                {
                    var json = $@"{{ ""EnableLua"": true, ""LuaScriptTimeoutMs"": {ms} }}";
                    var parseSuccessful = TryParseGarnetConfOptions(json, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Negative rejected
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaScriptTimeoutMs"": -10 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void LuaLoggingOptions()
        {
            // Command line args
            {
                // No value is accepted
                {
                    var args = new[] { "--lua" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Enable, options.LuaLoggingMode);
                }

                // Enable accepted
                {
                    var args = new[] { "--lua", "--lua-logging-mode", "Enable" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Enable, options.LuaLoggingMode);
                }

                // Silent accepted
                {
                    var args = new[] { "--lua", "--lua-logging-mode", "Silent" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Silent, options.LuaLoggingMode);
                }

                // Disable accepted
                {
                    var args = new[] { "--lua", "--lua-logging-mode", "Disable" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Disable, options.LuaLoggingMode);
                }

                // Invalid rejected
                {
                    var args = new[] { "--lua", "--lua-logging-mode", "Foo" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }

            // JSON args
            {
                // No value is accepted
                {
                    const string JSON = @"{ ""EnableLua"": true }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Enable, options.LuaLoggingMode);
                }

                // Enable accepted
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaLoggingMode"": ""Enable"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Enable, options.LuaLoggingMode);
                }

                // Silent accepted
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaLoggingMode"": ""Silent"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Silent, options.LuaLoggingMode);
                }

                // Disable accepted
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaLoggingMode"": ""Disable"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(LuaLoggingMode.Disable, options.LuaLoggingMode);
                }

                // Invalid rejected
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaLoggingMode"": ""Foo"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void LuaAllowedFunctions()
        {
            // Command line args
            {
                // No value is accepted
                {
                    var args = new[] { "--lua" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(0, options.LuaAllowedFunctions.Count());
                }

                // One option works
                {
                    var args = new[] { "--lua", "--lua-allowed-functions", "os" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(1, options.LuaAllowedFunctions.Count());
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("os"));
                }

                // Multiple option works
                {
                    var args = new[] { "--lua", "--lua-allowed-functions", "os,assert,rawget" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(3, options.LuaAllowedFunctions.Count());
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("os"));
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("assert"));
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("rawget"));
                }

                // Invalid rejected
                {
                    var args = new[] { "--lua", "--lua-allowed-functions" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }

            // JSON args
            {
                // No value is accepted
                {
                    const string JSON = @"{ ""EnableLua"": true }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(0, options.LuaAllowedFunctions.Count());
                }

                // One option works
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaAllowedFunctions"": [""os""] }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(1, options.LuaAllowedFunctions.Count());
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("os"));
                }

                // Multiple option works
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaAllowedFunctions"": [""os"", ""assert"", ""rawget""] }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableLua);
                    ClassicAssert.AreEqual(3, options.LuaAllowedFunctions.Count());
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("os"));
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("assert"));
                    ClassicAssert.IsTrue(options.LuaAllowedFunctions.Contains("rawget"));
                }

                // Invalid rejected
                {
                    const string JSON = @"{ ""EnableLua"": true, ""LuaAllowedFunctions"": { } }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void ClusterReplicationReestablishmentTimeout()
        {
            // Command line args
            {
                // No value is accepted
                {
                    var args = Array.Empty<string>();
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.ClusterReplicationReestablishmentTimeout);
                }

                // 0 accepted
                {
                    var args = new[] { "--cluster-replication-reestablishment-timeout", "0" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.ClusterReplicationReestablishmentTimeout);
                }

                // Positive accepted
                {
                    var args = new[] { "--cluster-replication-reestablishment-timeout", "30" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(30, options.ClusterReplicationReestablishmentTimeout);
                }

                // Negative rejected
                {
                    var args = new[] { "--cluster-replication-reestablishment-timeout", "-1" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Invalid rejected
                {
                    var args = new[] { "--cluster-replication-reestablishment-timeout", "foo" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }

            // JSON args
            {
                // No value is accepted
                {
                    const string JSON = @"{ }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.ClusterReplicationReestablishmentTimeout);
                }

                // 0 accepted
                {
                    const string JSON = @"{ ""ClusterReplicationReestablishmentTimeout"": 0 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.ClusterReplicationReestablishmentTimeout);
                }

                // Positive accepted
                {
                    const string JSON = @"{ ""ClusterReplicationReestablishmentTimeout"": 30 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(30, options.ClusterReplicationReestablishmentTimeout);
                }

                // Negative rejected
                {
                    const string JSON = @"{ ""ClusterReplicationReestablishmentTimeout"": -1 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Invalid rejected
                {
                    const string JSON = @"{ ""ClusterReplicationReestablishmentTimeout"": ""foo"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void ClusterReplicaResumeWithData()
        {
            // Command line args
            {
                // Default accepted
                {
                    var args = Array.Empty<string>();
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.ClusterReplicaResumeWithData);
                }

                // Switch is accepted
                {
                    var args = new[] { "--cluster-replica-resume-with-data" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.ClusterReplicaResumeWithData);
                }
            }

            // JSON args
            {
                // Default accepted
                {
                    const string JSON = @"{ }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.ClusterReplicaResumeWithData);
                }

                // False is accepted
                {
                    const string JSON = @"{ ""ClusterReplicaResumeWithData"": false }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.ClusterReplicaResumeWithData);
                }

                // True is accepted
                {
                    const string JSON = @"{ ""ClusterReplicaResumeWithData"": true }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.ClusterReplicaResumeWithData);
                }

                // Invalid rejected
                {
                    const string JSON = @"{ ""ClusterReplicaResumeWithData"": ""foo"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void EnableVectorSetPreview()
        {
            // Command line args
            {
                // Default accepted
                {
                    var args = Array.Empty<string>();
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.EnableVectorSetPreview);
                }

                // Switch is accepted
                {
                    var args = new[] { "--enable-vector-set-preview" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableVectorSetPreview);
                }
            }

            // JSON args
            {
                // Default accepted
                {
                    const string JSON = @"{ }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.EnableVectorSetPreview);
                }

                // False is accepted
                {
                    const string JSON = @"{ ""EnableVectorSetPreview"": false }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.EnableVectorSetPreview);
                }

                // True is accepted
                {
                    const string JSON = @"{ ""EnableVectorSetPreview"": true }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableVectorSetPreview);
                }

                // Invalid rejected
                {
                    const string JSON = @"{ ""EnableVectorSetPreview"": ""foo"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void MinimumPageSizeWithVectorSetPreview()
        {
            // Command line args
            {
                // Allow exactly minimum
                {
                    var args = new[] { "--enable-vector-set-preview", "--page", "16k" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableVectorSetPreview);
                    ClassicAssert.AreEqual("16k", options.PageSize);
                }

                // Allow lower than minimum if preview not enabled
                {
                    var args = new[] { "--page", "1k" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.EnableVectorSetPreview);
                    ClassicAssert.AreEqual("1k", options.PageSize);
                }

                // Reject too small
                {
                    var args = new[] { "--enable-vector-set-preview", "--page", "4k" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }

            // JSON args
            {
                // Allow exactly minimum
                {
                    const string JSON = @"{ ""EnableVectorSetPreview"": true, ""PageSize"": ""16k"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.EnableVectorSetPreview);
                    ClassicAssert.AreEqual("16k", options.PageSize);
                }

                // Allow lower than minimum if preview not enabled
                {
                    const string JSON = @"{ ""EnableVectorSetPreview"": false, ""PageSize"": ""1k"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.EnableVectorSetPreview);
                    ClassicAssert.AreEqual("1k", options.PageSize);
                }

                // Reject too small
                {
                    const string JSON = @"{ ""EnableVectorSetPreview"": true, ""PageSize"": ""4k"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void VectorSetQuantizationTaskCount()
        {
            // Command line args
            {
                // Default accepted
                {
                    var args = Array.Empty<string>();
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.VectorSetQuantizationTaskCount);
                }

                // Switch is accepted
                {
                    var args = new[] { "--vector-set-quantization-task-count", "1" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(1, options.VectorSetQuantizationTaskCount);
                }

                // Zero is accepted
                {
                    var args = new[] { "--vector-set-quantization-task-count", "0" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.VectorSetQuantizationTaskCount);
                }

                // Invalid rejected
                {
                    var args = new[] { "--vector-set-quantization-task-count", "foo" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Too low rejected
                {
                    var args = new[] { "--vector-set-quantization-task-count", "-1" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Too high rejected
                {
                    var args = new[] { "--vector-set-quantization-task-count", "2147483648" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }

            // JSON args
            {
                // Default accepted
                {
                    const string JSON = @"{ }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.VectorSetQuantizationTaskCount);
                }

                // Field is accepted
                {
                    const string JSON = @"{ ""VectorSetQuantizationTaskCount"": 1 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(1, options.VectorSetQuantizationTaskCount);
                }

                // Zero is accepted
                {
                    const string JSON = @"{ ""VectorSetQuantizationTaskCount"": 0 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.AreEqual(0, options.VectorSetQuantizationTaskCount);
                }

                // Invalid rejected
                {
                    const string JSON = @"{ ""VectorSetQuantizationTaskCount"": ""foo"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Too low rejected
                {
                    const string JSON = @"{ ""VectorSetQuantizationTaskCount"": -1 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }

                // Too high rejected
                {
                    const string JSON = @"{ ""VectorSetQuantizationTaskCount"": 2147483648 }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out var exitGracefully);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        /// <summary>
        /// Import a garnet.conf file with the given contents
        /// </summary>
        private static bool TryParseGarnetConfOptions(string json, out Options options, out List<string> invalidOptions, out bool exitGracefully)
        {
            var tempPath = Path.GetTempFileName();
            try
            {
                File.WriteAllText(tempPath, json);

                return ServerSettingsManager.TryParseCommandLineArguments(["--config-import-path", tempPath], out options, out invalidOptions, out _, out exitGracefully, silentMode: true);
            }
            finally
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch
                {
                    // Best effort
                }
            }
        }

        [Test]
        public void UnixSocketPath_CanParseValidPath()
        {
            string[] args = ["--unixsocket", "./config-parse-test.sock"];
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
        }

        [Test]
        public void UnixSocketPath_InvalidPathFails()
        {
            // Socket path directory does not exists
            string[] args = ["--unixsocket", "./does-not-exists/config-parse-test.sock"];
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful);
        }

        [Test]
        public void UnixSocketPermission_CanParseValidPermission()
        {
            if (OperatingSystem.IsWindows())
                return;

            string[] args = ["--unixsocketperm", "777"];
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(777, options.UnixSocketPermission);
        }

        [Test]
        public void UnixSocketPermission_InvalidPermissionFails()
        {
            if (OperatingSystem.IsWindows())
                return;

            string[] args = ["--unixsocketperm", "888"];
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out _, out _, out _, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful);
        }

        [Test]
        [TestCase(TestUtils.pemCertFile)]
        [TestCase("testcert.crt")]
        [TestCase("testcert.cer")]
        public void CertFileName_AcceptsPemExtensions(string certFileName)
        {
            // Only the extension is under test here, so it's fine to point multiple extensions at the same PEM contents.
            var certFilePath = certFileName;
            if (certFileName != TestUtils.pemCertFile)
            {
                Directory.CreateDirectory(TestUtils.MethodTestDir);
                certFilePath = Path.Combine(TestUtils.MethodTestDir, certFileName);
                File.Copy(TestUtils.pemCertFile, certFilePath, overwrite: true);
            }

            string[] args = ["--tls", "--cert-file-name", certFilePath];
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            ClassicAssert.AreEqual(certFilePath, options.CertFileName);

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void CertFileName_RejectsUnsupportedExtension()
        {
            // File exists, but its extension is not one of the recognized certificate extensions.
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var unsupportedExtensionFile = Path.Combine(TestUtils.MethodTestDir, "testcert.txt");
            File.Copy(TestUtils.certFile, unsupportedExtensionFile, overwrite: true);

            string[] args = ["--tls", "--cert-file-name", unsupportedExtensionFile];
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out var invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsFalse(parseSuccessful);
            ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.CertFileName)));

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [TestCase(ConnectionProtectionOption.No)]
        [TestCase(ConnectionProtectionOption.Local)]
        [TestCase(ConnectionProtectionOption.Yes)]
        public async Task ConnectionProtectionTest(ConnectionProtectionOption connectionProtectionOption)
        {
            List<IPAddress> addresses = [IPAddress.IPv6Loopback, IPAddress.Loopback];

            var hostname = TestUtils.GetHostName();

            var address = Dns.GetHostAddresses(hostname).Where(x => !IPAddress.IsLoopback(x)).FirstOrDefault();
            if (address == default)
            {
                if (connectionProtectionOption == ConnectionProtectionOption.Local)
                    Assert.Ignore("No nonloopback address");
            }
            else
            {
                addresses.Add(address);
            }

            var endpoints = addresses.Select(address => new IPEndPoint(address, TestUtils.TestPort)).ToArray();
            var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, endpoints: endpoints,
                                                      enableDebugCommand: connectionProtectionOption);
            server.Start();

            foreach (var endpoint in endpoints)
            {
                var shouldfail = connectionProtectionOption == ConnectionProtectionOption.No ||
                        (!IPAddress.IsLoopback(endpoint.Address) && connectionProtectionOption == ConnectionProtectionOption.Local);
                var client = TestUtils.GetGarnetClientSession(endPoint: endpoint);
                client.Connect();

                try
                {
                    var result = await client.ExecuteAsync("DEBUG", "LOG", "Loopback test").ConfigureAwait(false);
                    if (shouldfail)
                        Assert.Fail("Connection protection should have not allowed the command to run");
                    else
                        ClassicAssert.AreEqual("OK", result);
                }
                catch (Exception ex)
                {
                    if (shouldfail)
                        ClassicAssert.AreEqual("ERR", ex.Message[0..3]);
                    else
                        Assert.Fail("Connection protection should have allowed command from this address");
                }
                finally
                {
                    client.Dispose();
                }
            }

            server.Dispose();
        }

        [Test]
        public async Task MultiTcpSocketTest()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var hostname = TestUtils.GetHostName();
            var addresses = Dns.GetHostAddresses(hostname);
            addresses = [.. addresses, IPAddress.IPv6Loopback, IPAddress.Loopback];

            var endpoints = addresses.Distinct().Select(address => new IPEndPoint(address, TestUtils.TestPort)).ToArray();
            var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, endpoints: endpoints);
            server.Start();

            var clients = endpoints.Select(endpoint => TestUtils.GetGarnetClientSession(endPoint: endpoint)).ToArray();
            foreach (var client in clients)
            {
                client.Connect();
                var result = await client.ExecuteAsync("PING").ConfigureAwait(false);
                ClassicAssert.AreEqual("PONG", result);
                client.Dispose();
            }

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void ClusterPreferredEndpointTypeTest()
        {
            {
                var args = Array.Empty<string>();
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(ClusterPreferredEndpointType.Ip, options.ClusterPreferredEndpointType);
            }

            {
                var args = new[] { "--cluster-preferred-endpoint-type", "ip" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(ClusterPreferredEndpointType.Ip, options.ClusterPreferredEndpointType);
            }

            {
                var args = new[] { "--cluster-preferred-endpoint-type", "hostname" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(ClusterPreferredEndpointType.Hostname, options.ClusterPreferredEndpointType);
            }

            {
                var args = new[] { "--cluster-preferred-endpoint-type", "unknown" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(ClusterPreferredEndpointType.Unknown, options.ClusterPreferredEndpointType);
            }
        }

        [Test]
        public void ClusterAnnounceHostnameTest()
        {
            {
                var args = Array.Empty<string>();
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(null, options.ClusterAnnounceHostname);
            }

            {
                var args = new[] { "--cluster-announce-hostname", "test" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual("test", options.ClusterAnnounceHostname);
            }
        }

        [Test]
        public void RevivificationFlagOrderingIndependence()
        {
            // Specifying --reviv alongside explicit bin sizes and counts should work
            // regardless of argument ordering, because the explicit bins override the
            // power-of-2 default from --reviv (as documented in --reviv help text).
            string[][] argOrderings =
            [
                ["--reviv", "--reviv-bin-record-sizes", "64,128,256", "--reviv-bin-record-counts", "100,200,300"],
                ["--reviv-bin-record-sizes", "64,128,256", "--reviv-bin-record-counts", "100,200,300", "--reviv"],
                ["--reviv-bin-record-sizes", "64,128,256", "--reviv", "--reviv-bin-record-counts", "100,200,300"],
                ["--reviv-bin-record-counts", "100,200,300", "--reviv", "--reviv-bin-record-sizes", "64,128,256"],
                ["--reviv-bin-record-counts", "100,200,300", "--reviv-bin-record-sizes", "64,128,256", "--reviv"],
            ];

            foreach (var args in argOrderings)
            {
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful, $"Parse failed for args: {string.Join(" ", args)}");

                var serverOptions = options.GetServerOptions();
                ClassicAssert.IsFalse(serverOptions.UseRevivBinsPowerOf2, $"UseRevivBinsPowerOf2 should be false for args: {string.Join(" ", args)}");
                CollectionAssert.AreEqual(new[] { 64, 128, 256 }, serverOptions.RevivBinRecordSizes, $"RevivBinRecordSizes mismatch for args: {string.Join(" ", args)}");
                CollectionAssert.AreEqual(new[] { 100, 200, 300 }, serverOptions.RevivBinRecordCounts, $"RevivBinRecordCounts mismatch for args: {string.Join(" ", args)}");
            }

            // --reviv with only sizes (no counts) should also work in any order
            string[][] sizesOnlyOrderings =
            [
                ["--reviv", "--reviv-bin-record-sizes", "64,128"],
                ["--reviv-bin-record-sizes", "64,128", "--reviv"],
            ];

            foreach (var args in sizesOnlyOrderings)
            {
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful, $"Parse failed for args: {string.Join(" ", args)}");

                var serverOptions = options.GetServerOptions();
                ClassicAssert.IsFalse(serverOptions.UseRevivBinsPowerOf2, $"UseRevivBinsPowerOf2 should be false for args: {string.Join(" ", args)}");
                CollectionAssert.AreEqual(new[] { 64, 128 }, serverOptions.RevivBinRecordSizes, $"RevivBinRecordSizes mismatch for args: {string.Join(" ", args)}");
            }

            // --reviv with only counts (no sizes) should still fail regardless of order
            string[][] countsOnlyOrderings =
            [
                ["--reviv", "--reviv-bin-record-counts", "100,200"],
                ["--reviv-bin-record-counts", "100,200", "--reviv"],
            ];

            foreach (var args in countsOnlyOrderings)
            {
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful, $"Parse failed for args: {string.Join(" ", args)}");

                Assert.Throws<Exception>(() => options.GetServerOptions(), $"Should throw for args: {string.Join(" ", args)}");
            }
        }

        [Test]
        public void AclStrictCustomCommands()
        {
            // Command line args
            {
                // Default (option unset) - must apply the strict default (true).
                {
                    var args = Array.Empty<string>();
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.GetServerOptions().AclStrictCustomCommands);
                }

                // Explicit true is accepted.
                {
                    var args = new[] { "--acl-strict-custom-commands", "true" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.GetServerOptions().AclStrictCustomCommands);
                }

                // Explicit false overrides the strict default (operator opt-out path).
                {
                    var args = new[] { "--acl-strict-custom-commands", "false" };
                    var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out _, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.GetServerOptions().AclStrictCustomCommands);
                }
            }

            // JSON args
            {
                // Default (key omitted) - falls back to strict.
                {
                    const string JSON = @"{ }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.GetServerOptions().AclStrictCustomCommands);
                }

                // True is accepted.
                {
                    const string JSON = @"{ ""AclStrictCustomCommands"": true }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsTrue(options.GetServerOptions().AclStrictCustomCommands);
                }

                // False is accepted.
                {
                    const string JSON = @"{ ""AclStrictCustomCommands"": false }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out _, out _);
                    ClassicAssert.IsTrue(parseSuccessful);
                    ClassicAssert.IsFalse(options.GetServerOptions().AclStrictCustomCommands);
                }

                // Invalid value is rejected.
                {
                    const string JSON = @"{ ""AclStrictCustomCommands"": ""foo"" }";
                    var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out _, out _);
                    ClassicAssert.IsFalse(parseSuccessful);
                }
            }
        }

        [Test]
        public void AofSizeLimitWithoutAofEnabled()
        {
            // Setting --aof-size-limit without --aof should throw
            var args = new[] { "--aof-size-limit", "64m" };
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            var ex = Assert.Throws<GarnetException>(() => options.GetServerOptions());
            ClassicAssert.IsTrue(ex.Message.Contains("AofSizeLimit"));

            // Setting --aof-size-limit with --aof enabled should succeed
            args = ["--aof", "--aof-size-limit", "64m"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            Assert.DoesNotThrow(() => options.GetServerOptions());
        }

        [Test]
        public void AofSegmentSizeFlowsToTsavoriteLogSettings()
        {
            // Default AofSegmentSize from defaults.conf should be 1g and applied to TsavoriteLogSettings.
            var args = new[] { "--aof" };
            var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            ClassicAssert.AreEqual("1g", options.AofSegmentSize);

            var serverOptions = options.GetServerOptions();
            serverOptions.GetAofSettings(0, out var logSettings);
            try
            {
                ClassicAssert.AreEqual(1, logSettings.Length);
                ClassicAssert.AreEqual(1L << 30, logSettings[0].SegmentSize);
            }
            finally
            {
                foreach (var s in logSettings)
                {
                    s.LogDevice?.Dispose();
                    s.LogCommitManager?.Dispose();
                }
            }

            // Configured AofSegmentSize should override default and propagate to TsavoriteLogSettings.SegmentSize.
            args = ["--aof", "--aof-segment-size", "64m"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            ClassicAssert.AreEqual("64m", options.AofSegmentSize);

            serverOptions = options.GetServerOptions();
            serverOptions.GetAofSettings(0, out logSettings);
            try
            {
                ClassicAssert.AreEqual(1, logSettings.Length);
                ClassicAssert.AreEqual(1L << 26, logSettings[0].SegmentSize);
            }
            finally
            {
                foreach (var s in logSettings)
                {
                    s.LogDevice?.Dispose();
                    s.LogCommitManager?.Dispose();
                }
            }

            // AofPageSize > AofSegmentSize should throw.
            args = ["--aof", "--aof-page-size", "8m", "--aof-segment-size", "4m", "--aof-memory", "16m"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            serverOptions = options.GetServerOptions();
            var ex = Assert.Throws<Exception>(() => serverOptions.GetAofSettings(0, out _));
            ClassicAssert.IsTrue(ex.Message.Contains("AofPageSize"));
            ClassicAssert.IsTrue(ex.Message.Contains("AofSegmentSize"));
            ClassicAssert.IsTrue(ex.Message.Contains("--aof-segment-size"));

            // AofMemorySize == AofPageSize should throw with a Garnet-specific message naming both
            // AofMemorySize and AofPageSize (regression test for misleading
            // "MemorySize must be at least twice the page size" error surfaced in issue #1811).
            args = ["--aof", "--aof-page-size", "64m", "--aof-memory", "64m"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            serverOptions = options.GetServerOptions();
            ex = Assert.Throws<Exception>(() => serverOptions.GetAofSettings(0, out _));
            ClassicAssert.IsTrue(ex.Message.Contains("AofMemorySize"), $"Expected message to mention AofMemorySize, got: {ex.Message}");
            ClassicAssert.IsTrue(ex.Message.Contains("AofPageSize"), $"Expected message to mention AofPageSize, got: {ex.Message}");
            ClassicAssert.IsTrue(ex.Message.Contains("--aof-memory"), $"Expected message to mention --aof-memory, got: {ex.Message}");

            // AofMemorySize == 2 * AofPageSize should be the minimum that succeeds.
            args = ["--aof", "--aof-page-size", "64m", "--aof-memory", "128m"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            serverOptions = options.GetServerOptions();
            serverOptions.GetAofSettings(0, out logSettings);
            try
            {
                ClassicAssert.AreEqual(1, logSettings.Length);
                ClassicAssert.AreEqual(1L << 26, logSettings[0].PageSize);
                ClassicAssert.AreEqual(1L << 27, logSettings[0].MemorySize);
            }
            finally
            {
                foreach (var s in logSettings)
                {
                    s.LogDevice?.Dispose();
                    s.LogCommitManager?.Dispose();
                }
            }

            // AofPageSize < 2 * (main-log) PageSize should throw, because an AOF entry can be as
            // large as the underlying main-log record. Regression test for the runtime
            // "Entry does not fit on page" surfaced in issue #1811.
            args = ["--aof", "--aof-page-size", "8m", "--aof-memory", "32m", "--page", "16m"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            serverOptions = options.GetServerOptions();
            ex = Assert.Throws<Exception>(() => serverOptions.GetAofSettings(0, out _));
            ClassicAssert.IsTrue(ex.Message.Contains("AofPageSize"), $"Expected message to mention AofPageSize, got: {ex.Message}");
            ClassicAssert.IsTrue(ex.Message.Contains("PageSize"), $"Expected message to mention PageSize, got: {ex.Message}");
            ClassicAssert.IsTrue(ex.Message.Contains("--aof-page-size"), $"Expected message to mention --aof-page-size, got: {ex.Message}");

            // AofPageSize == 2 * (main-log) PageSize should be the minimum that succeeds against the new rule.
            args = ["--aof", "--aof-page-size", "32m", "--aof-memory", "64m", "--page", "16m"];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(0, invalidOptions.Count);
            serverOptions = options.GetServerOptions();
            serverOptions.GetAofSettings(0, out logSettings);
            try
            {
                ClassicAssert.AreEqual(1, logSettings.Length);
                ClassicAssert.AreEqual(1L << 25, logSettings[0].PageSize);
                ClassicAssert.AreEqual(1L << 26, logSettings[0].MemorySize);
            }
            finally
            {
                foreach (var s in logSettings)
                {
                    s.LogDevice?.Dispose();
                    s.LogCommitManager?.Dispose();
                }
            }
        }

        [Test]
        public void MaxInlineValueSizeParsing()
        {
            // Default value from defaults.conf is null (computed based on page size at runtime)
            {
                var args = Array.Empty<string>();
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.IsNull(options.MaxInlineValueSize);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.IsNull(serverOptions.MaxInlineValueSize);
                var pageSize = 1L << serverOptions.PageSizeBits();
                // With default 16m page size, the default should be 1m (min of 1m and pageSize/2)
                ClassicAssert.AreEqual(1048576, serverOptions.MaxInlineValueSizeBytes(pageSize));
            }

            // Various valid memory size strings (CLI). Use a 1g page size so the upper-bound case (15m) passes the page-fit check.
            foreach (var (input, expectedBytes) in new[] { ("64", 64), ("1k", 1024), ("4k", 4096), ("1m", 1048576), ("15m", 15 * 1048576) })
            {
                var args = new[] { "--page", "1g", "--max-inline-value-size", input };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful, $"CLI parsing failed for '{input}'");
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual(input, options.MaxInlineValueSize);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(expectedBytes, serverOptions.MaxInlineValueSizeBytes(pageSize), $"Expected {expectedBytes} bytes for '{input}'");
            }

            // JSON parsing of a valid memory size string
            {
                const string JSON = @"{ ""MaxInlineValueSize"": ""1m"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual("1m", options.MaxInlineValueSize);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(1048576, serverOptions.MaxInlineValueSizeBytes(pageSize));
            }
        }

        [Test]
        public void MaxInlineValueSizeValidation()
        {
            // Reject invalid format (CLI)
            {
                var args = new[] { "--max-inline-value-size", "abc" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsFalse(parseSuccessful);
                ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.MaxInlineValueSize)));
            }

            // Regression: the previous regex used a `[K|k|M|m|G|g]` character class which treated the pipe as a literal,
            // so inputs like '4|' were silently accepted. The tightened regex rejects them.
            {
                var args = new[] { "--max-inline-value-size", "4|" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsFalse(parseSuccessful);
                ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.MaxInlineValueSize)));
            }

            // Reject below minimum (below 0 bytes minimum) — enforced at server-options time.
            {
                var args = new[] { "--max-inline-value-size", "-1" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsFalse(parseSuccessful);
            }

            // Reject above maximum (17m > 16777214 bytes maximum) — enforced at server-options time.
            {
                var args = new[] { "--page", "1g", "--max-inline-value-size", "17m" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                var ex = Assert.Throws<Exception>(() => serverOptions.MaxInlineValueSizeBytes(pageSize));
                ClassicAssert.IsTrue(ex.Message.Contains(nameof(serverOptions.MaxInlineValueSize)));
            }

            // Accept exactly the minimum (0 bytes)
            {
                var args = new[] { "--max-inline-value-size", "0" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(0, serverOptions.MaxInlineValueSizeBytes(pageSize));
            }

            // Accept exactly the maximum (16777214 bytes). Use a 1g PageSize so the page-fit check passes.
            {
                var args = new[] { "--page", "1g", "--max-inline-value-size", "16777214" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(16777214, serverOptions.MaxInlineValueSizeBytes(pageSize));
            }

            // JSON: reject invalid format
            {
                const string JSON = @"{ ""MaxInlineValueSize"": ""xyz"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out var invalidOptions, out _);
                ClassicAssert.IsFalse(parseSuccessful);
                ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.MaxInlineValueSize)));
            }

            // JSON: reject below minimum (enforced at server-options time)
            {
                const string JSON = @"{ ""MaxInlineValueSize"": ""-1"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out var invalidOptions, out _);
                ClassicAssert.IsFalse(parseSuccessful);
            }

            // JSON: reject above maximum (enforced at server-options time)
            {
                const string JSON = @"{ ""PageSize"": ""1g"", ""MaxInlineValueSize"": ""17m"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                Assert.Throws<Exception>(() => serverOptions.MaxInlineValueSizeBytes(pageSize));
            }
        }

        [Test]
        public void MaxInlineValueSizeMustFitOnPage()
        {
            // MaxInlineValueSize equal to PageSize must throw an exception.
            {
                var args = new[] { "--page", "4k", "--max-inline-value-size", "4k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                var ex = Assert.Throws<Exception>(() => serverOptions.MaxInlineValueSizeBytes(pageSize));
                ClassicAssert.IsTrue(ex.Message.Contains("greater than half the page size"));
            }

            // MaxInlineValueSize greater than PageSize must throw an exception.
            {
                var args = new[] { "--page", "1k", "--max-inline-value-size", "4k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                var ex = Assert.Throws<Exception>(() => serverOptions.MaxInlineValueSizeBytes(pageSize));
                ClassicAssert.IsTrue(ex.Message.Contains("greater than half the page size"));
            }

            // MaxInlineValueSize equal to PageSize/2 is accepted.
            {
                var args = new[] { "--page", "8k", "--max-inline-value-size", "4k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(4096, serverOptions.MaxInlineValueSizeBytes(pageSize));
            }

            // MaxInlineValueSize strictly less than PageSize/2 is returned as-is (Tsavorite may round down internally).
            {
                var args = new[] { "--page", "16k", "--max-inline-value-size", "5k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                // Raw bytes 5120 are returned by the helper; Tsavorite may round to 4096 internally.
                ClassicAssert.AreEqual(5120, serverOptions.MaxInlineValueSizeBytes(pageSize));
            }
        }

        [Test]
        public void MaxInlineValueSizeDefaultBehavior()
        {
            // When MaxInlineValueSize is not specified and PageSize >= 2m, default should be 1m.
            {
                var args = new[] { "--page", "4m" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(1048576, serverOptions.MaxInlineValueSizeBytes(pageSize)); // 1m
            }

            // When MaxInlineValueSize is not specified and PageSize >= 2m (exactly), default should be 1m.
            {
                var args = new[] { "--page", "2m" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(1048576, serverOptions.MaxInlineValueSizeBytes(pageSize)); // 1m
            }

            // When MaxInlineValueSize is not specified and PageSize < 2m, default should be pageSize/2.
            {
                var args = new[] { "--page", "1m" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(524288, serverOptions.MaxInlineValueSizeBytes(pageSize)); // 512k = 1m/2
            }

            // When MaxInlineValueSize is not specified and PageSize is very small, default should be pageSize/2.
            {
                var args = new[] { "--page", "4k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(2048, serverOptions.MaxInlineValueSizeBytes(pageSize)); // 2k = 4k/2
            }

            // When MaxInlineValueSize IS specified, it must be <= pageSize/2.
            {
                var args = new[] { "--page", "4m", "--max-inline-value-size", "1m" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                ClassicAssert.AreEqual(1048576, serverOptions.MaxInlineValueSizeBytes(pageSize)); // 1m
            }

            // When MaxInlineValueSize IS specified but exceeds pageSize/2, throw exception.
            {
                var args = new[] { "--page", "1m", "--max-inline-value-size", "1m" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var pageSize = 1L << serverOptions.PageSizeBits();
                var ex = Assert.Throws<Exception>(() => serverOptions.MaxInlineValueSizeBytes(pageSize));
                ClassicAssert.IsTrue(ex.Message.Contains("greater than half the page size"));
            }
        }

        [Test]
        public void MaxInlineKeySizeParsing()
        {
            // Default value from defaults.conf is null (falls back to 1022 at server-options time)
            {
                var args = Array.Empty<string>();
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.IsNull(options.MaxInlineKeySize);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.IsNull(serverOptions.MaxInlineKeySize);
                ClassicAssert.AreEqual(1022, serverOptions.MaxInlineKeySizeBytes());
            }

            // Various valid memory size strings (CLI)
            foreach (var (input, expectedBytes) in new[] { ("0", 0), ("64", 64), ("128", 128), ("512", 512), ("1022", 1022) })
            {
                var args = new[] { "--max-inline-key-size", input };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful, $"CLI parsing failed for '{input}'");
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual(input, options.MaxInlineKeySize);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.AreEqual(expectedBytes, serverOptions.MaxInlineKeySizeBytes(), $"Expected {expectedBytes} bytes for '{input}'");
            }

            // JSON parsing of a valid memory size string
            {
                const string JSON = @"{ ""MaxInlineKeySize"": ""512"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual("512", options.MaxInlineKeySize);
                ClassicAssert.AreEqual(512, options.GetServerOptions().MaxInlineKeySizeBytes());
            }
        }

        [Test]
        public void MaxInlineKeySizeValidation()
        {
            // Reject invalid format (CLI)
            {
                var args = new[] { "--max-inline-key-size", "abc" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsFalse(parseSuccessful);
                ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.MaxInlineKeySize)));
            }

            // Reject below minimum (-1 bytes < 0 bytes minimum)
            {
                var args = new[] { "--max-inline-key-size", "-1" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out _, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsFalse(parseSuccessful);
            }

            // Reject above maximum (1023 bytes > 1022 bytes maximum) — enforced at server-options time.
            {
                var args = new[] { "--max-inline-key-size", "1023" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var ex = Assert.Throws<Exception>(() => serverOptions.MaxInlineKeySizeBytes());
                ClassicAssert.IsTrue(ex.Message.Contains(nameof(serverOptions.MaxInlineKeySize)));
            }

            // Reject above maximum using size suffix (1k = 1024 bytes > 1022 bytes maximum)
            {
                var args = new[] { "--max-inline-key-size", "1k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var ex = Assert.Throws<Exception>(() => serverOptions.MaxInlineKeySizeBytes());
                ClassicAssert.IsTrue(ex.Message.Contains(nameof(serverOptions.MaxInlineKeySize)));
            }

            // Accept exactly the minimum (0 bytes)
            {
                var args = new[] { "--max-inline-key-size", "0" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual(0, options.GetServerOptions().MaxInlineKeySizeBytes());
            }

            // Accept exactly the maximum (1022 bytes)
            {
                var args = new[] { "--max-inline-key-size", "1022" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual(1022, options.GetServerOptions().MaxInlineKeySizeBytes());
            }

            // JSON: reject invalid format
            {
                const string JSON = @"{ ""MaxInlineKeySize"": ""xyz"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out _, out var invalidOptions, out _);
                ClassicAssert.IsFalse(parseSuccessful);
                ClassicAssert.IsTrue(invalidOptions.Contains(nameof(Options.MaxInlineKeySize)));
            }

            // JSON: reject above maximum (enforced at server-options time)
            {
                const string JSON = @"{ ""MaxInlineKeySize"": ""1023"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                Assert.Throws<Exception>(() => serverOptions.MaxInlineKeySizeBytes());
            }
        }

        [Test]
        public void MinimumPageSize()
        {
            // PageSize below 512 bytes must be rejected at server-options consumption time.
            // 256B is a valid memory-size string at parse time but is rejected by PageSizeBits().
            {
                var args = new[] { "--page", "256" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var ex = Assert.Throws<Exception>(() => serverOptions.PageSizeBits());
                ClassicAssert.IsTrue(ex.Message.Contains(nameof(serverOptions.PageSize)));
                ClassicAssert.IsTrue(ex.Message.Contains("512"));
            }

            // 384B rounds down to 256B (previous power of 2) and is rejected.
            {
                var args = new[] { "--page", "384" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                Assert.Throws<Exception>(() => serverOptions.PageSizeBits());
            }

            // Exactly 512B is accepted.
            {
                var args = new[] { "--page", "512" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.AreEqual(9, serverOptions.PageSizeBits());
            }

            // 1k is accepted.
            {
                var args = new[] { "--page", "1k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.AreEqual(10, serverOptions.PageSizeBits());
            }
        }

        [Test]
        public void MinimumReadCachePageSize()
        {
            // ReadCachePageSize below 512 bytes must be rejected.
            // 256B is a valid memory-size string at parse time but is rejected by ReadCachePageSizeBits().
            {
                var args = new[] { "--readcache-page", "256" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                var ex = Assert.Throws<Exception>(() => serverOptions.ReadCachePageSizeBits());
                ClassicAssert.IsTrue(ex.Message.Contains(nameof(serverOptions.ReadCachePageSize)));
                ClassicAssert.IsTrue(ex.Message.Contains("512"));
            }

            // 384B rounds down to 256B (previous power of 2) and is rejected.
            {
                var args = new[] { "--readcache-page", "384" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                Assert.Throws<Exception>(() => serverOptions.ReadCachePageSizeBits());
            }

            // Exactly 512B is accepted.
            {
                var args = new[] { "--readcache-page", "512" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.AreEqual(9, serverOptions.ReadCachePageSizeBits());
            }

            // 1k is accepted.
            {
                var args = new[] { "--readcache-page", "1k" };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.AreEqual(10, serverOptions.ReadCachePageSizeBits());
            }
        }

        [Test]
        public void InitialIORecordSizeParsing()
        {
            // Default value from defaults.conf is null (unset)
            {
                var args = Array.Empty<string>();
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.IsNull(options.InitialIORecordSize);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.IsNull(serverOptions.InitialIORecordSize);
                ClassicAssert.AreEqual(KVSettings.UseDefaultInitialIORecordSize, serverOptions.GetInitialIORecordSizeBytes());
            }

            // Various valid memory size strings (CLI)
            foreach (var (input, expectedBytes) in new[] { ("24", 24), ("4k", 4096), ("8k", 8192) })
            {
                var args = new[] { "--initial-io-record-size", input };
                var parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out var options, out var invalidOptions, out _, out _, silentMode: true);
                ClassicAssert.IsTrue(parseSuccessful, $"CLI parsing failed for '{input}'");
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual(input, options.InitialIORecordSize);
                var serverOptions = options.GetServerOptions();
                ClassicAssert.AreEqual(expectedBytes, serverOptions.GetInitialIORecordSizeBytes(), $"Expected {expectedBytes} bytes for '{input}'");
            }

            // JSON parsing of a valid memory size string
            {
                const string JSON = @"{ ""InitialIORecordSize"": ""4k"" }";
                var parseSuccessful = TryParseGarnetConfOptions(JSON, out var options, out var invalidOptions, out _);
                ClassicAssert.IsTrue(parseSuccessful);
                ClassicAssert.AreEqual(0, invalidOptions.Count);
                ClassicAssert.AreEqual("4k", options.InitialIORecordSize);
                ClassicAssert.AreEqual(4096, options.GetServerOptions().GetInitialIORecordSizeBytes());
            }
        }
    }
}