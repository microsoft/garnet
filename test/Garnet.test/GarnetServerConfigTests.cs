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
    public class GarnetServerConfigTests
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
            ClassicAssert.AreEqual("32m", options.PageSize);
            ClassicAssert.AreEqual("16g", options.MemorySize);
            var nonDefaultOptions = JsonSerializer.Deserialize<Dictionary<string, object>>(optionsJson);
            ClassicAssert.IsEmpty(nonDefaultOptions);

            // No import path, include command line args, export to file
            // Check values from command line override values from defaults.conf
            static string GetFullExtensionBinPath(string testProjectName) => Path.GetFullPath(testProjectName, TestUtils.RootTestsProjectPath);
            var binPaths = new[] { GetFullExtensionBinPath("Garnet.test"), GetFullExtensionBinPath("Garnet.test.cluster") };
            var modules = new[] { Assembly.GetExecutingAssembly().Location };

            var args = new[] { "--config-export-path", configPath, "-p", "4m", "-m", "128m", "-s", "2g", "--index", "128m", "--recover", "--port", "53", "--reviv-fraction", "0.5", "--reviv-bin-record-counts", "1,2,3", "--extension-bin-paths", string.Join(',', binPaths), "--loadmodulecs", string.Join(',', modules) };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out optionsJson, out exitGracefully, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("4m", options.PageSize);
            ClassicAssert.AreEqual("128m", options.MemorySize);
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
            ClassicAssert.AreEqual("4m", ((JsonElement)nonDefaultOptions[nameof(Options.PageSize)]).GetString());
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
            ClassicAssert.IsTrue(options.PageSize == "4m");
            ClassicAssert.IsTrue(options.MemorySize == "128m");
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, options.RevivBinRecordCounts);
            CollectionAssert.AreEqual(binPaths, options.ExtensionBinPaths);
            CollectionAssert.AreEqual(modules, options.LoadModuleCS);

            // Validate non-default configuration options
            nonDefaultOptions = JsonSerializer.Deserialize<Dictionary<string, object>>(optionsJson);
            ClassicAssert.AreEqual(9, nonDefaultOptions.Count);
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.PageSize)));
            ClassicAssert.AreEqual("4m", ((JsonElement)nonDefaultOptions[nameof(Options.PageSize)]).GetString());

            // Import from previous export command, include command line args, export to file
            // Check values from import path override values from default.conf, and values from command line override values from default.conf and import path
            binPaths = [GetFullExtensionBinPath("Garnet.test")];
            args = ["--config-import-path", configPath, "-p", "12m", "-s", "1g", "--recover", "false", "--index", "256m", "--port", "0", "--no-obj", "--aof", "--reviv-bin-record-counts", "4,5", "--extension-bin-paths", string.Join(',', binPaths)];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out optionsJson, out exitGracefully, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.AreEqual("12m", options.PageSize);
            ClassicAssert.AreEqual("128m", options.MemorySize);
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
            ClassicAssert.IsTrue(nonDefaultOptions.ContainsKey(nameof(Options.IndexSize)));
            ClassicAssert.AreEqual("256m", ((JsonElement)nonDefaultOptions[nameof(Options.IndexSize)]).GetString());
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
            ClassicAssert.AreEqual("20gb", options.MemorySize);
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
            ClassicAssert.AreEqual("20gb", options.MemorySize);
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
            ClassicAssert.IsTrue(options.PageSize == "32m");
            ClassicAssert.IsTrue(options.MemorySize == "16g");
            ClassicAssert.IsNull(options.AzureStorageServiceUri);
            ClassicAssert.IsNull(options.AzureStorageManagedIdentity);
            ClassicAssert.AreNotEqual(DeviceType.AzureStorage, options.GetDeviceType());

            var args = new[] { "--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-export", "true", "--config-export-path", configPath, "-p", "4m", "-m", "128m", "--storage-service-uri", "https://demo.blob.core.windows.net", "--storage-managed-identity", "demo" };
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.IsTrue(options.PageSize == "4m");
            ClassicAssert.IsTrue(options.MemorySize == "128m");
            ClassicAssert.IsTrue(options.AzureStorageServiceUri == "https://demo.blob.core.windows.net");
            ClassicAssert.IsTrue(options.AzureStorageManagedIdentity == "demo");

            args = ["--storage-string", AzureEmulatedStorageString, "--use-azure-storage-for-config-import", "true", "--config-import-path", configPath];
            parseSuccessful = ServerSettingsManager.TryParseCommandLineArguments(args, out options, out invalidOptions, out _, out _, silentMode: true);
            ClassicAssert.IsTrue(parseSuccessful);
            ClassicAssert.AreEqual(invalidOptions.Count, 0);
            ClassicAssert.IsTrue(options.PageSize == "4m");
            ClassicAssert.IsTrue(options.MemorySize == "128m");
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
                    var result = await client.ExecuteAsync("DEBUG", "LOG", "Loopback test");
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
                var result = await client.ExecuteAsync("PING");
                ClassicAssert.AreEqual("PONG", result);
                client.Dispose();
            }

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}