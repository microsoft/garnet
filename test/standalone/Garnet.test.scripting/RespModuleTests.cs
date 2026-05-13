// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespModuleTests : AllureTestBase
    {
        GarnetServer server;
        private string testModuleDir;
        string binPath;

        [SetUp]
        public void Setup()
        {
            testModuleDir = Path.Combine(TestUtils.MethodTestDir, "testModules");
            binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                disablePubSub: true,
                enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes,
                extensionBinPaths: [testModuleDir, binPath],
                extensionAllowUnsignedAssemblies: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown(waitForDelete: true);
            TestUtils.DeleteDirectory(Directory.GetParent(testModuleDir)?.FullName);
        }

        private string CreateTestModule(string onLoadBody, string moduleName)
        {
            var runtimePath = RuntimeEnvironment.GetRuntimeDirectory();
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            ClassicAssert.IsNotNull(binPath);

            var referenceFiles = new[]
            {
                Path.Combine(runtimePath, "System.dll"),
                Path.Combine(runtimePath, "System.Core.dll"),
                Path.Combine(runtimePath, "System.Collections.dll"),
                Path.Combine(runtimePath, "System.Private.CoreLib.dll"),
                Path.Combine(runtimePath, "System.Runtime.dll"),
                Path.Combine(binPath, "Garnet.server.dll"),
                Path.Combine(binPath, "Garnet.common.dll"),
                Path.Combine(binPath, "Tsavorite.core.dll"),
            };

            var dir1 = Path.Combine(testModuleDir, Path.GetFileName(TestUtils.MethodTestDir));

            Directory.CreateDirectory(testModuleDir);
            Directory.CreateDirectory(dir1);

            var testFilePath = Path.Combine(TestUtils.MethodTestDir, "TestModule.cs");
            using (var testFile = File.CreateText(testFilePath))
            {
                testFile.WriteLine(
                    @"using System;
                    using System.Collections.Generic; 
                    using Garnet; 
                    using Garnet.server; 
                    using Tsavorite.core; 
                    namespace TestGarnetModule  
                    {  
                       public class TestModule : ModuleBase  
                       {  
                           public override void OnLoad(ModuleLoadContext context, string[] args) 
                           {" +
                    onLoadBody +
                    @"     }
                       }
                    }");
            }

            var modulePath = Path.Combine(dir1, moduleName);
            var filesToCompile = new[] {
                testFilePath,
                Path.GetFullPath(@"../main/GarnetServer/Extensions/SetIfPM.cs", TestUtils.RootTestsProjectPath),
                Path.GetFullPath(@"../main/GarnetServer/Extensions/ReadWriteTxn.cs", TestUtils.RootTestsProjectPath),
                Path.GetFullPath(@"../main/GarnetServer/Extensions/MyDictObject.cs", TestUtils.RootTestsProjectPath),
                Path.GetFullPath(@"../main/GarnetServer/Extensions/MyDictSet.cs", TestUtils.RootTestsProjectPath),
                Path.GetFullPath(@"../main/GarnetServer/Extensions/MyDictGet.cs", TestUtils.RootTestsProjectPath),
                Path.GetFullPath(@"../main/GarnetServer/Extensions/Sum.cs", TestUtils.RootTestsProjectPath)};
            TestUtils.CreateTestLibrary(null, referenceFiles, filesToCompile, modulePath);
            return modulePath;
        }

        [Test]
        public void TestModuleLoad()
        {
            var onLoad =
                    @"context.Initialize(""TestModule3"", 1);
                    
                    context.RegisterCommand(""TestModule3.SetIfPM"", new SetIfPMCustomCommand(), CommandType.ReadModifyWrite,
                    new RespCommandsInfo { Name = ""TestModule3.SETIFPM"", Arity = 4, FirstKey = 1, LastKey = 1, Step = 1,
                    Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.String | RespAclCategories.Write });
                    
                    context.RegisterTransaction(""TestModule3.READWRITETX"", () => new ReadWriteTxn(),
                    new RespCommandsInfo { Name = ""TestModule3.READWRITETX"", Arity = 4, FirstKey = 1, LastKey = 3, Step = 1,
                    Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.Write });

                    var factory = new MyDictFactory();
                    context.RegisterType(factory);

                    context.RegisterCommand(""TestModule3.MYDICTSET"", factory, new MyDictSet(), CommandType.ReadModifyWrite,
                    new RespCommandsInfo { Name = ""TestModule3.MYDICTSET"", Arity = 4, FirstKey = 1, LastKey = 1, Step = 1, 
                    Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.Write });

                    context.RegisterCommand(""TestModule3.MYDICTGET"", factory, new MyDictGet(), CommandType.Read,
                    new RespCommandsInfo { Name = ""TestModule3.MYDICTGET"", Arity = 3, FirstKey = 1, LastKey = 1, Step = 1,
                    Flags = RespCommandFlags.ReadOnly, AclCategories = RespAclCategories.Read });

                    context.RegisterProcedure(""TestModule3.SUM"", () => new Sum());";

            var modulePath = CreateTestModule(onLoad, "TestModule3.dll");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resp = db.Execute($"MODULE", ["LOADCS", modulePath]);
            ClassicAssert.AreEqual("OK", (string)resp);

            // Test SETIFPM
            string key = "testkey";
            string value = "foovalue1";
            db.StringSet(key, value);
            var retValue = db.StringGet(key);
            ClassicAssert.AreEqual(value, retValue.ToString());

            string newValue = "foovalue2";
            resp = db.Execute("TestModule3.SETIFPM", key, newValue, "foo");
            ClassicAssert.AreEqual("OK", (string)resp);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue, retValue.ToString());

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            var result = db.Execute("TestModule3.READWRITETX", key, writekey1, writekey2);
            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            retValue = db.StringGet(writekey1);
            ClassicAssert.IsNotNull(retValue);
            ClassicAssert.AreEqual(newValue, retValue.ToString());

            retValue = db.StringGet(writekey2);
            ClassicAssert.AreEqual(newValue, retValue.ToString());

            // Test MYDICTSET
            var dictKey = "dictkey";
            var dictField = "dictfield";
            var dictValue = "dictvalue";
            resp = db.Execute("TestModule3.MYDICTSET", dictKey, dictField, dictValue);
            ClassicAssert.AreEqual("OK", (string)resp);

            var dictRetValue = db.Execute("TestModule3.MYDICTGET", dictKey, dictField);
            ClassicAssert.AreEqual(dictValue, (string)dictRetValue);

            // Test SUM command
            db.StringSet("key1", "1");
            db.StringSet("key2", "2");
            db.StringSet("key3", "3");
            result = db.Execute("TestModule3.SUM", "key1", "key2", "key3");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual("6", result.ToString());
        }


        [Test]
        public void TestModuleLoadUsingGarnetOptions()
        {
            var onLoad =
                    @"context.Initialize(""TestModule1"", 1);
                    
                    context.RegisterCommand(""TestModule1.SetIfPM"", new SetIfPMCustomCommand(), CommandType.ReadModifyWrite,
                    new RespCommandsInfo { Name = ""TestModule.SETIFPM"", Arity = 4, FirstKey = 1, LastKey = 1, Step = 1,
                    Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.String | RespAclCategories.Write });";

            var onLoad2 =
                   @"context.Initialize(""TestModule2"", 1);
                   
                    context.RegisterProcedure(""TestModule2.SUM"", () => new Sum());";

            var module1Path = CreateTestModule(onLoad, "TestModule1.dll");
            var module2Path = CreateTestModule(onLoad2, "TestModule2.dll");
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                disablePubSub: true,
                loadModulePaths: [module1Path, module2Path]);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            //// Test SETIFPM
            string key = "testkey";
            string value = "foovalue1";
            db.StringSet(key, value);
            var retValue = db.StringGet(key);
            ClassicAssert.AreEqual(value, retValue.ToString());

            string newValue = "foovalue2";
            var resp = db.Execute("TestModule1.SETIFPM", key, newValue, "foo");
            ClassicAssert.AreEqual("OK", (string)resp);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue, retValue.ToString());

            // Test SUM command
            db.StringSet("key1", "1");
            db.StringSet("key2", "2");
            db.StringSet("key3", "3");
            var result = db.Execute("TestModule2.SUM", "key1", "key2", "key3");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual("6", result.ToString());
        }

        [Test]
        public void TestModuleLoadCSArgs()
        {
            var onLoad =
                    @"context.Initialize(""TestModuleLoadCSArgs"", 1);
                    
                    if (args.Length != 2)
                        throw new Exception(""Invalid number of arguments"");

                    if (args[0] != ""arg0"")
                        throw new Exception($""Incorrect arg value {args[0]}"");

                    if (args[1] != ""arg1"")
                        throw new Exception($""Incorrect arg value {args[1]}"");";

            var modulePath = CreateTestModule(onLoad, "TestModuleLoadCSArgs.dll");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resp = db.Execute($"MODULE", ["LOADCS", modulePath, "arg0", "arg1"]);
            ClassicAssert.AreEqual("OK", (string)resp);
        }

        [Test]
        public void TestUninitializedModule()
        {
            var modulePath = CreateTestModule("", "TestModule4.dll");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute($"MODULE", ["LOADCS", modulePath]),
                "Module with empty OnLoad should not load successfully");
            ClassicAssert.AreEqual("ERR Error during module OnLoad", ex.Message);
        }

        [Test]
        public void TestAlreadyLoadedModule()
        {
            var modulePath = CreateTestModule(
                @"context.Initialize(""TestAlreadyLoadedModule"", 1);", "TestModule5.dll");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute($"MODULE", ["LOADCS", modulePath]);
            var ex = Assert.Throws<RedisServerException>(() => db.Execute($"MODULE", ["LOADCS", modulePath]),
                "Already loaded module should not successfully load again");
            ClassicAssert.AreEqual("ERR Error during module OnLoad", ex.Message);
        }

        [Test]
        public void TestNonExistingModule()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Test loading non-existent module
            var nonExistentModulePath = "NonExistentModule.dll";

            try
            {
                db.Execute($"MODULE", ["LOADCS", nonExistentModulePath]);
                Assert.Fail("Non existing module assembly should not load successfully");
            }
            catch (RedisException ex)
            {
                ClassicAssert.AreEqual("ERR unable to access one or more binary files.", ex.Message);
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestNoOpModule(bool loadFromDll)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Test loading no-op module
            if (loadFromDll)
            {
                var noOpModulePath = Path.Join(binPath, "NoOpModule.dll");
                db.Execute($"MODULE", "LOADCS", noOpModulePath);
            }
            else
            {
                server.Register.NewModule(new NoOpModule.NoOpModule(), [], out _);
            }

            // Test raw string command in no-op module
            var key = $"mykey";
            var value = $"myval";
            db.StringSet(key, value);

            var retValue = db.Execute("NoOpModule.NOOPCMDREAD", key);
            ClassicAssert.AreEqual("OK", (string)retValue);

            retValue = db.Execute("NoOpModule.NOOPCMDRMW", key);
            ClassicAssert.AreEqual("OK", (string)retValue);

            // Test object commands in no-op module
            var objKey = "myobjkey";
            retValue = db.Execute("NoOpModule.NOOPOBJRMW", objKey);
            ClassicAssert.AreEqual("OK", (string)retValue);

            retValue = db.Execute("NoOpModule.NOOPOBJREAD", objKey);
            ClassicAssert.AreEqual("OK", (string)retValue);

            // Test transaction in no-op module
            retValue = db.Execute("NoOpModule.NOOPTXN");
            ClassicAssert.AreEqual("OK", (string)retValue);

            // Test procedure in no-op module
            retValue = db.Execute("NoOpModule.NOOPPROC");
            ClassicAssert.AreEqual("OK", (string)retValue);
        }
    }

    [NonParallelizable]
    [AllureNUnit]
    [TestFixture]
    public class RespModuleAdditionalTests : AllureTestBase
    {
        private string testModuleDir;
        string binPath;

        [SetUp]
        public void Setup()
        {
            testModuleDir = Path.Combine(TestUtils.MethodTestDir, "testModules");
            binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown(waitForDelete: true);
            TestUtils.DeleteDirectory(Directory.GetParent(testModuleDir)?.FullName);
        }

        [Test]
        public void TestNoAllowedPathsForModuleLoading()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                disableObjects: true,
                disablePubSub: true,
                enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes,
                extensionBinPaths: null,
                extensionAllowUnsignedAssemblies: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Test loading no-op module
            var noOpModulePath = Path.Join(binPath, "NoOpModule.dll");
            var ex = Assert.Throws<RedisServerException>(() => db.Execute($"MODULE", "LOADCS", noOpModulePath),
                "Should fail since module path allowlist is undefined");

            var err = "To enable client-side assembly loading, you must specify a list of allowed paths from which assemblies " +
                      "can potentially be loaded using the ExtensionBinPath directive.";
            ClassicAssert.AreEqual(err, ex.Message);
        }

        [Test]
        public void TestModuleCommandNotEnabled()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                disableObjects: true,
                disablePubSub: true,
                enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.No,
                extensionBinPaths: [testModuleDir, binPath],
                extensionAllowUnsignedAssemblies: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Test loading no-op module
            var noOpModulePath = Path.Join(binPath, "NoOpModule.dll");
            var ex = Assert.Throws<RedisServerException>(() => db.Execute($"MODULE", "LOADCS", noOpModulePath),
            "Should fail since MODULE command is disabled");

            var err = "ERR MODULE command not allowed. If the enable-module-command option is set to \"local\", " +
                      "you can run it from a local connection, otherwise you need to set this option in the configuration file, " +
                      "and then restart the server.";
            ClassicAssert.AreEqual(err, ex.Message);
        }
    }
}