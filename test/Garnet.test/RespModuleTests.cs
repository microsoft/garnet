// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespModuleTests
    {
        GarnetServer server;
        private string testModuleDir;

        [SetUp]
        public void Setup()
        {
            testModuleDir = Path.Combine(TestUtils.MethodTestDir, "testModules");
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                disablePubSub: true,
                extensionBinPaths: [testModuleDir],
                extensionAllowUnsignedAssemblies: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.DeleteDirectory(Directory.GetParent(testModuleDir)?.FullName);
        }

        private string CreateTestModule(string onLoadBody)
        {
            var runtimePath = RuntimeEnvironment.GetRuntimeDirectory();
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            Assert.IsNotNull(binPath);

            var referenceFiles = new[]
            {
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
                    using Garnet.server.Module; 
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

            var modulePath = Path.Combine(dir1, "TestModule.dll");
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
                    @"context.Initialize(""TestModule"", 1);
                    
                    context.RegisterCommand(""TestModule.SetIfPM"", new SetIfPMCustomCommand(), CommandType.ReadModifyWrite, 2,
                    new RespCommandsInfo { Name = ""TestModule.SETIFPM"", Arity = 4, FirstKey = 1, LastKey = 1, Step = 1,
                    Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.String | RespAclCategories.Write });
                    
                    context.RegisterTransaction(""TestModule.READWRITETX"", () => new ReadWriteTxn(), 3,
                    new RespCommandsInfo { Name = ""TestModule.READWRITETX"", Arity = 4, FirstKey = 1, LastKey = 3, Step = 1,
                    Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.Write });

                    var factory = new MyDictFactory();
                    context.RegisterType(factory);

                    context.RegisterCommand(""TestModule.MYDICTSET"", factory, new MyDictSet(), CommandType.ReadModifyWrite, 2,
                    new RespCommandsInfo { Name = ""TestModule.MYDICTSET"", Arity = 4, FirstKey = 1, LastKey = 1, Step = 1, 
                    Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.Write });

                    context.RegisterCommand(""TestModule.MYDICTGET"", factory, new MyDictGet(), CommandType.Read, 1,
                    new RespCommandsInfo { Name = ""TestModule.MYDICTGET"", Arity = 3, FirstKey = 1, LastKey = 1, Step = 1,
                    Flags = RespCommandFlags.ReadOnly, AclCategories = RespAclCategories.Read });

                    context.RegisterCommand(""TestModule.SUM"", new Sum());";

            var modulePath = CreateTestModule(onLoad);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resp = db.Execute($"MODULE", ["LOADCS", modulePath]);
            Assert.AreEqual("OK", (string)resp);

            // Test SETIFPM
            string key = "testkey";
            string value = "foovalue1";
            db.StringSet(key, value);
            var retValue = db.StringGet(key);
            Assert.AreEqual(value, retValue.ToString());

            string newValue = "foovalue2";
            resp = db.Execute("TestModule.SETIFPM", key, newValue, "foo");
            Assert.AreEqual("OK", (string)resp);
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue, retValue.ToString());

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            var result = db.Execute("TestModule.READWRITETX", key, writekey1, writekey2);
            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            retValue = db.StringGet(writekey1);
            Assert.IsNotNull(retValue);
            Assert.AreEqual(newValue, retValue.ToString());

            retValue = db.StringGet(writekey2);
            Assert.AreEqual(newValue, retValue.ToString());

            // Test MYDICTSET
            var dictKey = "dictkey";
            var dictField = "dictfield";
            var dictValue = "dictvalue";
            resp = db.Execute("TestModule.MYDICTSET", dictKey, dictField, dictValue);
            Assert.AreEqual("OK", (string)resp);

            var dictRetValue = db.Execute("TestModule.MYDICTGET", dictKey, dictField);
            Assert.AreEqual(dictValue, (string)dictRetValue);

            // Test SUM command
            db.StringSet("key1", "1");
            db.StringSet("key2", "2");
            db.StringSet("key3", "3");
            result = db.Execute("TestModule.SUM", "key1", "key2", "key3");
            Assert.IsNotNull(result);
            Assert.AreEqual("6", result.ToString());
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

            var modulePath = CreateTestModule(onLoad);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resp = db.Execute($"MODULE", ["LOADCS", modulePath, "arg0", "arg1"]);
            Assert.AreEqual("OK", (string)resp);
        }

        [Test]
        public void TestUninitializedModule()
        {
            var modulePath = CreateTestModule("");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            try
            {
                db.Execute($"MODULE", ["LOADCS", modulePath]);
                Assert.Fail("Module with empty OnLoad should not load successfully");
            }
            catch (RedisException ex)
            {
                Assert.AreEqual("ERR Error during module OnLoad", ex.Message);
            }
        }

        [Test]
        public void TestAlreadyLoadedModule()
        {
            var modulePath = CreateTestModule(
                @"context.Initialize(""TestAlreadyLoadedModule"", 1);");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute($"MODULE", ["LOADCS", modulePath]);
            try
            {
                db.Execute($"MODULE", ["LOADCS", modulePath]);
                Assert.Fail("Already loaded module should not successfully load again");
            }
            catch (RedisException ex)
            {
                Assert.AreEqual("ERR Error during module OnLoad", ex.Message);
            }
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
                Assert.AreEqual("ERR unable to access one or more binary files.", ex.Message);
            }
        }
    }
}