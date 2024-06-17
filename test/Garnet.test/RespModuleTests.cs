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

        [Test]
        public void TestModuleLoad()
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
                    "using Garnet.server.Module;" +
                    "using System.Collections.Generic;" +
                    "using Tsavorite.core;" +
                    "using Garnet;" +
                    "using Garnet.server;" +
                    "namespace TestGarnetModule " +
                    "{ " +
                    "   public class TestModule : IModule " +
                    "   { " +
                    "       public void OnLoad(ModuleLoadContext context, List<string> moduleArgs)" +
                    "       { " +
                    "            context.Initialize(\"TestModule\", 1); " +
                    "            context.RegisterCommand(\"TestModule.SetIfPM\", 2, CommandType.ReadModifyWrite, new SetIfPMCustomCommand()," +
                    "            new RespCommandsInfo{ Name = \"TestModule.SETIFPM\", Arity = 4, FirstKey = 1, LastKey = 1, Step = 1," +
                    "            Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.String | RespAclCategories.Write}); " +
                    "            context.RegisterTransaction(\"TestModule.READWRITETX\", 3, () => new ReadWriteTxn()," +
                    "            new RespCommandsInfo{ Name = \"TestModule.READWRITETX\", Arity = 4, FirstKey = 1, LastKey = 3, Step = 1," +
                    "            Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write, AclCategories = RespAclCategories.Write}); " +
                    "       } " +
                    "   } " +
                    "}");
            }

            var modulePath = Path.Combine(dir1, "TestModule.dll");
            var filesToCompile = new[] {
                testFilePath,
                Path.GetFullPath(@"../main/GarnetServer/Extensions/SetIfPM.cs", TestUtils.RootTestsProjectPath),
                Path.GetFullPath(@"../main/GarnetServer/Extensions/ReadWriteTxn.cs", TestUtils.RootTestsProjectPath)};
            TestUtils.CreateTestLibrary(null, referenceFiles, filesToCompile, modulePath);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resp = db.Execute($"MODULE", ["LOAD", modulePath]);
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

        }
    }
}