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
                Path.Combine(binPath, "Garnet.server.dll"),
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
                    "namespace TestGarnetModule " +
                    "{ " +
                    "   public class TestModule : IModule " +
                    "   { " +
                    "       public void OnLoad(List<string> moduleArgs)" +
                    "       { } " +
                    "   } " +
                    "}");
            }

            var modulePath = Path.Combine(dir1, "TestModule.dll");
            TestUtils.CreateTestLibrary(null, referenceFiles, [testFilePath], modulePath);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resp = (string)db.Execute($"MODULE", ["LOAD", modulePath]);
        }
    }
}