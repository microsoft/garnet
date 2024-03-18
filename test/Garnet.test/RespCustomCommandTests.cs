// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespCustomCommandTests
    {
        GarnetServer server;
        private string _extTestDir1;
        private string _extTestDir2;

        [SetUp]
        public void Setup()
        {
            _extTestDir1 = Path.Combine(TestUtils.MethodTestDir, "test1");
            _extTestDir2 = Path.Combine(TestUtils.MethodTestDir, "test2");

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                disablePubSub: true,
                extensionBinPaths: new[] { _extTestDir1, _extTestDir2 },
                extensionAllowUnsignedAssemblies: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.DeleteDirectory(Directory.GetParent(_extTestDir1)?.FullName);
        }

        [Test]
        public void CustomCommandTest1()
        {
            // Register sample custom command (SETIFPM = "set if prefix match")
            int x = server.Register.NewCommand("SETIFPM", 2, CommandType.ReadModifyWrite, new SetIfPMCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "foovalue0";
            db.StringSet(key, origValue);

            string newValue1 = "foovalue1";
            db.Execute("SETIFPM", key, newValue1, "foo");

            // This conditional set should pass (prefix matches)
            string retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue);

            // This conditional set should fail (prefix does not match)
            string newValue2 = "foovalue2";
            db.Execute("SETIFPM", key, newValue2, "bar");

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue);

            // This conditional set should pass (prefix matches)
            // New value is smaller than existing
            string newValue3 = "fooval3";
            db.Execute("SETIFPM", key, newValue3, "foo");

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue3, retValue);


            // This conditional set should pass (prefix matches)
            // New value is smaller than existing
            string newValue4 = "foolargervalue4";
            db.Execute("SETIFPM", key, newValue4, "foo");

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue4, retValue);
        }

        [Test]
        public void CustomCommandTest2()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should fail (wrong prefix size)
            bool exception = false;
            try
            {
                db.Execute("SETWPIFPGT", key, origValue, "foo");
            }
            catch (Exception ex)
            {
                exception = true;
                Assert.AreEqual(SetWPIFPGTCustomCommand.PrefixError, ex.Message);
            }
            Assert.IsTrue(exception);
            string retValue = db.StringGet(key);
            Assert.AreEqual(null, retValue);

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            retValue = db.StringGet(key);
            Assert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should fail (wrong prefix size)
            var newValue1 = "foovalue1";
            exception = false;
            try
            {
                db.Execute("SETWPIFPGT", key, newValue1, "foobar123");
            }
            catch (Exception ex)
            {
                exception = true;
                Assert.AreEqual(SetWPIFPGTCustomCommand.PrefixError, ex.Message);
            }
            Assert.IsTrue(exception);
            retValue = db.StringGet(key);
            Assert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should pass (prefix is greater)
            db.Execute("SETWPIFPGT", key, newValue1, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue.Substring(8));

            // This conditional set should fail (prefix is not greater)
            var newValue2 = "foovalue2";
            db.Execute("SETWPIFPGT", key, newValue2, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue.Substring(8));

            // This conditional set should pass (prefix is greater)
            // New value is smaller than existing
            var newValue3 = "fooval3";
            db.Execute("SETWPIFPGT", key, newValue3, BitConverter.GetBytes((long)3));
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue3, retValue.Substring(8));

            // This conditional set should pass (prefix is greater)
            // New value is larger than existing
            var newValue4 = "foolargervalue4";
            db.Execute("SETWPIFPGT", key, newValue4, BitConverter.GetBytes((long)4));
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue4, retValue.Substring(8));

            // This conditional set should pass (prefix is greater)
            var newValue5 = "foolargervalue4";
            db.Execute("SETWPIFPGT", key, newValue4, BitConverter.GetBytes(long.MaxValue));
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue5, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest3()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            string retValue = db.StringGet(key);
            Assert.AreEqual(retValue.Substring(8), origValue);

            db.KeyExpire(key, TimeSpan.FromSeconds(1));
            Thread.Sleep(1100);

            // Key expired, return fails
            retValue = db.StringGet(key);
            Assert.AreEqual(null, retValue);

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest4()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            string retValue = db.StringGet(key);
            Assert.AreEqual(retValue.Substring(8), origValue);

            db.KeyExpire(key, TimeSpan.FromSeconds(100));

            // Key not expired
            retValue = db.StringGet(key);
            Assert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should pass
            var newValue2 = "foovalue2";
            db.Execute("SETWPIFPGT", key, newValue2, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue2, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest5()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            string retValue = db.StringGet(key);
            Assert.AreEqual(retValue.Substring(8), origValue);

            db.KeyExpire(key, TimeSpan.FromSeconds(100));

            // This conditional set should pass
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue.Substring(8));

            // Expiration should survive operation
            var ttl = db.KeyTimeToLive(key);
            Assert.IsTrue(ttl > TimeSpan.FromSeconds(10));

            retValue = db.StringGet(key);
            Assert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should pass
            var newValue2 = "foovalue2";
            db.Execute("SETWPIFPGT", key, newValue2, BitConverter.GetBytes((long)2));

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue2, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest6()
        {
            // Register sample custom command (SETIFPM = "set if prefix match")
            server.Register.NewCommand("DELIFM", 1, CommandType.ReadModifyWrite, new DeleteIfMatchCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "foovalue0";
            db.StringSet(key, origValue);

            // DELIFM with different value
            string newValue1 = "foovalue1";
            db.Execute("DELIFM", key, newValue1);

            // Delete should not have happened, as value does not match
            string retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue);

            // DELIFM with same value
            db.Execute("DELIFM", key, origValue);

            // Delete should have happened, as value matches
            retValue = db.StringGet(key);
            Assert.AreEqual(null, retValue);
        }

        [Test]
        public void CustomObjectCommandTest1()
        {
            // Register sample custom command on object
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", 2, CommandType.ReadModifyWrite, factory);
            server.Register.NewCommand("MYDICTGET", 1, CommandType.Read, factory);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string mainkey = "key";

            string key1 = "mykey1";
            string value1 = "foovalue1";
            db.Execute("MYDICTSET", mainkey, key1, value1);

            var retValue = db.Execute("MYDICTGET", mainkey, key1);
            Assert.AreEqual(value1, (string)retValue);

            var result = db.Execute("MEMORY", "USAGE", mainkey);
            var actualValue = ResultType.Integer == result.Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 272;
            Assert.AreEqual(expectedResponse, actualValue);

            string key2 = "mykey2";
            string value2 = "foovalue2";
            db.Execute("MYDICTSET", mainkey, key2, value2);

            retValue = db.Execute("MYDICTGET", mainkey, key2);
            Assert.AreEqual(value2, (string)retValue);

            result = db.Execute("MEMORY", "USAGE", mainkey);
            actualValue = ResultType.Integer == result.Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 408;
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CustomCommandSetWhileKeyHasTtlTest()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 5;
            string key = "mykey";
            string origValue = "foovalue0";
            long prefix = 0;
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes(prefix));
            string retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue.Substring(8));

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            var time = db.KeyTimeToLive(key);
            Assert.IsTrue(time.Value.Seconds > 0);

            // This conditional set should pass (new prefix is greater)
            string newValue1 = "foovalue1";
            prefix = 1;
            db.Execute("SETWPIFPGT", key, newValue1, BitConverter.GetBytes(prefix));
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue.Substring(8));

            Thread.Sleep((expire + 1) * 1000);
            string value = db.StringGet(key);
            Assert.AreEqual(null, value);
        }

        [Test]
        public void CustomCommandSetAfterKeyDeletedWithTtlTest()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 1;
            string key = "mykey";
            string origValue = "foovalue0";
            long prefix = 0;
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes(prefix));

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            // Key should expire
            Thread.Sleep((expire + 1) * 1000);
            string value = db.StringGet(key);
            Assert.AreEqual(null, value);

            // Setting on they key that was deleted with a custom command should succeed
            string newValue1 = "foovalue10";
            prefix = 1;
            db.Execute("SETWPIFPGT", key, newValue1, BitConverter.GetBytes(prefix));
            string retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue.Substring(8));
        }

        [Test]
        public void CustomObjectCommandTest2()
        {
            // Register sample custom command on object
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", 2, CommandType.ReadModifyWrite, factory);
            server.Register.NewCommand("MYDICTGET", 1, CommandType.Read, factory);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string mainkey = "key";

            string key1 = "mykey1";
            string value1 = "foovalue1";
            db.Execute("MYDICTSET", mainkey, key1, value1);

            var retValue = db.Execute("MYDICTGET", mainkey, key1);
            Assert.AreEqual(value1, (string)retValue);

            db.KeyExpire(mainkey, TimeSpan.FromSeconds(1));
            Thread.Sleep(1100);

            retValue = db.Execute("MYDICTGET", mainkey, key1);
            Assert.AreEqual(null, (string)retValue);

            string key2 = "mykey2";
            string value2 = "foovalue2";
            db.Execute("MYDICTSET", mainkey, key2, value2);

            retValue = db.Execute("MYDICTGET", mainkey, key1);
            Assert.AreEqual(null, (string)retValue);

            retValue = db.Execute("MYDICTGET", mainkey, key2);
            Assert.AreEqual(value2, (string)retValue);
        }


        [Test]
        public async Task CustomCommandSetFollowedByTtlTestAsync()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 180;
            string key = "mykeyKeyExpire";
            string origValue = "foovalue0";
            long prefix = 0;

            await db.ExecuteAsync("SETWPIFPGT", key, origValue, BitConverter.GetBytes(prefix));
            await db.KeyExpireAsync(key, TimeSpan.FromMinutes(expire));

            string retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue.Substring(8));

            string newValue1 = "foovalue10";
            prefix = 1;
            await db.ExecuteAsync("SETWPIFPGT", key, newValue1, BitConverter.GetBytes(prefix));
            await db.KeyExpireAsync(key, TimeSpan.FromMinutes(expire));

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue.Substring(8));
        }

        [Test]
        public async Task CustomCommandSetWithCustomExpirationTestAsync()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGTE", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(),
                expirationTicks: TimeSpan.FromSeconds(4).Ticks); // provide default expiration at registration time

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykeyCommandExpire";
            string origValue = "foovalue0";
            long prefix = 0;

            await db.ExecuteAsync("SETWPIFPGTE", key, origValue, BitConverter.GetBytes(prefix));

            string retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue.Substring(8));

            string newValue1 = "foovalue10";
            prefix = 1;
            await db.ExecuteAsync("SETWPIFPGTE", key, newValue1, BitConverter.GetBytes(prefix));

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue.Substring(8));

            Thread.Sleep(5000);

            // should be expired now
            retValue = db.StringGet(key);
            Assert.AreEqual(null, retValue);
        }

        public void CreateTestLibrary(string[] namespaces, string[] referenceFiles, string[] filesToCompile, string dstFilePath)
        {
            if (File.Exists(dstFilePath))
            {
                File.Delete(dstFilePath);
            }

            foreach (var referenceFile in referenceFiles)
            {
                Assert.IsTrue(File.Exists(referenceFile));
            }

            var references = referenceFiles.Select(f => MetadataReference.CreateFromFile(f));

            foreach (var fileToCompile in filesToCompile)
            {
                Assert.IsTrue(File.Exists(fileToCompile));
            }

            var parseFunc = new Func<string, SyntaxTree>(filePath =>
            {
                var source = File.ReadAllText(filePath);
                var stringText = SourceText.From(source, Encoding.UTF8);
                return SyntaxFactory.ParseSyntaxTree(stringText,
                    CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.CSharp10), string.Empty);
            });

            var syntaxTrees = filesToCompile.Select(f => parseFunc(f));

            var compilationOptions = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                .WithAllowUnsafe(true)
                .WithOverflowChecks(true)
                .WithOptimizationLevel(OptimizationLevel.Release)
                .WithUsings(namespaces);


            var compilation = CSharpCompilation.Create(Path.GetFileName(dstFilePath), syntaxTrees, references, compilationOptions);

            try
            {
                var result = compilation.Emit(dstFilePath);
                Assert.IsTrue(result.Success);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        private string[] CreateTestLibraries()
        {
            var runtimePath = RuntimeEnvironment.GetRuntimeDirectory();
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            Assert.IsNotNull(binPath);

            var namespaces = new[]
            {
                "Tsavorite.core",
                "Garnet.common",
                "Garnet.server",
                "System",
                "System.Buffers",
                "System.Collections.Generic",
                "System.Diagnostics",
                "System.IO",
                "System.Text",
            };

            var referenceFiles = new[]
            {
                Path.Combine(runtimePath, "System.dll"),
                Path.Combine(runtimePath, "System.Collections.dll"),
                Path.Combine(runtimePath, "System.Core.dll"),
                Path.Combine(runtimePath, "System.Private.CoreLib.dll"),
                Path.Combine(runtimePath, "System.Runtime.dll"),
                Path.Combine(binPath, $"Tsavorite.core.dll"),
                Path.Combine(binPath, $"Garnet.common.dll"),
                Path.Combine(binPath, $"Garnet.server.dll"),
            };

            var dir1 = Path.Combine(this._extTestDir1, Path.GetFileName(TestUtils.MethodTestDir));
            var dir2 = Path.Combine(this._extTestDir2, Path.GetFileName(TestUtils.MethodTestDir));

            if (!Directory.Exists(this._extTestDir1))
                Directory.CreateDirectory(this._extTestDir1);

            if (!Directory.Exists(dir1))
                Directory.CreateDirectory(dir1);

            if (!Directory.Exists(this._extTestDir2))
                Directory.CreateDirectory(this._extTestDir2);

            if (!Directory.Exists(dir2))
                Directory.CreateDirectory(dir2);

            var testFilePath = Path.Combine(TestUtils.MethodTestDir, "test.cs");
            using (var testFile = File.CreateText(testFilePath))
            {
                testFile.WriteLine("namespace Garnet { public class TestClass { } }");
            }

            var libPathToFiles = new Dictionary<string, string[]>
            {
                { Path.Combine(dir1, "testLib1.dll"), new [] {@"../../../../../../main/GarnetServer/Extensions/MyDictObject.cs"}},
                { Path.Combine(dir2, "testLib2.dll"), new [] {@"../../../../../../main/GarnetServer/Extensions/SetIfPM.cs"}},
                { Path.Combine(dir2, "testLib3.dll"), new []
                {
                    @"../../../../../../main/GarnetServer/Extensions/ReadWriteTxn.cs",
                    testFilePath,
                }}
            };

            foreach (var ltf in libPathToFiles)
            {
                this.CreateTestLibrary(namespaces, referenceFiles, ltf.Value, ltf.Key);
            }

            var notAllowedPath = Path.Combine(TestUtils.MethodTestDir, "testLib1.dll");
            if (!File.Exists(notAllowedPath))
            {
                File.Copy(Path.Combine(dir1, "testLib1.dll"), notAllowedPath);
            }

            return new[] { Path.Combine(dir1, "testLib1.dll"), dir2 };
        }

        [Test]
        public void RegisterCustomCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var libraryPaths = this.CreateTestLibraries();

            var args = new List<object>
            {
                "TXN", "READWRITETX", 3, "ReadWriteTxn",
                "RMW", "SETIFPM", 2, "SetIfPMCustomCommand", TimeSpan.FromSeconds(10).Ticks,
                "RMW", "MYDICTSET", 2, "MyDictFactory",
                "READ", "MYDICTGET", 1, "MyDictFactory",
                "SRC",
            };
            args.AddRange(libraryPaths);

            // Register select custom commands and transactions
            var resp = (string)db.Execute($"REGISTERCS",
                args.ToArray());

            // Test READWRITETX
            string key = "readkey";
            string value = "foovalue0";
            db.StringSet(key, value);

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            var result = db.Execute("READWRITETX", key, writekey1, writekey2);
            Assert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(writekey1);
            Assert.IsNotNull(retValue);
            Assert.AreEqual(value, retValue);

            retValue = db.StringGet(writekey2);
            Assert.AreEqual(value, retValue);

            // Test SETIFPM
            string newValue1 = "foovalue1";
            string newValue2 = "foovalue2";

            // This conditional set should pass (prefix matches)
            result = db.Execute("SETIFPM", key, newValue1, "foo");
            Assert.AreEqual("OK", (string)result);

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue);

            // This conditional set should fail (prefix does not match)
            result = db.Execute("SETIFPM", key, newValue2, "bar");
            Assert.AreEqual("OK", (string)result);

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue);

            // Test MYDICTSET
            string newKey1 = "newkey1";
            string newKey2 = "newkey2";

            db.Execute("MYDICTSET", key, newKey1, newValue1);

            var dictVal = db.Execute("MYDICTGET", key, newKey1);
            Assert.AreEqual(newValue1, (string)dictVal);

            db.Execute("MYDICTSET", key, newKey2, newValue2);

            // Test MYDICTGET
            dictVal = db.Execute("MYDICTGET", key, newKey2);
            Assert.AreEqual(newValue2, (string)dictVal);
        }

        [Test]
        public void RegisterCustomCommandErrorConditionsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var libraryPaths = this.CreateTestLibraries();

            // Malformed request #1 - no arguments
            string resp = null;
            try
            {
                resp = (string)db.Execute($"REGISTERCS");
            }
            catch (RedisServerException rse)
            {
                Assert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_MALFORMED_REGISTERCS_COMMAND), $"-{rse.Message}\r\n");
            }
            Assert.IsNull(resp);

            // Malformed request #2 - binary paths before sub-command
            var args = new List<object>() { "SRC" };
            args.AddRange(libraryPaths);
            args.AddRange(new object[] { "TXN", "READWRITETX", 3, "ReadWriteTxn" });

            try
            {
                resp = (string)db.Execute($"REGISTERCS", args.ToArray());
            }
            catch (RedisServerException rse)
            {
                Assert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_MALFORMED_REGISTERCS_COMMAND), $"-{rse.Message}\r\n");
            }
            Assert.IsNull(resp);

            // Binary file not contained in allowed paths
            args = new List<object>
            {
                "RMW", "MYDICTSET", 2, "MyDictFactory",
                "SRC", Path.Combine(TestUtils.MethodTestDir, "testLib1.dll")
            };

            try
            {
                resp = (string)db.Execute($"REGISTERCS", args.ToArray());
            }
            catch (RedisServerException rse)
            {
                Assert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERROR_BINARY_FILES_NOT_IN_ALLOWED_PATHS), $"-{rse.Message}\r\n");
            }
            Assert.IsNull(resp);

            // Class not in supplied dlls
            args = new List<object>
            {
                "RMW", "MYDICTSET", 2, "MyDictFactory",
                "SRC",
            };
            args.AddRange(libraryPaths.Skip(1));

            try
            {
                resp = (string)db.Execute($"REGISTERCS", args.ToArray());
            }
            catch (RedisServerException rse)
            {
                Assert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERROR_INSTANTIATING_CLASS), $"-{rse.Message}\r\n");
            }
            Assert.IsNull(resp);

            // Class not in supported
            args = new List<object>
            {
                "RMW", "MYDICTSET", 2, "TestClass",
                "SRC",
            };
            args.AddRange(libraryPaths);

            try
            {
                resp = (string)db.Execute($"REGISTERCS", args.ToArray());
            }
            catch (RedisServerException rse)
            {
                Assert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERROR_REGISTERCS_UNSUPPORTED_CLASS), $"-{rse.Message}\r\n");
            }
            Assert.IsNull(resp);
        }
    }
}