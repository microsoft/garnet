// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.server;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    internal class LuaScriptRunnerTests
    {
        [SetUp]
        public void Setup()
        {
        }

        [TearDown]
        public void TearDown()
        {
            //Nothing to dispose yet
        }

        [Test]
        public void CannotRunUnsafeScript()
        {
            Runner luarunner = new(null);
            string source;

            //try to load an assembly
            source = "luanet.load_assembly('mscorlib')";
            var resultRun = luarunner.RunScript(source, default, default).ToString();
            Assert.IsTrue(resultRun.Contains("attempt to index a nil value (global 'luanet')", StringComparison.OrdinalIgnoreCase));

            //try to call a OS function
            source = "os = require('os'); return os.time();";
            resultRun = luarunner.RunScript(source, default, default).returnValue.ToString();
            Assert.IsTrue(resultRun.Contains("attempt to call a nil value (global 'require')", StringComparison.OrdinalIgnoreCase));

            //try to execute the input stream
            source = "dofile();";
            resultRun = luarunner.RunScript(source, default, default).returnValue.ToString();
            Assert.IsTrue(resultRun.Contains("attempt to call a nil value (global 'dofile')", StringComparison.OrdinalIgnoreCase));

            //try to call a windows executable
            source = "require \"notepad\"";
            resultRun = luarunner.RunScript(source, default, default).returnValue.ToString();
            Assert.IsTrue(resultRun.Contains("attempt to call a nil value (global 'require')", StringComparison.OrdinalIgnoreCase));

            //try to call an os function
            source = "os.exit();";
            resultRun = luarunner.RunScript(source, default, default).returnValue.ToString();
            Assert.IsTrue(resultRun.Contains("attempt to index a nil value (global 'os')", StringComparison.OrdinalIgnoreCase));

            //try to include a new .net library
            source = "import ('System.Diagnostics');";
            resultRun = luarunner.RunScript(source, default, default).returnValue.ToString();
            Assert.IsTrue(resultRun.Contains("attempt to call a nil value (global 'import')", StringComparison.OrdinalIgnoreCase));
        }

        [Test]
        public void CanLoadScript()
        {
            Runner luarunner = new(null);

            //code with error
            var source = Encoding.ASCII.GetBytes("local;");
            var result = luarunner.LoadScript(source, out string error);
            Assert.AreEqual(1, result);
            Assert.IsTrue("Compilation error: [string \"local;\"]:1: <name> expected near ';'".Equals(error));

            //code without error
            source = Encoding.ASCII.GetBytes("local list; list = 1; return list;");
            result = luarunner.LoadScript(source, out error);
            Assert.AreEqual(0, result);
            Assert.IsTrue(error.Equals(""));
        }

        [Test]
        public void CanRunScript()
        {
            var valSB = "45";
            var valSB2 = "other";

            var keys = new (ArgSlice, bool)[1];
            var args = new string[] { valSB, valSB, valSB2 };
            var luaRunner = new Runner(null);

            // Run code without errors using args
            var source = "local list; list = ARGV[1] ; return list;";

            (bool success, object res) t = luaRunner.RunScript(source, keys, args);
            Assert.AreEqual("45", (string)t.res);
            Assert.AreEqual(true, t.success);

            // Run code with errors
            source = "local list; list = ; return list;";
            t = luaRunner.RunScript(source, keys, args);
            Assert.AreEqual("Compilation error: attempt to call a nil value", (string)t.res);
            Assert.AreEqual(false, t.success);
        }
    }
}
