// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using NLua.Exceptions;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    internal class LuaScriptRunnerTests
    {
        [Test]
        public void CannotRunUnsafeScript()
        {
            // Try to load an assembly
            using (var runner = new LuaRunner("luanet.load_assembly('mscorlib')"))
            {
                runner.Compile();
                var ex = Assert.Throws<LuaScriptException>(() => runner.Run());
                Assert.AreEqual("[string \"luanet.load_assembly('mscorlib')\"]:1: attempt to index a nil value (global 'luanet')", ex.Message);
            }

            // Try to call a OS function
            using (var runner = new LuaRunner("os = require('os'); return os.time();"))
            {
                runner.Compile();
                var ex = Assert.Throws<LuaScriptException>(() => runner.Run());
                Assert.AreEqual("[string \"os = require('os'); return os.time();\"]:1: attempt to call a nil value (global 'require')", ex.Message);
            }

            // Try to execute the input stream
            using (var runner = new LuaRunner("dofile();"))
            {
                runner.Compile();
                var ex = Assert.Throws<LuaScriptException>(() => runner.Run());
                Assert.AreEqual("[string \"dofile();\"]:1: attempt to call a nil value (global 'dofile')", ex.Message);
            }

            // Try to call a windows executable
            using (var runner = new LuaRunner("require \"notepad\""))
            {
                runner.Compile();
                var ex = Assert.Throws<LuaScriptException>(() => runner.Run());
                Assert.AreEqual("[string \"require \"notepad\"\"]:1: attempt to call a nil value (global 'require')", ex.Message);
            }

            // Try to call an OS function
            using (var runner = new LuaRunner("os.exit();"))
            {
                runner.Compile();
                var ex = Assert.Throws<LuaScriptException>(() => runner.Run());
                Assert.AreEqual("[string \"os.exit();\"]:1: attempt to index a nil value (global 'os')", ex.Message);
            }

            // Try to include a new .net library
            using (var runner = new LuaRunner("import ('System.Diagnostics');"))
            {
                runner.Compile();
                var ex = Assert.Throws<LuaScriptException>(() => runner.Run());
                Assert.AreEqual("[string \"import ('System.Diagnostics');\"]:1: attempt to call a nil value (global 'import')", ex.Message);
            }
        }

        [Test]
        public void CanLoadScript()
        {
            // Code with error
            using (var runner = new LuaRunner("local;"))
            {
                var ex = Assert.Throws<GarnetException>(runner.Compile);
                Assert.AreEqual("Compilation error: [string \"local;\"]:1: <name> expected near ';'", ex.Message);
            }

            // Code without error
            using (var runner = new LuaRunner("local list; list = 1; return list;"))
            {
                runner.Compile();
            }
        }

        [Test]
        public void CanRunScript()
        {
            string[] keys = null;
            string[] args = ["arg1", "arg2", "arg3"];

            // Run code without errors
            using (var runner = new LuaRunner("local list; list = ARGV[1] ; return list;"))
            {
                runner.Compile();
                var res = runner.Run(keys, args);
                Assert.AreEqual("arg2", res);
            }

            // Run code with errors
            using (var runner = new LuaRunner("local list; list = ; return list;"))
            {
                var ex = Assert.Throws<GarnetException>(runner.Compile);
                Assert.AreEqual("Compilation error: [string \"local list; list = ; return list;\"]:1: unexpected symbol near ';'", ex.Message);
            }
        }
    }
}