// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

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
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"luanet.load_assembly('mscorlib')\"]:1: attempt to index a nil value (global 'luanet')", ex.Message);
            }

            // Try to call a OS function
            using (var runner = new LuaRunner("os = require('os'); return os.time();"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"os = require('os'); return os.time();\"]:1: attempt to call a nil value (global 'require')", ex.Message);
            }

            // Try to execute the input stream
            using (var runner = new LuaRunner("dofile();"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"dofile();\"]:1: attempt to call a nil value (global 'dofile')", ex.Message);
            }

            // Try to call a windows executable
            using (var runner = new LuaRunner("require \"notepad\""))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"require \"notepad\"\"]:1: attempt to call a nil value (global 'require')", ex.Message);
            }

            // Try to call an OS function
            using (var runner = new LuaRunner("os.exit();"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"os.exit();\"]:1: attempt to index a nil value (global 'os')", ex.Message);
            }

            // Try to include a new .net library
            using (var runner = new LuaRunner("import ('System.Diagnostics');"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"import ('System.Diagnostics');\"]:1: attempt to call a nil value (global 'import')", ex.Message);
            }
        }

        [Test]
        public void CanLoadScript()
        {
            // Code with error
            using (var runner = new LuaRunner("local;"))
            {
                var ex = Assert.Throws<GarnetException>(runner.CompileForRunner);
                ClassicAssert.AreEqual("Compilation error: [string \"local;\"]:1: <name> expected near ';'", ex.Message);
            }

            // Code without error
            using (var runner = new LuaRunner("local list; list = 1; return list;"))
            {
                runner.CompileForRunner();
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
                runner.CompileForRunner();
                var res = runner.RunForRunner(keys, args);
                ClassicAssert.AreEqual("arg1", res);
            }

            // Run code with errors
            using (var runner = new LuaRunner("local list; list = ; return list;"))
            {
                var ex = Assert.Throws<GarnetException>(runner.CompileForRunner);
                ClassicAssert.AreEqual("Compilation error: [string \"local list; list = ; return list;\"]:1: unexpected symbol near ';'", ex.Message);
            }
        }

        [Test]
        public void KeysAndArgsCleared()
        {
            using (var runner = new LuaRunner("return { KEYS[1], ARGV[1], KEYS[2], ARGV[2] }"))
            {
                runner.CompileForRunner();
                var res1 = runner.RunForRunner(["hello", "world"], ["fizz", "buzz"]);
                var obj1 = (object[])res1;
                ClassicAssert.AreEqual(4, obj1.Length);
                ClassicAssert.AreEqual("hello", (string)obj1[0]);
                ClassicAssert.AreEqual("fizz", (string)obj1[1]);
                ClassicAssert.AreEqual("world", (string)obj1[2]);
                ClassicAssert.AreEqual("buzz", (string)obj1[3]);

                var res2 = runner.RunForRunner(["abc"], ["def"]);
                var obj2 = (object[])res2;
                ClassicAssert.AreEqual(2, obj2.Length);
                ClassicAssert.AreEqual("abc", (string)obj2[0]);
                ClassicAssert.AreEqual("def", (string)obj2[1]);

                var res3 = runner.RunForRunner(["012", "345"], ["678"]);
                var obj3 = (object[])res3;
                ClassicAssert.AreEqual(3, obj3.Length);
                ClassicAssert.AreEqual("012", (string)obj3[0]);
                ClassicAssert.AreEqual("678", (string)obj3[1]);
                ClassicAssert.AreEqual("345", (string)obj3[2]);
            }
        }
    }
}