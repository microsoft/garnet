﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
            using (var runner = new LuaRunner(new(), "luanet.load_assembly('mscorlib')"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"luanet.load_assembly('mscorlib')\"]:1: attempt to index a nil value (global 'luanet')", ex.Message);
            }

            // Try to call a OS function
            using (var runner = new LuaRunner(new(), "os = require('os'); return os.time();"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"os = require('os'); return os.time();\"]:1: attempt to call a nil value (global 'require')", ex.Message);
            }

            // Try to execute the input stream
            using (var runner = new LuaRunner(new(), "dofile();"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"dofile();\"]:1: attempt to call a nil value (global 'dofile')", ex.Message);
            }

            // Try to call a windows executable
            using (var runner = new LuaRunner(new(), "require \"notepad\""))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"require \"notepad\"\"]:1: attempt to call a nil value (global 'require')", ex.Message);
            }

            // Try to call an OS function
            using (var runner = new LuaRunner(new(), "os.exit();"))
            {
                runner.CompileForRunner();
                var ex = Assert.Throws<GarnetException>(() => runner.RunForRunner());
                ClassicAssert.AreEqual("[string \"os.exit();\"]:1: attempt to index a nil value (global 'os')", ex.Message);
            }

            // Try to include a new .net library
            using (var runner = new LuaRunner(new(), "import ('System.Diagnostics');"))
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
            using (var runner = new LuaRunner(new(), "local;"))
            {
                var ex = Assert.Throws<GarnetException>(runner.CompileForRunner);
                ClassicAssert.AreEqual("Compilation error: [string \"local;\"]:1: <name> expected near ';'", ex.Message);
            }

            // Code without error
            using (var runner = new LuaRunner(new(), "local list; list = 1; return list;"))
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
            using (var runner = new LuaRunner(new(), "local list; list = ARGV[1] ; return list;"))
            {
                runner.CompileForRunner();
                var res = runner.RunForRunner(keys, args);
                ClassicAssert.AreEqual("arg1", res);
            }

            // Run code with errors
            using (var runner = new LuaRunner(new(), "local list; list = ; return list;"))
            {
                var ex = Assert.Throws<GarnetException>(runner.CompileForRunner);
                ClassicAssert.AreEqual("Compilation error: [string \"local list; list = ; return list;\"]:1: unexpected symbol near ';'", ex.Message);
            }
        }

        [Test]
        public void KeysAndArgsCleared()
        {
            using (var runner = new LuaRunner(new(), "return { KEYS[1], ARGV[1], KEYS[2], ARGV[2] }"))
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

        [Test]
        public unsafe void LuaLimittedPOH()
        {
            const int Iters = 20;
            const int TotalAllocSizeBytes = 1_024 * 1_024;

            var rand = new Random(2024_12_16);  // Repeatable, but random

            // Special cases
            {
                var luaAlloc = new LuaLimittedPOHAllocator(TotalAllocSizeBytes);
                luaAlloc.CheckCorrectness();

                // 0 sized should work, and return the same thing each time
                ref var zero0 = ref luaAlloc.AllocateNew(0, out var failed0);
                ClassicAssert.IsFalse(failed0);
                ClassicAssert.IsFalse(Unsafe.IsNullRef(ref zero0));
                ref var zero1 = ref luaAlloc.AllocateNew(0, out var failed1);
                ClassicAssert.IsFalse(failed1);
                ClassicAssert.IsFalse(Unsafe.IsNullRef(ref zero1));

                ClassicAssert.IsTrue(Unsafe.AreSame(ref zero0, ref zero1));

                luaAlloc.Free(ref zero0, 0);
                luaAlloc.Free(ref zero1, 0);

                // Impossibly large fails
                ref var failedRef = ref luaAlloc.AllocateNew(TotalAllocSizeBytes * 2, out var failedLarge);
                ClassicAssert.IsTrue(failedLarge);
                ClassicAssert.IsTrue(Unsafe.IsNullRef(ref failedRef));

                luaAlloc.CheckCorrectness();
            }

            // Fill whole allocation
            {
                var luaAlloc = new LuaLimittedPOHAllocator(TotalAllocSizeBytes);
                var freeSpace = luaAlloc.FirstBlockSizeBytes;

                for (var i = 0; i < Iters; i++)
                {
                    for (var size = 1; size <= 64; size *= 2)
                    {
                        var lastSize = 0L;
                        var toFree = new List<nint>();
                        while (true)
                        {
                            ref var newData = ref luaAlloc.AllocateNew(size, out var failed);
                            if (failed)
                            {
                                break;
                            }
                            ClassicAssert.IsTrue(luaAlloc.AllocatedBytes > lastSize);
                            lastSize = luaAlloc.AllocatedBytes;

                            var into = new Span<byte>(Unsafe.AsPointer(ref newData), size);
                            into.Fill((byte)toFree.Count);

                            toFree.Add((nint)Unsafe.AsPointer(ref newData));
                        }
                        luaAlloc.CheckCorrectness();

                        for (var j = 0; j < toFree.Count; j++)
                        {
                            var ptr = toFree[j];

                            var into = new Span<byte>((void*)ptr, size);
                            ClassicAssert.IsFalse(into.ContainsAnyExcept((byte)j));

                            luaAlloc.Free(ref Unsafe.AsRef<byte>((void*)ptr), size);
                        }
                        luaAlloc.CheckCorrectness();

                        ClassicAssert.AreEqual(0, luaAlloc.AllocatedBytes);

                        _ = luaAlloc.TryCoalesceAllFreeBlocks();
                        luaAlloc.CheckCorrectness();

                        ClassicAssert.AreEqual(freeSpace, luaAlloc.FirstBlockSizeBytes);
                    }
                }
            }

            // Repeated realloc preserves data and doesn't move.
            {
                var luaAlloc = new LuaLimittedPOHAllocator(TotalAllocSizeBytes);
                var freeSpace = luaAlloc.FirstBlockSizeBytes;

                for (var i = 0; i < Iters; i++)
                {
                    for (var initialSize = 1; initialSize <= 64; initialSize *= 2)
                    {
                        ref var initialData = ref luaAlloc.AllocateNew(initialSize, out var failed);
                        ClassicAssert.IsFalse(failed);

                        var size = initialSize;
                        var val = 1;

                        ref var curData = ref initialData;

                        MemoryMarshal.CreateSpan(ref curData, initialSize).Fill((byte)val);

                        while (true)
                        {
                            var newSize = size + rand.Next(4 * 1024) + 1;
                            ref var newData = ref luaAlloc.ResizeAllocation(ref curData, size, newSize, out failed);
                            if (failed)
                            {
                                ClassicAssert.IsTrue(Unsafe.IsNullRef(ref newData));
                                break;
                            }

                            // Byte totals are believable
                            ClassicAssert.IsTrue(luaAlloc.AllocatedBytes >= newSize);

                            // Shouldn't have moved
                            ClassicAssert.IsTrue(Unsafe.AreSame(ref initialData, ref newData));

                            // Data preserved
                            var oldData = MemoryMarshal.CreateReadOnlySpan(ref newData, size);
                            ClassicAssert.IsFalse(oldData.ContainsAnyExcept((byte)val));

                            // Mutate to check for faults
                            val++;
                            MemoryMarshal.CreateSpan(ref newData, newSize).Fill((byte)val);

                            // Continue
                            size = newSize;
                            curData = ref newData;
                        }
                        luaAlloc.CheckCorrectness();

                        // Check final correctness
                        var finalData = MemoryMarshal.CreateReadOnlySpan(ref curData, size);
                        ClassicAssert.IsFalse(finalData.ContainsAnyExcept((byte)val));

                        // Hand the one block back, which should fully free everything
                        luaAlloc.Free(ref curData, size);

                        ClassicAssert.AreEqual(0, luaAlloc.AllocatedBytes);
                        ClassicAssert.AreEqual(freeSpace, luaAlloc.FirstBlockSizeBytes);

                        luaAlloc.CheckCorrectness();
                    }
                }
            }

            // Basic fixed sized allocs
            {
                const int AllocSize = 16;

                var luaAlloc = new LuaLimittedPOHAllocator(TotalAllocSizeBytes);
                var freeSpace = luaAlloc.FirstBlockSizeBytes;

                for (var i = 0; i < Iters; i++)
                {
                    ClassicAssert.AreEqual(0, luaAlloc.AllocatedBytes);
                    ClassicAssert.AreEqual(1, luaAlloc.FreeBlockCount);

                    var numOps = rand.Next(50) + 1;

                    var toFree = new List<nint>();

                    // Do a bunch of allocs, all should succeed
                    for (var j = 0; j < numOps; j++)
                    {
                        ref var newData = ref luaAlloc.AllocateNew(AllocSize, out var failed);
                        ClassicAssert.False(failed);
                        ClassicAssert.False(Unsafe.IsNullRef(ref newData));

                        var into = new Span<byte>(Unsafe.AsPointer(ref newData), AllocSize);
                        into.Fill((byte)j);

                        toFree.Add((nint)Unsafe.AsPointer(ref newData));
                    }
                    luaAlloc.CheckCorrectness();

                    // Each block should be served out of a split, so free list should stay at 1
                    ClassicAssert.AreEqual(1, luaAlloc.FreeBlockCount);

                    // Check that data wasn't corrupted
                    for (var j = 0; j < toFree.Count; j++)
                    {
                        var ptr = toFree[j];
                        var data = new Span<byte>((void*)ptr, AllocSize);
                        var expected = (byte)j;

                        ClassicAssert.IsFalse(data.ContainsAnyExcept(expected));
                    }

                    // Free in a random order
                    toFree = toFree.Select(p => (Pointer: p, Order: rand.Next())).OrderBy(t => t.Order).Select(t => t.Pointer).ToList();
                    foreach (var ptr in toFree)
                    {
                        ref var asData = ref Unsafe.AsRef<byte>((void*)ptr);

                        luaAlloc.Free(ref asData, AllocSize);
                    }
                    luaAlloc.CheckCorrectness();

                    // Check that all free's didn't corrupt anything
                    ClassicAssert.AreEqual(0, luaAlloc.AllocatedBytes);

                    // Check that all memory is reclaimable
                    _ = luaAlloc.TryCoalesceAllFreeBlocks();
                    ClassicAssert.AreEqual(freeSpace, luaAlloc.FirstBlockSizeBytes);
                    luaAlloc.CheckCorrectness();
                }
            }

            // Random operations with variable sized allocs
            {
                var luaAlloc = new LuaLimittedPOHAllocator(TotalAllocSizeBytes);
                var freeSpace = luaAlloc.FirstBlockSizeBytes;

                for (var i = 0; i < Iters; i++)
                {
                    ClassicAssert.AreEqual(0, luaAlloc.AllocatedBytes);
                    ClassicAssert.AreEqual(1, luaAlloc.FreeBlockCount);

                    var toFree = new List<(nint Pointer, byte Expected, int AllocSize)>();
                    for (var j = 0; j < 1_000; j++)
                    {
                        var op = rand.Next(4);
                        switch (op)
                        {
                            // Allocate
                            case 0:
                                {
                                    var allocSize = rand.Next(4 * 1024) + 1;

                                    ref var newData = ref luaAlloc.AllocateNew(allocSize, out var failed);
                                    ClassicAssert.IsFalse(failed);
                                    ClassicAssert.IsFalse(Unsafe.IsNullRef(ref newData));

                                    var into = new Span<byte>(Unsafe.AsPointer(ref newData), allocSize);
                                    into.Fill((byte)j);

                                    toFree.Add(((nint)Unsafe.AsPointer(ref newData), (byte)j, allocSize));
                                }

                                break;

                            // Reallocate
                            case 1:
                                {
                                    if (toFree.Count == 0)
                                    {
                                        goto case 0;
                                    }

                                    var reallocIx = rand.Next(toFree.Count);
                                    var (ptr, expected, size) = toFree[reallocIx];

                                    ref var reallocRef = ref Unsafe.AsRef<byte>((void*)ptr);

                                    int newSizeBytes;
                                    if (rand.Next(2) == 0)
                                    {
                                        newSizeBytes = size + rand.Next(32) + 1;
                                    }
                                    else
                                    {
                                        newSizeBytes = size - rand.Next(size);
                                        if (newSizeBytes == 0)
                                        {
                                            goto case 0;
                                        }
                                    }

                                    ref var updatedRef = ref luaAlloc.ResizeAllocation(ref reallocRef, size, newSizeBytes, out var failed);
                                    ClassicAssert.IsFalse(failed);

                                    if (newSizeBytes <= size)
                                    {
                                        // Shrink should always leave in place
                                        ClassicAssert.IsTrue(Unsafe.AreSame(ref updatedRef, ref reallocRef));
                                    }

                                    var toCheck = MemoryMarshal.CreateReadOnlySpan(ref updatedRef, Math.Min(size, newSizeBytes));
                                    ClassicAssert.IsFalse(toCheck.ContainsAnyExcept(expected));

                                    toFree.RemoveAt(reallocIx);

                                    var toFill = MemoryMarshal.CreateSpan(ref updatedRef, newSizeBytes);
                                    toFill.Fill((byte)j);

                                    toFree.Add(((nint)Unsafe.AsPointer(ref updatedRef), (byte)j, newSizeBytes));
                                }

                                break;

                            // Free
                            case 2:
                                {
                                    if (toFree.Count == 0)
                                    {
                                        goto case 0;
                                    }

                                    var freeIx = rand.Next(toFree.Count);
                                    var (ptr, expected, size) = toFree[freeIx];

                                    toFree.RemoveAt(freeIx);
                                    luaAlloc.Free(ref Unsafe.AsRef<byte>((void*)ptr), size);
                                }

                                break;

                            // Validate
                            case 3:
                                {
                                    if (toFree.Count == 0)
                                    {
                                        goto case 0;
                                    }

                                    foreach (var (ptr, expected, size) in toFree)
                                    {
                                        var data = new Span<byte>((void*)ptr, size);
                                        ClassicAssert.IsFalse(data.ContainsAnyExcept(expected));
                                    }
                                }

                                break;

                            default:
                                ClassicAssert.Fail("Unexpected operation");
                                break;
                        }
                        luaAlloc.CheckCorrectness();
                    }
                    luaAlloc.CheckCorrectness();

                    // Validate and free everything that's left
                    foreach (var (ptr, expected, size) in toFree)
                    {
                        ref var asData = ref Unsafe.AsRef<byte>((void*)ptr);

                        var data = new Span<byte>((void*)ptr, size);
                        ClassicAssert.IsFalse(data.ContainsAnyExcept(expected));

                        luaAlloc.Free(ref asData, size);
                    }
                    luaAlloc.CheckCorrectness();

                    // Check that all free's didn't corrupt anything
                    ClassicAssert.AreEqual(0, luaAlloc.AllocatedBytes);

                    // Full coalesce gets contiguous blocks back
                    _ = luaAlloc.TryCoalesceAllFreeBlocks();
                    ClassicAssert.AreEqual(freeSpace, luaAlloc.FirstBlockSizeBytes);

                    luaAlloc.CheckCorrectness();
                }
            }
        }
    }
}