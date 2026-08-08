// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Tests for the mimalloc-backed <see cref="SectorAlignedBufferPool"/> (native-allocator "buffer-pool" mode).
    /// The pool's native hook is a process-global static (mirroring <c>Disabled</c>/<c>UnpinOnReturn</c>), so this
    /// fixture is <see cref="NonParallelizableAttribute"/> and resets the hook in teardown.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public unsafe class NativeAllocatorTests
    {
        const int SectorSize = 512;

        [TearDown]
        public void TearDown() => SectorAlignedBufferPool.NativeAllocator = null;

        static void RequireMimalloc()
        {
            if (!Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");
        }

        [Test]
        public void MimallocLoads()
        {
            RequireMimalloc();
            ClassicAssert.IsTrue(Mimalloc.Available);
        }

        [Test]
        public void NativePoolGetReturnRoundTrips()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            var page = pool.Get(1000);
            try
            {
                ClassicAssert.AreEqual(0, ((long)page.aligned_pointer) % SectorSize, "aligned_pointer must be sector-aligned");
                ClassicAssert.GreaterOrEqual(page.AlignedTotalCapacity, 1000);
                ClassicAssert.IsNull(page.buffer, "native-backed page must have no managed array");

                // Default clearOnReturn:true maps to mi_zalloc -> zeroed.
                for (var i = 0; i < page.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, page.aligned_pointer[i]);

                // Round-trip a pattern through the native buffer.
                for (var i = 0; i < 1000; i++)
                    page.aligned_pointer[i] = (byte)(i & 0xFF);
                for (var i = 0; i < 1000; i++)
                    ClassicAssert.AreEqual((byte)(i & 0xFF), page.aligned_pointer[i]);
            }
            finally
            {
                page.Return();
            }
        }

        [Test]
        public void NativeTrackerReflectsMimallocCommit()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            // Native usage is read on demand from mimalloc's committed stats (not per-op), so hold several
            // large buffers so mimalloc must have committed memory, then assert the tracker reflects it.
            var pages = new System.Collections.Generic.List<SectorAlignedMemory>();
            for (var i = 0; i < 64; i++)
                pages.Add(pool.Get(64 * 1024));
            try
            {
                ClassicAssert.Greater(NativeMemoryTracker.Bytes, 0, "tracker should reflect mimalloc committed bytes");
            }
            finally
            {
                foreach (var p in pages)
                    p.Return();
            }
        }

        [Test]
        public void NativeCrossThreadReturnIsSafe()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            var page = pool.Get(2048);
            page.aligned_pointer[0] = 0x42;

            // Rent on this thread, free on another: exercises mimalloc's cross-thread free path (the scenario
            // PR #2018 hand-rolled with origin-stripe return tracking). Must not throw or corrupt state.
            Exception captured = null;
            var t = new Thread(() =>
            {
                try { page.Return(); }
                catch (Exception e) { captured = e; }
            });
            t.Start();
            t.Join();

            ClassicAssert.IsNull(captured, "cross-thread Return must not throw");

            // Pool remains usable after a cross-thread free.
            var page2 = pool.Get(2048);
            try
            {
                ClassicAssert.AreEqual(0, ((long)page2.aligned_pointer) % SectorSize);
            }
            finally
            {
                page2.Return();
            }
        }

        [Test]
        public void NativeWrapperIsRecycled()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            // Get+Return then Get again on the same thread should recycle the wrapper object (no Gen0 churn),
            // returning a usable, correctly-aligned buffer.
            var p1 = pool.Get(1024);
            p1.Return();
            var p2 = pool.Get(1024);
            try
            {
                ClassicAssert.AreEqual(0, ((long)p2.aligned_pointer) % SectorSize);
                ClassicAssert.GreaterOrEqual(p2.AlignedTotalCapacity, 1024);
            }
            finally
            {
                p2.Return();
            }
        }

        [Test]
        public void DirectVmAllocateIsZeroedAlignedWritable()
        {
            var block = DirectVirtualMemory.Allocate(1 << 20, 4096);   // 1 MB, 4 KB aligned
            try
            {
                ClassicAssert.IsFalse(block.IsEmpty);
                ClassicAssert.AreEqual(0, ((long)block.AlignedPtr) % 4096, "must be aligned");
                var span = new Span<byte>((void*)block.AlignedPtr, 1 << 20);
                foreach (var b in span)
                    ClassicAssert.AreEqual(0, b, "fresh OS mapping must be demand-zero");
                span.Fill(0xCD);
                ClassicAssert.AreEqual(0xCD, span[(1 << 20) - 1], "must be writable end-to-end");
            }
            finally
            {
                DirectVirtualMemory.Free(block);
            }
        }

        [Test]
        public void DirectVmTrackerReflectsAllocation()
        {
            var before = NativeMemoryTracker.Bytes;
            var block = DirectVirtualMemory.Allocate(8L << 20, 512);   // 8 MB
            var afterAlloc = NativeMemoryTracker.Bytes;
            ClassicAssert.GreaterOrEqual(afterAlloc - before, 8L << 20, "tracker should reflect the direct-VM reservation");
            DirectVirtualMemory.Free(block);
            var afterFree = NativeMemoryTracker.Bytes;
            ClassicAssert.Less(afterFree - before, 8L << 20, "tracker should drop after free");
        }

        // The mimalloc exports the P/Invoke bindings resolve (Mimalloc.GetExport). A shipped prebuilt that omits
        // any of these would fail at load on that platform, so the packaging test below asserts they are present.
        static readonly string[] RequiredMimallocExports =
        [
            "mi_malloc", "mi_malloc_aligned", "mi_zalloc_aligned", "mi_free", "mi_usable_size", "mi_collect", "mi_process_info"
        ];

        /// <summary>
        /// Packaging regression guard for the prebuilt mimalloc binaries shipped per-RID under
        /// <c>runtimes/&lt;rid&gt;/native/</c>. Runs on any host (it parses the PE/ELF export table, it does not
        /// load or execute the binary), so CI on Linux still validates the Windows <c>mimalloc.dll</c>: the file
        /// must be present in the build output and export every symbol the P/Invoke layer binds. Catches a missing
        /// binary (the csproj glob broke), a truncated copy, or a rebuild that dropped exports.
        /// </summary>
        [Test]
        [TestCase("win-x64", "mimalloc.dll")]
        [TestCase("linux-x64", "libmimalloc.so")]
        public void ShippedMimallocBinaryExportsRequiredSymbols(string rid, string fileName)
        {
            var path = System.IO.Path.Combine(AppContext.BaseDirectory, "runtimes", rid, "native", fileName);
            ClassicAssert.IsTrue(System.IO.File.Exists(path),
                $"prebuilt mimalloc for {rid} not found at '{path}' — the Native/runtimes/**/native packaging glob must ship it to consumers");

            var bytes = System.IO.File.ReadAllBytes(path);
            var exports = fileName.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
                ? PeExports.Read(bytes)
                : ElfDynamicSymbols.Read(bytes);

            foreach (var sym in RequiredMimallocExports)
                ClassicAssert.Contains(sym, exports, $"{rid}/{fileName} must export '{sym}' (bound by Mimalloc.GetExport)");
        }
    }

    /// <summary>Minimal PE (PE32/PE32+) export-directory reader — names only. Enough to validate a shipped DLL's
    /// export table from any host without loading it. Not a general PE parser.</summary>
    static class PeExports
    {
        public static System.Collections.Generic.List<string> Read(byte[] b)
        {
            uint peOff = BitConverter.ToUInt32(b, 0x3C);                       // e_lfanew
            if (BitConverter.ToUInt32(b, (int)peOff) != 0x00004550)           // "PE\0\0"
                throw new InvalidOperationException("not a PE image");
            int coff = (int)peOff + 4;
            ushort numSections = BitConverter.ToUInt16(b, coff + 2);
            ushort optSize = BitConverter.ToUInt16(b, coff + 16);
            int opt = coff + 20;
            ushort magic = BitConverter.ToUInt16(b, opt);                     // 0x10b PE32, 0x20b PE32+
            // Export directory is data directory entry 0; its RVA sits at a fixed offset into the optional header.
            int dirOff = opt + (magic == 0x20b ? 112 : 96);
            uint exportRva = BitConverter.ToUInt32(b, dirOff);
            if (exportRva == 0)
                throw new InvalidOperationException("no export directory");

            int sectionTable = opt + optSize;
            int RvaToOff(uint rva)
            {
                for (int i = 0; i < numSections; i++)
                {
                    int s = sectionTable + i * 40;
                    uint va = BitConverter.ToUInt32(b, s + 12), rawSize = BitConverter.ToUInt32(b, s + 16), raw = BitConverter.ToUInt32(b, s + 20);
                    if (rva >= va && rva < va + rawSize)
                        return (int)(raw + (rva - va));
                }
                throw new InvalidOperationException("RVA not in any section");
            }

            int ed = RvaToOff(exportRva);
            uint nNames = BitConverter.ToUInt32(b, ed + 24);
            uint namesRva = BitConverter.ToUInt32(b, ed + 32);
            int names = RvaToOff(namesRva);
            var result = new System.Collections.Generic.List<string>((int)nNames);
            for (uint i = 0; i < nNames; i++)
            {
                uint nameRva = BitConverter.ToUInt32(b, names + (int)i * 4);
                int p = RvaToOff(nameRva);
                int end = p; while (b[end] != 0) end++;
                result.Add(System.Text.Encoding.ASCII.GetString(b, p, end - p));
            }
            return result;
        }
    }

    /// <summary>Minimal ELF (.dynsym) exported-symbol reader — names of defined dynamic symbols. Enough to
    /// validate a shipped .so's exports from any host without loading it. Not a general ELF parser.</summary>
    static class ElfDynamicSymbols
    {
        public static System.Collections.Generic.List<string> Read(byte[] b)
        {
            if (b[0] != 0x7F || b[1] != (byte)'E' || b[2] != (byte)'L' || b[3] != (byte)'F')
                throw new InvalidOperationException("not an ELF image");
            // 64-bit little-endian assumed (linux-x64).
            ulong shoff = BitConverter.ToUInt64(b, 0x28);
            ushort shentsize = BitConverter.ToUInt16(b, 0x3A), shnum = BitConverter.ToUInt16(b, 0x3C);
            int dynsym = -1;
            for (int i = 0; i < shnum; i++)
            {
                int sh = (int)shoff + i * shentsize;
                if (BitConverter.ToUInt32(b, sh + 4) == 11)      // SHT_DYNSYM
                {
                    dynsym = sh;
                    break;
                }
            }
            if (dynsym < 0) throw new InvalidOperationException("no .dynsym");
            uint link = BitConverter.ToUInt32(b, dynsym + 40);   // sh_link -> string table section index
            int strSh = (int)shoff + (int)link * shentsize;
            ulong strOff = BitConverter.ToUInt64(b, strSh + 24);
            ulong symOff = BitConverter.ToUInt64(b, dynsym + 24), symSize = BitConverter.ToUInt64(b, dynsym + 32), entsize = BitConverter.ToUInt64(b, dynsym + 56);
            var result = new System.Collections.Generic.List<string>();
            for (ulong o = 0; o + entsize <= symSize; o += entsize)
            {
                int e = (int)(symOff + o);
                uint nameIdx = BitConverter.ToUInt32(b, e);
                ushort shndx = BitConverter.ToUInt16(b, e + 6);
                if (nameIdx == 0 || shndx == 0) continue;         // unnamed or undefined (imported)
                int p = (int)strOff + (int)nameIdx;
                int end = p; while (b[end] != 0) end++;
                result.Add(System.Text.Encoding.ASCII.GetString(b, p, end - p));
            }
            return result;
        }
    }
}