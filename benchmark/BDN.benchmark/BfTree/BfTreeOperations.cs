// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.server.BfTreeInterop;
using Tsavorite.core;

namespace BDN.benchmark.BfTree
{
    /// <summary>
    /// Benchmarks for BfTree FFI point operations comparing span-based (fixed pinning)
    /// vs PinnedSpanByte (zero-overhead) hot paths, across Memory and Disk backends.
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class BfTreePointOperations
    {
        const int ValueSize = 8;

        BfTreeService tree;
        string treePath;

        // Pinned arrays allocated via GC.AllocateArray(pinned: true)
        byte[] key;
        byte[] value;
        byte[] readBuffer;

        // Pre-built PinnedSpanByte for zero-overhead benchmarks
        PinnedSpanByte pinnedKey;
        PinnedSpanByte pinnedValue;
        byte* pinnedReadBufPtr;
        int pinnedReadBufLen;

        [Params("Memory", "Disk")]
        public string Backend { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            if (Backend == "Disk")
            {
                treePath = Path.Combine(Path.GetTempPath(), $"bftree_bench_{Guid.NewGuid():N}.bftree");
                tree = new BfTreeService(
                    storageBackend: StorageBackendType.Disk,
                    filePath: treePath,
                    cbMinRecordSize: 8);
            }
            else
            {
                tree = new BfTreeService(
                    storageBackend: StorageBackendType.Memory,
                    cbMinRecordSize: 8);
            }

            // Allocate pinned arrays — no GCHandle needed
            key = GC.AllocateArray<byte>("key00000"u8.Length, pinned: true);
            value = GC.AllocateArray<byte>(ValueSize, pinned: true);
            readBuffer = GC.AllocateArray<byte>(ValueSize, pinned: true);

            pinnedKey = PinnedSpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref key[0]), key.Length);
            pinnedValue = PinnedSpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref value[0]), value.Length);
            pinnedReadBufPtr = (byte*)Unsafe.AsPointer(ref readBuffer[0]);
            pinnedReadBufLen = readBuffer.Length;

            // Pre-populate 1000 key-value entries (key00000..key00999 → val00000..val00999)
            // so the total data exceeds the base page size (~4 KB default). This
            // ensures reads are served from the cache and disk-backed reads don't
            // hit a cold-page corner case.
            for (var i = 0; i < 1000; i++)
            {
                Encoding.UTF8.GetBytes($"key{i:D5}", key);
                Encoding.UTF8.GetBytes($"val{i:D5}", value);
                tree.Insert(key, value);
            }

            // Set the benchmark key-value (key00000)
            "key00000"u8.CopyTo(key);
            "val00000"u8.CopyTo(value);

            // Validate the read actually returns the correct data
            var result = tree.Read(key, readBuffer, out var bytesRead);
            Debug.Assert(result == BfTreeReadResult.Found,
                $"GlobalSetup validation: expected Found, got {result}");
            Debug.Assert(bytesRead == value.Length,
                $"GlobalSetup validation: expected {value.Length} bytes, got {bytesRead}");
            Debug.Assert(readBuffer.AsSpan(0, bytesRead).SequenceEqual(value),
                "GlobalSetup validation: read value does not match inserted value");
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            tree?.Dispose();
            if (treePath != null && File.Exists(treePath))
                File.Delete(treePath);
        }

        [Benchmark]
        public BfTreeReadResult Read_Span()
        {
            return tree.Read(key, readBuffer, out _);
        }

        [Benchmark]
        public BfTreeReadResult Read_Pinned()
        {
            return tree.Read(pinnedKey, pinnedReadBufPtr, pinnedReadBufLen, out _);
        }

        [Benchmark]
        public int FFI_Noop()
        {
            return tree.Noop(pinnedKey);
        }
    }

    /// <summary>
    /// Benchmarks for BfTree scan operations with callback (zero-alloc).
    /// Uses disk-backed mode since cache_only does not support scan.
    /// </summary>
    [MemoryDiagnoser]
    public class BfTreeScanOperations
    {
        private BfTreeService tree;
        private string treePath;
        private byte[] scanBuffer;
        private static readonly byte[] StartKey = [0];

        [Params(10, 100)]
        public int EntryCount { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            treePath = Path.Combine(Path.GetTempPath(), $"bftree_scanbench_{Guid.NewGuid():N}.bftree");
            tree = new BfTreeService(filePath: treePath, cbMinRecordSize: 8);
            scanBuffer = new byte[8192];

            // Pre-populate 1000 key-value entries (key00000..key00999 → val00000..val00999)
            // so the total data exceeds the base page size (~4 KB default). This
            // ensures reads are served from the cache and disk-backed reads don't
            // hit a cold-page corner case.
            for (var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key:{i:D5}");
                var value = Encoding.UTF8.GetBytes($"val:{i:D5}");
                tree.Insert(key, value);
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            tree?.Dispose();
            if (File.Exists(treePath))
                File.Delete(treePath);
        }

        [Benchmark]
        public int Scan()
        {
            return tree.ScanWithCount(StartKey, EntryCount + 1, scanBuffer,
                static (key, value) => true);
        }
    }
}