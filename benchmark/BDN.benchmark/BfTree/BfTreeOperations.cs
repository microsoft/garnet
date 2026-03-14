// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.InteropServices;
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
        private BfTreeService _tree;
        private string _treePath;

        // Pinned arrays — GCHandle keeps them fixed so PinnedSpanByte is safe
        private byte[] _key;
        private byte[] _value;
        private byte[] _readBuffer;
        private GCHandle _keyHandle;
        private GCHandle _valueHandle;
        private GCHandle _readBufferHandle;

        // Pre-built PinnedSpanByte for zero-overhead benchmarks
        private PinnedSpanByte _pinnedKey;
        private PinnedSpanByte _pinnedValue;
        private byte* _pinnedReadBufPtr;
        private int _pinnedReadBufLen;

        [Params("Memory", "Disk")]
        public string Backend { get; set; }

        [Params(128)]
        public int ValueSize { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            if (Backend == "Disk")
            {
                _treePath = Path.Combine(Path.GetTempPath(), $"bftree_bench_{Guid.NewGuid():N}.bftree");
                _tree = new BfTreeService(
                    storageBackend: StorageBackendType.Disk,
                    filePath: _treePath,
                    cbMinRecordSize: 4);
            }
            else
            {
                _tree = new BfTreeService(
                    storageBackend: StorageBackendType.Memory,
                    cbMinRecordSize: 4);
            }

            _key = Encoding.UTF8.GetBytes("bench:key:00001");
            _value = new byte[ValueSize];
            new Random(42).NextBytes(_value);
            _readBuffer = new byte[ValueSize + 64];

            // Pin arrays for PinnedSpanByte benchmarks
            _keyHandle = GCHandle.Alloc(_key, GCHandleType.Pinned);
            _valueHandle = GCHandle.Alloc(_value, GCHandleType.Pinned);
            _readBufferHandle = GCHandle.Alloc(_readBuffer, GCHandleType.Pinned);

            _pinnedKey = PinnedSpanByte.FromPinnedPointer(
                (byte*)_keyHandle.AddrOfPinnedObject(), _key.Length);
            _pinnedValue = PinnedSpanByte.FromPinnedPointer(
                (byte*)_valueHandle.AddrOfPinnedObject(), _value.Length);
            _pinnedReadBufPtr = (byte*)_readBufferHandle.AddrOfPinnedObject();
            _pinnedReadBufLen = _readBuffer.Length;

            // Pre-insert so Read benchmarks hit
            _tree.Insert(_key, _value);

            // Validate the read actually returns the correct data
            var result = _tree.Read(_key, _readBuffer, out int bytesRead);
            Debug.Assert(result == BfTreeReadResult.Found,
                $"GlobalSetup validation: expected Found, got {result}");
            Debug.Assert(bytesRead == _value.Length,
                $"GlobalSetup validation: expected {_value.Length} bytes, got {bytesRead}");
            Debug.Assert(_readBuffer.AsSpan(0, bytesRead).SequenceEqual(_value),
                "GlobalSetup validation: read value does not match inserted value");
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _keyHandle.Free();
            _valueHandle.Free();
            _readBufferHandle.Free();
            _tree?.Dispose();
            if (_treePath != null && File.Exists(_treePath))
                File.Delete(_treePath);
        }

        [Benchmark]
        public BfTreeReadResult Read_Span()
        {
            return _tree.Read(_key, _readBuffer, out _);
        }

        [Benchmark]
        public BfTreeReadResult Read_Pinned()
        {
            return _tree.Read(_pinnedKey, _pinnedReadBufPtr, _pinnedReadBufLen, out _);
        }

        [Benchmark]
        public int FFI_Noop()
        {
            return _tree.Noop(_pinnedKey);
        }
    }

    /// <summary>
    /// Benchmarks for BfTree scan operations with callback (zero-alloc).
    /// Uses disk-backed mode since cache_only does not support scan.
    /// </summary>
    [MemoryDiagnoser]
    public class BfTreeScanOperations
    {
        private BfTreeService _tree;
        private string _treePath;
        private byte[] _scanBuffer;
        private static readonly byte[] StartKey = [0];

        [Params(10, 100)]
        public int EntryCount { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _treePath = Path.Combine(Path.GetTempPath(), $"bftree_scanbench_{Guid.NewGuid():N}.bftree");
            _tree = new BfTreeService(filePath: _treePath, cbMinRecordSize: 4);
            _scanBuffer = new byte[8192];

            for (int i = 0; i < EntryCount; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key:{i:D6}");
                var value = Encoding.UTF8.GetBytes($"val:{i:D6}");
                _tree.Insert(key, value);
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _tree?.Dispose();
            if (File.Exists(_treePath))
                File.Delete(_treePath);
        }

        [Benchmark]
        public int Scan()
        {
            return _tree.ScanWithCount(StartKey, EntryCount + 1, _scanBuffer,
                static (key, value) => true);
        }
    }
}
