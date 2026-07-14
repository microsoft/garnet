// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Garnet.server.BfTreeInterop;
using NUnit.Framework;

namespace BfTreeInterop.test
{
    /// <summary>
    /// Tests that call the native P/Invoke layer (<see cref="NativeBfTreeMethods"/>) directly with
    /// invalid arguments — negative lengths, null data pointers, and a null tree handle — bypassing
    /// the managed <c>PinnedSpanByte</c> / <c>BfTreeService</c> wrappers. These exercise the
    /// FFI-boundary guards at the exact point where a signed length would be cast to an unsigned
    /// size, verifying they return a sentinel instead of constructing an invalid slice.
    /// </summary>
    [TestFixture]
    public unsafe class BfTreeInteropNativeTests
    {
        private const byte StorageDisk = 0;
        private const byte StorageMemory = 1;

        // Native result codes (mirror the Rust constants in lib.rs).
        private const int InsertInvalidArgs = -1;
        private const int ReadInvalidArgs = -4;
        private const int DeleteInvalidArgs = -1;

        private nint tree;

        [SetUp]
        public void Setup()
        {
            tree = NativeBfTreeMethods.bftree_create(0, 0, 0, 0, 0, StorageMemory, null, 0, 0);
            Assert.That(tree, Is.Not.EqualTo(nint.Zero), "failed to create memory-backed tree");
        }

        [TearDown]
        public void TearDown()
        {
            if (tree != nint.Zero)
                NativeBfTreeMethods.bftree_drop(tree);
            tree = nint.Zero;
        }

        [Test]
        public void Insert_InvalidArgs_ReturnsInvalidArgs()
        {
            byte k = 0, v = 0;
            Assert.That(NativeBfTreeMethods.bftree_insert(nint.Zero, &k, 1, &v, 1), Is.EqualTo(InsertInvalidArgs), "null tree");
            Assert.That(NativeBfTreeMethods.bftree_insert(tree, null, 1, &v, 1), Is.EqualTo(InsertInvalidArgs), "null key");
            Assert.That(NativeBfTreeMethods.bftree_insert(tree, &k, 1, null, 1), Is.EqualTo(InsertInvalidArgs), "null value");
            Assert.That(NativeBfTreeMethods.bftree_insert(tree, &k, -1, &v, 1), Is.EqualTo(InsertInvalidArgs), "negative key length");
            Assert.That(NativeBfTreeMethods.bftree_insert(tree, &k, 1, &v, -1), Is.EqualTo(InsertInvalidArgs), "negative value length");
        }

        [Test]
        public void Read_InvalidArgs_ReturnsInvalidArgs()
        {
            byte k = 0;
            byte* buf = stackalloc byte[16];
            int outLen = 0;
            Assert.That(NativeBfTreeMethods.bftree_read(nint.Zero, &k, 1, buf, 16, &outLen), Is.EqualTo(ReadInvalidArgs), "null tree");
            Assert.That(NativeBfTreeMethods.bftree_read(tree, null, 1, buf, 16, &outLen), Is.EqualTo(ReadInvalidArgs), "null key");
            Assert.That(NativeBfTreeMethods.bftree_read(tree, &k, -1, buf, 16, &outLen), Is.EqualTo(ReadInvalidArgs), "negative key length");
            Assert.That(NativeBfTreeMethods.bftree_read(tree, &k, 1, null, 16, &outLen), Is.EqualTo(ReadInvalidArgs), "null out buffer");
            Assert.That(NativeBfTreeMethods.bftree_read(tree, &k, 1, buf, -1, &outLen), Is.EqualTo(ReadInvalidArgs), "negative out buffer length");
        }

        [Test]
        public void Delete_InvalidArgs_ReturnsInvalidArgs()
        {
            byte k = 0;
            Assert.That(NativeBfTreeMethods.bftree_delete(nint.Zero, &k, 1), Is.EqualTo(DeleteInvalidArgs), "null tree");
            Assert.That(NativeBfTreeMethods.bftree_delete(tree, null, 1), Is.EqualTo(DeleteInvalidArgs), "null key");
            Assert.That(NativeBfTreeMethods.bftree_delete(tree, &k, -1), Is.EqualTo(DeleteInvalidArgs), "negative key length");
        }

        [Test]
        public void ScanWithCount_InvalidArgs_ReturnsNullHandle()
        {
            byte start = 0;
            Assert.That(NativeBfTreeMethods.bftree_scan_with_count(nint.Zero, &start, 1, 10, 2), Is.EqualTo(nint.Zero), "null tree");
            Assert.That(NativeBfTreeMethods.bftree_scan_with_count(tree, null, 1, 10, 2), Is.EqualTo(nint.Zero), "null start key");
            Assert.That(NativeBfTreeMethods.bftree_scan_with_count(tree, &start, -1, 10, 2), Is.EqualTo(nint.Zero), "negative start key length");
            Assert.That(NativeBfTreeMethods.bftree_scan_with_count(tree, &start, 1, -1, 2), Is.EqualTo(nint.Zero), "negative count");
        }

        [Test]
        public void ScanWithEndKey_InvalidArgs_ReturnsNullHandle()
        {
            byte start = 0, end = 0xff;
            Assert.That(NativeBfTreeMethods.bftree_scan_with_end_key(nint.Zero, &start, 1, &end, 1, 2), Is.EqualTo(nint.Zero), "null tree");
            Assert.That(NativeBfTreeMethods.bftree_scan_with_end_key(tree, null, 1, &end, 1, 2), Is.EqualTo(nint.Zero), "null start key");
            Assert.That(NativeBfTreeMethods.bftree_scan_with_end_key(tree, &start, -1, &end, 1, 2), Is.EqualTo(nint.Zero), "negative start key length");
            Assert.That(NativeBfTreeMethods.bftree_scan_with_end_key(tree, &start, 1, null, 1, 2), Is.EqualTo(nint.Zero), "null end key");
            Assert.That(NativeBfTreeMethods.bftree_scan_with_end_key(tree, &start, 1, &end, -1, 2), Is.EqualTo(nint.Zero), "negative end key length");
        }

        [Test]
        public void ScanNext_InvalidArgs_ReturnsZero()
        {
            byte* buf = stackalloc byte[16];
            int keyLen = 0, valueLen = 0;

            // A null handle short-circuits to 0 regardless of the other arguments.
            Assert.That(NativeBfTreeMethods.bftree_scan_next(nint.Zero, buf, 16, &keyLen, &valueLen), Is.EqualTo(0), "null handle");

            // Isolating the out-buffer guards needs a valid handle. Scanning is only supported on
            // disk-backed trees (a memory/cache_only tree aborts natively), so build one here.
            var path = Path.Combine(Path.GetTempPath(), $"bftree_native_{Guid.NewGuid():N}.bftree");
            var pathBytes = Encoding.UTF8.GetBytes(path);
            nint diskTree;
            fixed (byte* pp = pathBytes)
                diskTree = NativeBfTreeMethods.bftree_create(0, 4, 0, 0, 0, StorageDisk, pp, pathBytes.Length, 0);
            Assert.That(diskTree, Is.Not.EqualTo(nint.Zero), "failed to create disk-backed tree");

            nint handle = nint.Zero;
            try
            {
                byte start = 0;
                handle = NativeBfTreeMethods.bftree_scan_with_count(diskTree, &start, 1, 10, 2);
                Assert.That(handle, Is.Not.EqualTo(nint.Zero), "expected a valid scan handle");
                Assert.That(NativeBfTreeMethods.bftree_scan_next(handle, null, 16, &keyLen, &valueLen), Is.EqualTo(0), "null out buffer");
                Assert.That(NativeBfTreeMethods.bftree_scan_next(handle, buf, -1, &keyLen, &valueLen), Is.EqualTo(0), "negative out buffer length");
            }
            finally
            {
                if (handle != nint.Zero)
                    NativeBfTreeMethods.bftree_scan_drop(handle);
                NativeBfTreeMethods.bftree_drop(diskTree);
                if (File.Exists(path))
                    File.Delete(path);
            }
        }
    }
}