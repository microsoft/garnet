// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Garnet.server.BfTreeInterop;
using NUnit.Framework;
using Tsavorite.core;

namespace BfTreeInterop.test
{
    /// <summary>
    /// Integration tests for the bftree-garnet native FFI interop layer.
    /// Tests all core BfTree APIs: lifecycle, point operations, scans, and snapshot/recovery.
    /// </summary>
    [TestFixture]
    public class BfTreeInteropTests
    {
        private BfTreeService tree;
        private string treePath;

        [SetUp]
        public void Setup()
        {
            treePath = Path.Combine(Path.GetTempPath(), $"bftree_test_{Guid.NewGuid():N}.bftree");
            tree = new BfTreeService(filePath: treePath, cbMinRecordSize: 4);
        }

        [TearDown]
        public void TearDown()
        {
            tree?.Dispose();
            if (treePath != null && File.Exists(treePath))
                File.Delete(treePath);
        }

        // ---------------------------------------------------------------
        // Lifecycle tests
        // ---------------------------------------------------------------

        [Test]
        public void CreateAndDispose()
        {
            var path = Path.Combine(Path.GetTempPath(), $"bftree_t_{Guid.NewGuid():N}.bftree");
            try
            {
                using var tree = new BfTreeService(filePath: path, cbMinRecordSize: 4);
                Assert.Pass();
            }
            finally
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }

        [Test]
        public void CreateWithCustomConfig()
        {
            var path = Path.Combine(Path.GetTempPath(), $"bftree_t_{Guid.NewGuid():N}.bftree");
            try
            {
                using var tree = new BfTreeService(filePath: path, cbSizeByte: 16 * 1024 * 1024, cbMinRecordSize: 8, cbMaxRecordSize: 4096, cbMaxKeyLen: 128, leafPageSize: 16384);
                Assert.Pass();
            }
            finally
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }

        [Test]
        public void CreateMemoryOnly()
        {
            using var tree = new BfTreeService(storageBackend: StorageBackendType.Memory, cbMinRecordSize: 4);
            var insertResult = tree.Insert("testkey"u8, "testval"u8);
            Assert.That(insertResult, Is.EqualTo(BfTreeInsertResult.Success));
        }

        [Test]
        public void CreateDiskBacked_MissingPath_Throws()
        {
            Assert.Throws<ArgumentException>(() => new BfTreeService(filePath: null));
        }

        [Test]
        public void DoubleDispose_DoesNotThrow()
        {
            var path = Path.Combine(Path.GetTempPath(), $"bftree_t_{Guid.NewGuid():N}.bftree");
            try
            {
                var tree = new BfTreeService(filePath: path, cbMinRecordSize: 4);
                tree.Dispose();
                Assert.DoesNotThrow(() => tree.Dispose());
            }
            finally
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }

        // ---------------------------------------------------------------
        // Insert tests
        // ---------------------------------------------------------------

        [Test]
        public void InsertAndRead_BasicRoundTrip()
        {
            var key = "user:1001"u8;
            var value = "Alice"u8;

            var insertResult = tree.Insert(key, value);
            Assert.That(insertResult, Is.EqualTo(BfTreeInsertResult.Success));

            var readResult = tree.Read(key, out var readValue);
            Assert.That(readResult, Is.EqualTo(BfTreeReadResult.Found));
            Assert.That(readValue, Is.EqualTo(value.ToArray()));
        }

        [Test]
        public void InsertOverwrite_ReturnsUpdatedValue()
        {
            var key = "mykey"u8;

            tree.Insert(key, "value1"u8);
            tree.Insert(key, "value2"u8);

            var readResult = tree.Read(key, out var value);
            Assert.That(readResult, Is.EqualTo(BfTreeReadResult.Found));
            Assert.That(value, Is.EqualTo("value2"u8.ToArray()));
        }

        [Test]
        public void InsertMultiple_AllReadable()
        {
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key:{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value:{i}");
                var result = tree.Insert(key, value);
                Assert.That(result, Is.EqualTo(BfTreeInsertResult.Success));
            }

            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key:{i:D4}");
                var expectedValue = Encoding.UTF8.GetBytes($"value:{i}");
                var readResult = tree.Read(key, out var readValue);
                Assert.That(readResult, Is.EqualTo(BfTreeReadResult.Found));
                Assert.That(readValue, Is.EqualTo(expectedValue));
            }
        }

        // ---------------------------------------------------------------
        // Read tests
        // ---------------------------------------------------------------

        [Test]
        public void ReadNotFound()
        {
            var readResult = tree.Read("nonexistent"u8, out var value);
            Assert.That(readResult, Is.EqualTo(BfTreeReadResult.NotFound));
            Assert.That(value, Is.Empty);
        }

        [Test]
        public void ReadAfterDelete_ReturnsDeleted()
        {
            var key = "deleteme"u8;
            tree.Insert(key, "value"u8);
            tree.Delete(key);

            var readResult = tree.Read(key, out var value);
            Assert.That(readResult, Is.EqualTo(BfTreeReadResult.Deleted));
            Assert.That(value, Is.Empty);
        }

        [Test]
        public void ReadIntoSpan_ZeroAlloc()
        {
            var key = "spankey"u8;
            var expected = "spanvalue"u8;
            tree.Insert(key, expected);

            Span<byte> buffer = stackalloc byte[256];
            var result = tree.Read(key, buffer, out int bytesWritten);
            Assert.That(result, Is.EqualTo(BfTreeReadResult.Found));
            Assert.That(bytesWritten, Is.EqualTo(expected.Length));
            Assert.That(buffer[..bytesWritten].SequenceEqual(expected), Is.True);
        }

        [Test]
        public void ReadIntoSpan_NotFound()
        {
            Span<byte> buffer = stackalloc byte[256];
            var result = tree.Read("nope"u8, buffer, out int bytesWritten);
            Assert.That(result, Is.EqualTo(BfTreeReadResult.NotFound));
            Assert.That(bytesWritten, Is.EqualTo(0));
        }

        // ---------------------------------------------------------------
        // Delete tests
        // ---------------------------------------------------------------

        [Test]
        public void DeleteExistingKey()
        {
            var key = "toremove"u8;
            tree.Insert(key, "data"u8);
            Assert.That(tree.Delete(key), Is.EqualTo(BfTreeDeleteResult.Success));

            var readResult = tree.Read(key, out _);
            Assert.That(readResult, Is.EqualTo(BfTreeReadResult.Deleted));
        }

        [Test]
        public void DeleteNonExistentKey_ReturnsSuccess()
        {
            Assert.That(tree.Delete("ghost"u8), Is.EqualTo(BfTreeDeleteResult.Success));
        }

        // ---------------------------------------------------------------
        // Scan with count tests
        // ---------------------------------------------------------------

        [Test]
        public void ScanWithCount_ReturnsCorrectCount()
        {
            InsertTestData(10);

            var results = tree.ScanWithCount("key:"u8, 5);
            Assert.That(results, Has.Count.EqualTo(5));
        }

        [Test]
        public void ScanWithCount_ReturnsKeyAndValue()
        {
            InsertTestData(5);

            var results = tree.ScanWithCount("key:"u8, 10, ScanReturnField.KeyAndValue);
            Assert.That(results, Has.Count.EqualTo(5));

            foreach (var r in results)
            {
                Assert.That(r.Key.Length, Is.GreaterThan(0));
                Assert.That(r.Value.Length, Is.GreaterThan(0));
            }
        }

        [Test]
        public void ScanWithCount_KeyOnly()
        {
            InsertTestData(5);

            var results = tree.ScanWithCount("key:"u8, 10, ScanReturnField.Key);
            Assert.That(results, Has.Count.EqualTo(5));

            foreach (var r in results)
            {
                Assert.That(r.Key.Length, Is.GreaterThan(0));
                Assert.That(r.Value.Length, Is.EqualTo(0));
            }
        }

        [Test]
        public void ScanWithCount_ValueOnly()
        {
            InsertTestData(5);

            var results = tree.ScanWithCount("key:"u8, 10, ScanReturnField.Value);
            Assert.That(results, Has.Count.EqualTo(5));

            foreach (var r in results)
            {
                Assert.That(r.Key.Length, Is.EqualTo(0));
                Assert.That(r.Value.Length, Is.GreaterThan(0));
            }
        }

        [Test]
        public void ScanWithCount_Ordering()
        {
            // Insert keys out of order, verify scan returns them sorted
            tree.Insert("key:C"u8, "3"u8);
            tree.Insert("key:A"u8, "1"u8);
            tree.Insert("key:B"u8, "2"u8);

            var results = tree.ScanWithCount("key:"u8, 10, ScanReturnField.Key);
            Assert.That(results, Has.Count.EqualTo(3));

            var keys = results.Select(r => Encoding.UTF8.GetString(r.Key.Span)).ToList();
            string[] expectedKeys = ["key:A", "key:B", "key:C"];
            Assert.That(keys, Is.EqualTo(expectedKeys));
        }

        [Test]
        public void ScanWithCount_StartKeyInMiddle()
        {
            InsertTestData(10); // key:0000 through key:0009

            // Start from key:0005, should get key:0005 through key:0009
            var results = tree.ScanWithCount(Encoding.UTF8.GetBytes("key:0005"), 10, ScanReturnField.Key);
            Assert.That(results, Has.Count.EqualTo(5));

            var firstKey = Encoding.UTF8.GetString(results[0].Key.Span);
            Assert.That(firstKey, Is.EqualTo("key:0005"));
        }

        [Test]
        public void ScanWithCount_EmptyTree()
        {
            var results = tree.ScanWithCount("key:"u8, 10);
            Assert.That(results, Is.Empty);
        }

        // ---------------------------------------------------------------
        // Scan with end key tests
        // ---------------------------------------------------------------

        [Test]
        public void ScanWithEndKey_InclusiveRange()
        {
            InsertTestData(10); // key:0000 through key:0009

            var results = tree.ScanWithEndKey(Encoding.UTF8.GetBytes("key:0002"), Encoding.UTF8.GetBytes("key:0005"), ScanReturnField.Key);

            var keys = results.Select(r => Encoding.UTF8.GetString(r.Key.Span)).ToList();
            Assert.That(keys, Has.Count.GreaterThanOrEqualTo(3));
            Assert.That(keys[0], Is.EqualTo("key:0002"));
        }

        [Test]
        public void ScanWithEndKey_AllEntries()
        {
            InsertTestData(5);

            var results = tree.ScanWithEndKey("key:0000"u8.ToArray(), "key:9999"u8.ToArray(), ScanReturnField.KeyAndValue);
            Assert.That(results, Has.Count.EqualTo(5));
        }

        [Test]
        public void ScanWithEndKey_EmptyRange()
        {
            InsertTestData(5); // key:0000 through key:0004

            var results = tree.ScanWithEndKey("zzz:0000"u8.ToArray(), "zzz:9999"u8.ToArray());
            Assert.That(results, Is.Empty);
        }

        // ---------------------------------------------------------------
        // ScanAll tests
        // ---------------------------------------------------------------

        [Test]
        public void ScanAll_ReturnsAllEntries()
        {
            InsertTestData(20);

            var results = tree.ScanAll();
            Assert.That(results, Has.Count.EqualTo(20));

            // Verify ordering
            var keys = results.Select(r => Encoding.UTF8.GetString(r.Key.Span)).ToList();
            var sorted = keys.OrderBy(k => k, StringComparer.Ordinal).ToList();
            Assert.That(keys, Is.EqualTo(sorted));
        }

        [Test]
        public void ScanAll_EmptyTree()
        {
            var results = tree.ScanAll();
            Assert.That(results, Is.Empty);
        }

        [Test]
        public void ScanAll_KeyOnly()
        {
            InsertTestData(5);

            var results = tree.ScanAll(ScanReturnField.Key);
            Assert.That(results, Has.Count.EqualTo(5));

            foreach (var r in results)
            {
                Assert.That(r.Key.Length, Is.GreaterThan(0));
                Assert.That(r.Value.Length, Is.EqualTo(0));
            }
        }

        // ---------------------------------------------------------------
        // Zero-alloc callback scan tests
        // ---------------------------------------------------------------

        [Test]
        public void ScanWithCallback_ZeroAlloc()
        {
            InsertTestData(10);

            var keys = new List<string>();
            Span<byte> scanBuf = stackalloc byte[8192];
            int count = tree.ScanWithCount("key:"u8, 100, scanBuf, (key, value) =>
                {
                    keys.Add(Encoding.UTF8.GetString(key));
                    return true;
                });

            Assert.That(count, Is.EqualTo(10));
            Assert.That(keys, Has.Count.EqualTo(10));
            Assert.That(keys[0], Is.EqualTo("key:0000"));
        }

        [Test]
        public void ScanWithCallback_EarlyStop()
        {
            InsertTestData(10);

            int seen = 0;
            Span<byte> scanBuf = stackalloc byte[8192];
            int count = tree.ScanWithCount("key:"u8, 100, scanBuf, (key, value) =>
                {
                    seen++;
                    return seen < 3; // stop after 3 records
                });

            Assert.That(count, Is.EqualTo(3));
        }

        // ---------------------------------------------------------------
        // Snapshot / Recovery tests (disk-backed)
        // ---------------------------------------------------------------

        [Test]
        public void SnapshotAndRecover_RoundTrip()
        {
            var snapshotPath = Path.Combine(Path.GetTempPath(), $"bftree_snap_{Guid.NewGuid():N}.bftree");
            try
            {
                tree.Dispose();
                tree = new BfTreeService(filePath: treePath, enableSnapshots: true, cbMinRecordSize: 4);
                InsertTestData(20);
                BfTreeService.CprSnapshotByPtr(tree.NativePtr, snapshotPath);
                tree.Dispose();

                // Recover from the snapshot file
                tree = BfTreeService.RecoverFromCprSnapshot(snapshotPath, false, StorageBackendType.Disk);

                for (int i = 0; i < 20; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key:{i:D4}");
                    var expectedValue = Encoding.UTF8.GetBytes($"val:{i}");
                    var readResult = tree.Read(key, out var readValue);
                    Assert.That(readResult, Is.EqualTo(BfTreeReadResult.Found), $"Key key:{i:D4} not found after recovery");
                    Assert.That(readValue, Is.EqualTo(expectedValue));
                }
            }
            finally
            {
                if (File.Exists(snapshotPath))
                    File.Delete(snapshotPath);
            }
        }

        [Test]
        public void SnapshotAndRecover_ScanAfterRestore()
        {
            var snapshotPath = Path.Combine(Path.GetTempPath(), $"bftree_snap_{Guid.NewGuid():N}.bftree");
            try
            {
                tree.Dispose();
                tree = new BfTreeService(filePath: treePath, enableSnapshots: true, cbMinRecordSize: 4);
                InsertTestData(10);
                BfTreeService.CprSnapshotByPtr(tree.NativePtr, snapshotPath);
                tree.Dispose();

                tree = BfTreeService.RecoverFromCprSnapshot(snapshotPath, false, StorageBackendType.Disk);

                var results = tree.ScanWithCount("key:"u8, 100, ScanReturnField.Key);
                Assert.That(results, Has.Count.EqualTo(10));
            }
            finally
            {
                if (File.Exists(snapshotPath))
                    File.Delete(snapshotPath);
            }
        }

        [Test]
        public void RecoverNonExistentFile_Throws()
        {
            var path = Path.Combine(Path.GetTempPath(), $"bftree_noexist_{Guid.NewGuid():N}.bftree");
            Assert.Throws<InvalidOperationException>(() => BfTreeService.RecoverFromCprSnapshot(path, false, StorageBackendType.Disk));
        }

        [Test]
        public void MemoryOnly_SnapshotAndRecover_RoundTrip()
        {
            var snapshotPath = Path.Combine(Path.GetTempPath(), $"bftree_memsnap_{Guid.NewGuid():N}.bftree");
            try
            {
                using (var memTree = new BfTreeService(storageBackend: StorageBackendType.Memory, enableSnapshots: true, cbMinRecordSize: 4))
                {
                    memTree.Insert("testkey"u8, "testval"u8);
                    // bftree supports CPR snapshot for memory-backed trees uniformly with disk-backed.
                    Assert.DoesNotThrow(() => BfTreeService.CprSnapshotByPtr(memTree.NativePtr, snapshotPath));
                }

                using var recovered = BfTreeService.RecoverFromCprSnapshot(snapshotPath, false, StorageBackendType.Memory);
                Assert.That(recovered.Read("testkey"u8, out var value), Is.EqualTo(BfTreeReadResult.Found));
                Assert.That(value, Is.EqualTo("testval"u8.ToArray()));
            }
            finally
            {
                if (File.Exists(snapshotPath))
                    File.Delete(snapshotPath);
            }
        }

        [Test]
        public void MemoryOnly_RecoverFromNonExistentFile_Throws()
        {
            // Recovering a memory-backed tree from a missing snapshot file fails (null handle → throw).
            Assert.Throws<InvalidOperationException>(() => BfTreeService.RecoverFromCprSnapshot("/tmp/nonexistent.bftree", false, StorageBackendType.Memory));
        }

        // ---------------------------------------------------------------
        // Disposed object tests
        // ---------------------------------------------------------------

        [Test]
        public void OperationsOnDisposedTree_Throw()
        {
            var path = Path.Combine(Path.GetTempPath(), $"bftree_t_{Guid.NewGuid():N}.bftree");
            var tree = new BfTreeService(filePath: path, cbMinRecordSize: 4);
            tree.Dispose();

            try
            {
                Assert.Throws<ObjectDisposedException>(() => tree.Insert("k"u8, "v"u8));
                Assert.Throws<ObjectDisposedException>(() => tree.Read("k"u8, out _));
                Assert.Throws<ObjectDisposedException>(() => tree.Delete("k"u8));
                Assert.Throws<ObjectDisposedException>(() => tree.ScanWithCount("k"u8, 1));
                Assert.Throws<ObjectDisposedException>(() => tree.ScanWithEndKey("a"u8, "z"u8));
            }
            finally
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }

        // ---------------------------------------------------------------
        // Large data tests
        // ---------------------------------------------------------------

        [Test]
        public void LargeInsertAndScan()
        {
            const int count = 1000;
            for (int i = 0; i < count; i++)
            {
                var key = Encoding.UTF8.GetBytes($"large:{i:D6}");
                var value = Encoding.UTF8.GetBytes($"payload_{i}_{new string('x', 100)}");
                tree.Insert(key, value);
            }

            var results = tree.ScanWithCount("large:"u8, count + 1, ScanReturnField.Key);
            Assert.That(results, Has.Count.EqualTo(count));
        }

        [Test]
        public unsafe void Insert_InvalidArguments_ReturnsInvalidArguments()
        {
            var keyBytes = "k"u8.ToArray();
            var valueBytes = "v"u8.ToArray();
            fixed (byte* kp = keyBytes)
            fixed (byte* vp = valueBytes)
            {
                var validKey = PinnedSpanByte.FromPinnedPointer(kp, keyBytes.Length);
                var validValue = PinnedSpanByte.FromPinnedPointer(vp, valueBytes.Length);
                Assert.That(tree.Insert(PinnedSpanByte.FromPinnedPointer(kp, -1), validValue), Is.EqualTo(BfTreeInsertResult.InvalidArguments), "negative key length");
                Assert.That(tree.Insert(validKey, PinnedSpanByte.FromPinnedPointer(vp, -1)), Is.EqualTo(BfTreeInsertResult.InvalidArguments), "negative value length");
            }

            // Tree must still be functional after rejecting the invalid input.
            Assert.That(tree.Insert("healthy"u8, "value"u8), Is.EqualTo(BfTreeInsertResult.Success));
        }

        [Test]
        public unsafe void Read_InvalidArguments_ReturnsInvalidArguments()
        {
            var keyBytes = "k"u8.ToArray();
            Span<byte> outputBuffer = stackalloc byte[16];
            fixed (byte* kp = keyBytes)
            fixed (byte* op = outputBuffer)
            {
                var validKey = PinnedSpanByte.FromPinnedPointer(kp, keyBytes.Length);

                var negKeyResult = tree.Read(PinnedSpanByte.FromPinnedPointer(kp, -1), op, outputBuffer.Length, out var negKeyBytesWritten);
                Assert.That(negKeyResult, Is.EqualTo(BfTreeReadResult.InvalidArguments), "negative key length");
                Assert.That(negKeyBytesWritten, Is.EqualTo(0), "negative key length bytesWritten");

                var negBufResult = tree.Read(validKey, op, -1, out var negBufBytesWritten);
                Assert.That(negBufResult, Is.EqualTo(BfTreeReadResult.InvalidArguments), "negative output buffer length");
                Assert.That(negBufBytesWritten, Is.EqualTo(0), "negative output buffer length bytesWritten");
            }
        }

        [Test]
        public unsafe void Delete_InvalidArguments_DoesNotCorruptTree()
        {
            tree.Insert("keep"u8, "value"u8);

            var keyBytes = "k"u8.ToArray();
            fixed (byte* kp = keyBytes)
            {
                var key = PinnedSpanByte.FromPinnedPointer(kp, -1);
                Assert.That(tree.Delete(key), Is.EqualTo(BfTreeDeleteResult.InvalidArguments), "negative key length");
            }

            // The pre-existing key must be untouched by the rejected delete.
            Assert.That(tree.Read("keep"u8, out var value), Is.EqualTo(BfTreeReadResult.Found));
            Assert.That(value, Is.EqualTo("value"u8.ToArray()));
        }

        [Test]
        public void ScanWithCount_NegativeCount_Throws()
        {
            InsertTestData(10);

            // A negative count makes the native scan-create reject with a null handle,
            // which the wrapper surfaces as a bug rather than a silent empty result.
            Assert.Throws<InvalidOperationException>(() => tree.ScanWithCount("key:"u8, -1, ScanReturnField.Key));

            // A subsequent valid scan must still work.
            Assert.That(tree.ScanWithCount("key:"u8, 10, ScanReturnField.Key), Is.Not.Empty);
        }

        // ---------------------------------------------------------------
        // Helpers
        // ---------------------------------------------------------------

        private void InsertTestData(int count)
        {
            InsertTestDataInto(tree, count);
        }

        private static void InsertTestDataInto(BfTreeService tree, int count)
        {
            for (int i = 0; i < count; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key:{i:D4}");
                var value = Encoding.UTF8.GetBytes($"val:{i}");
                tree.Insert(key, value);
            }
        }
    }
}