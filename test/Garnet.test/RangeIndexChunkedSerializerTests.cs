// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Hashing;
using System.Text;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Unit tests for <see cref="RangeIndexChunkedSerializer"/> and <see cref="RangeIndexChunkedDeserializer"/>.
    /// Tests the serialization wire format, checksum validation, state machine transitions,
    /// and round-trip correctness without requiring a running Garnet server.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class RangeIndexChunkedSerializerTests : AllureTestBase
    {
        private string testDir;

        [SetUp]
        public void Setup()
        {
            testDir = Path.Combine(TestUtils.MethodTestDir, "ri-serializer-test");
            if (Directory.Exists(testDir))
                Directory.Delete(testDir, recursive: true);
            Directory.CreateDirectory(testDir);
        }

        [TearDown]
        public void TearDown()
        {
            if (Directory.Exists(testDir))
                Directory.Delete(testDir, recursive: true);
            TestUtils.OnTearDown();
        }

        private static byte[] CreateStub()
        {
            var stub = new byte[RangeIndexManager.IndexSizeBytes];
            for (var i = 0; i < stub.Length; i++)
                stub[i] = (byte)(0xA0 + i);
            return stub;
        }

        private static byte[] CreateBuffer()
        {
            return new byte[RangeIndexManager.DefaultMigrationChunkSize];
        }

        /// <summary>
        /// Small file that fits in a single chunk — serializer should emit exactly one MoveNext.
        /// </summary>
        [Test]
        public void SingleChunkRoundTrip()
        {
            var fileData = new byte[1024];
            new Random(42).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "small.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("mykey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            var len = serializer.MoveNext(buffer);
            ClassicAssert.Greater(len, 0);
            var payload = buffer.AsSpan(0, len).ToArray();
            ClassicAssert.IsTrue(serializer.IsComplete);

            // Verify wire format: [4-byte keyLen][key][8-byte fileCount]...
            var offset = 0;
            var keyLenFromPayload = BinaryPrimitives.ReadInt32LittleEndian(payload);
            ClassicAssert.AreEqual(key.Length, keyLenFromPayload);
            offset += sizeof(int);
            ClassicAssert.AreEqual(key, payload.AsSpan(offset, key.Length).ToArray());
            offset += key.Length;
            var fileSizeFromPayload = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(offset));
            ClassicAssert.AreEqual(fileData.Length, fileSizeFromPayload);

            // Round-trip through deserializer
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Large file that spans multiple chunks — verify all chunks round-trip correctly.
        /// </summary>
        [Test]
        public void MultiChunkRoundTrip()
        {
            var fileSize = RangeIndexManager.DefaultMigrationChunkSize * 3 + 1000;
            var fileData = new byte[fileSize];
            new Random(123).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "large.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("largekey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileSize);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            var chunkCount = 0;
            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
                chunkCount++;
            }

            ClassicAssert.Greater(chunkCount, 1);
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Empty file (0 bytes) — should still produce a valid record with key + header + trailer.
        /// </summary>
        [Test]
        public void EmptyFileRoundTrip()
        {
            var filePath = Path.Combine(testDir, "empty.bftree");
            File.WriteAllBytes(filePath, Array.Empty<byte>());

            var key = Encoding.UTF8.GetBytes("emptykey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, 0);

            var len = serializer.MoveNext(buffer);
            ClassicAssert.Greater(len, 0);
            var payload = buffer.AsSpan(0, len).ToArray();
            ClassicAssert.IsTrue(serializer.IsComplete);

            // File count should be 0
            var fileCountOffset = sizeof(int) + key.Length;
            var fileSizeFromPayload = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(fileCountOffset));
            ClassicAssert.AreEqual(0, fileSizeFromPayload);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Corrupted checksum should cause the deserializer to enter Error state.
        /// </summary>
        [Test]
        public void CorruptedChecksumDetected()
        {
            var fileData = new byte[512];
            new Random(99).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "corrupt.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("corruptkey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            var len = serializer.MoveNext(buffer);
            ClassicAssert.Greater(len, 0);
            var payload = buffer.AsSpan(0, len).ToArray();

            // Corrupt a file data byte (after keyLen + key + fileCount)
            var fileDataOffset = sizeof(int) + key.Length + sizeof(long);
            payload[fileDataOffset + 10] ^= 0xFF;

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
            ClassicAssert.IsFalse(deserializer.IsComplete);
        }

        /// <summary>
        /// Negative file size should cause Error state.
        /// </summary>
        [Test]
        public void NegativeFileSizeIsError()
        {
            // [4-byte keyLen=0][8-byte negative fileCount]
            var payload = new byte[sizeof(int) + sizeof(long)];
            BinaryPrimitives.WriteInt32LittleEndian(payload, 0);
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(sizeof(int)), -1);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// Too-small first record (less than 4 bytes for key header) should cause Error state.
        /// </summary>
        [Test]
        public void TooSmallHeaderIsError()
        {
            var payload = new byte[2]; // Less than sizeof(int)

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// After Error state, subsequent ProcessChunk calls should return false.
        /// </summary>
        [Test]
        public void ErrorStateIsTerminal()
        {
            // Trigger error with negative file size
            var payload = new byte[sizeof(int) + sizeof(long)];
            BinaryPrimitives.WriteInt32LittleEndian(payload, 0);
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(sizeof(int)), -1);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);

            ClassicAssert.IsFalse(deserializer.ProcessChunk(new byte[100]));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// Verify the serializer preserves exact file content through round-trip.
        /// </summary>
        [Test]
        public void FileContentPreservedInRoundTrip()
        {
            var fileData = new byte[RangeIndexManager.DefaultMigrationChunkSize * 2 + 500];
            new Random(77).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "content.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("contentkey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);

            var tmpDir = Path.Combine(testDir, "rangeindex", ".migration-tmp");
            var tmpFiles = Directory.GetFiles(tmpDir, "*.bftree");
            ClassicAssert.AreEqual(1, tmpFiles.Length);

            var restoredData = File.ReadAllBytes(tmpFiles[0]);
            ClassicAssert.AreEqual(fileData.Length, restoredData.Length);
            ClassicAssert.AreEqual(fileData, restoredData);
        }

        /// <summary>
        /// Stub bytes round-trip correctly through serializer → deserializer.
        /// </summary>
        [Test]
        public void StubPreservedInRoundTrip()
        {
            var filePath = Path.Combine(testDir, "stubtest.bftree");
            File.WriteAllBytes(filePath, new byte[100]);

            var key = Encoding.UTF8.GetBytes("stubkey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, 100);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray());
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Dispose cleans up temp file.
        /// </summary>
        [Test]
        public void DisposeCleansTempFile()
        {
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            var deserializer = new RangeIndexChunkedDeserializer(manager);

            // Feed key header + key + file header with fileCount > 0 to create the file stream
            var key = Encoding.UTF8.GetBytes("tmp");
            var payload = new byte[sizeof(int) + key.Length + sizeof(long) + 10];
            var offset = 0;
            BinaryPrimitives.WriteInt32LittleEndian(payload, key.Length);
            offset += sizeof(int);
            key.CopyTo(payload.AsSpan(offset));
            offset += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset), 100);
            // Remaining 10 bytes are file data (partial)
            deserializer.ProcessChunk(payload);

            var tmpDir = Path.Combine(testDir, "rangeindex", ".migration-tmp");
            var tmpFiles = Directory.GetFiles(tmpDir, "*.bftree");
            ClassicAssert.AreEqual(1, tmpFiles.Length);
            ClassicAssert.IsTrue(File.Exists(tmpFiles[0]));

            deserializer.Dispose();

            ClassicAssert.IsFalse(File.Exists(tmpFiles[0]));
        }

        /// <summary>
        /// Startup cleanup removes .migration-tmp directory.
        /// </summary>
        [Test]
        public void StartupCleansUpMigrationTmpDir()
        {
            var tmpDir = Path.Combine(testDir, "rangeindex", ".migration-tmp");
            Directory.CreateDirectory(tmpDir);
            File.WriteAllText(Path.Combine(tmpDir, "orphan.bftree"), "leftover");

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);

            ClassicAssert.IsFalse(Directory.Exists(tmpDir));
            manager.Dispose();
        }

        /// <summary>
        /// Invalid stub size in trailer should cause Error state.
        /// </summary>
        [Test]
        public void InvalidStubSizeIsError()
        {
            var badStubSize = 10; // Not IndexSizeBytes
            var key = Encoding.UTF8.GetBytes("badstub");
            var badStub = new byte[badStubSize];

            // Compute hash of empty file
            var hasher = new XxHash64();
            Span<byte> hashBytes = stackalloc byte[sizeof(ulong)];
            hasher.GetHashAndReset(hashBytes);

            // [4-byte keyLen][key][8-byte fileCount=0][8-byte hash][4-byte badStubLen][badStub]
            var trailerSize = sizeof(ulong) + sizeof(int) + badStubSize;
            var payload = new byte[sizeof(int) + key.Length + sizeof(long) + trailerSize];
            var offset = 0;
            BinaryPrimitives.WriteInt32LittleEndian(payload, key.Length);
            offset += sizeof(int);
            key.CopyTo(payload.AsSpan(offset));
            offset += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset), 0);
            offset += sizeof(long);
            hashBytes.CopyTo(payload.AsSpan(offset));
            offset += sizeof(ulong);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset), badStubSize);
            offset += sizeof(int);
            badStub.CopyTo(payload.AsSpan(offset));

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// Key spanning multiple chunks with a tiny chunkSize round-trips correctly.
        /// </summary>
        [Test]
        public void KeySpanningMultipleChunksRoundTrip()
        {
            var filePath = Path.Combine(testDir, "tinyChunk.bftree");
            var fileData = new byte[100];
            new Random(55).NextBytes(fileData);
            File.WriteAllBytes(filePath, fileData);

            // Key larger than chunkSize to force key chunking
            var key = new byte[200];
            new Random(66).NextBytes(key);
            var stub = CreateStub();
            const int tinyChunkSize = 50;
            var buffer = new byte[tinyChunkSize];

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            var chunkCount = 0;
            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
                chunkCount++;
            }

            // Key (200 bytes) at chunkSize=50 → 4 key chunks + file chunks
            ClassicAssert.Greater(chunkCount, 4);
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Calling MoveNext after the serializer is complete should throw InvalidOperationException.
        /// </summary>
        [Test]
        public void MoveNextAfterDoneThrows()
        {
            var filePath = Path.Combine(testDir, "done.bftree");
            File.WriteAllBytes(filePath, new byte[64]);

            var key = Encoding.UTF8.GetBytes("k");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, 64);

            while (!serializer.IsComplete)
                serializer.MoveNext(buffer);

            Assert.Throws<InvalidOperationException>(() => serializer.MoveNext(buffer));
        }

        /// <summary>
        /// When totalFileBytes exceeds the actual file size, the serializer should throw EndOfStreamException.
        /// </summary>
        [Test]
        public void TruncatedFileThrowsEndOfStreamException()
        {
            var filePath = Path.Combine(testDir, "truncated.bftree");
            File.WriteAllBytes(filePath, new byte[50]);

            var key = Encoding.UTF8.GetBytes("k");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, totalFileBytes: 1000);

            Assert.Throws<EndOfStreamException>(() =>
            {
                while (!serializer.IsComplete)
                    serializer.MoveNext(buffer);
            });
        }

        /// <summary>
        /// When the destination buffer is smaller than sizeof(int), the serializer should
        /// return 0 and defer the key header to the next call.
        /// </summary>
        [Test]
        public void BufferTooSmallForKeyHeaderDefersToNextChunk()
        {
            var filePath = Path.Combine(testDir, "keyheader.bftree");
            File.WriteAllBytes(filePath, new byte[32]);

            var key = Encoding.UTF8.GetBytes("mykey");
            var stub = CreateStub();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, 32);

            // Buffer too small for 4-byte key header
            var tinyBuf = new byte[3];
            var written = serializer.MoveNext(tinyBuf);
            ClassicAssert.AreEqual(0, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Retry with adequate buffer — should complete successfully
            var buffer = CreateBuffer();
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// When the remaining buffer after key data is too small for the 8-byte file header,
        /// the serializer defers the file header to the next chunk.
        /// </summary>
        [Test]
        public void BufferTooSmallForFileHeaderDefersToNextChunk()
        {
            var filePath = Path.Combine(testDir, "fileheader.bftree");
            var fileData = new byte[64];
            new Random(42).NextBytes(fileData);
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("k");
            var stub = CreateStub();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            // Buffer fits key header (4) + key (1) but leaves < 8 bytes for file header
            // sizeof(int) + 1 + 6 = 11 → remaining after key = 6, which is < sizeof(long)
            var smallBuf = new byte[sizeof(int) + key.Length + 6];
            var written = serializer.MoveNext(smallBuf);

            // Should have written key header + key data only
            ClassicAssert.AreEqual(sizeof(int) + key.Length, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Verify key header was written correctly
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(smallBuf);
            ClassicAssert.AreEqual(key.Length, keyLen);

            // Continue with adequate buffer — should round-trip
            var buffer = CreateBuffer();
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            // Feed the first partial chunk
            ClassicAssert.IsTrue(deserializer.ProcessChunk(smallBuf.AsSpan(0, written).ToArray()));

            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// When the remaining buffer after file data is too small for the trailer,
        /// the serializer defers the trailer to the next chunk.
        /// </summary>
        [Test]
        public void BufferTooSmallForTrailerDefersToNextChunk()
        {
            var fileData = new byte[64];
            new Random(42).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "trailer.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("k");
            var stub = CreateStub();
            var trailerSize = sizeof(ulong) + sizeof(int) + stub.Length;

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            // Buffer fits everything except the trailer: keyHeader(4) + key(1) + fileHeader(8) + fileData(64) + (trailerSize - 1)
            var bufSize = sizeof(int) + key.Length + sizeof(long) + fileData.Length + trailerSize - 1;
            var buf = new byte[bufSize];
            var written = serializer.MoveNext(buf);

            // Should have written everything except the trailer
            ClassicAssert.AreEqual(sizeof(int) + key.Length + sizeof(long) + fileData.Length, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Next call with adequate buffer should emit the trailer
            var buffer = CreateBuffer();
            var trailerLen = serializer.MoveNext(buffer);
            ClassicAssert.AreEqual(trailerSize, trailerLen);
            ClassicAssert.IsTrue(serializer.IsComplete);

            // Round-trip through deserializer
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsTrue(deserializer.ProcessChunk(buf.AsSpan(0, written).ToArray()));
            ClassicAssert.IsFalse(deserializer.IsComplete);
            ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, trailerLen).ToArray()));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
        }

        /// <summary>
        /// File size that is an exact multiple of the chunk size — verifies that
        /// phase transitions at exact chunk boundaries work correctly.
        /// </summary>
        [Test]
        public void ExactPhaseBoundaryTransitions()
        {
            var key = Encoding.UTF8.GetBytes("k");
            var stub = CreateStub();
            const int chunkSize = 64;
            var headerOverhead = sizeof(int) + key.Length + sizeof(long);

            // File size = chunkSize - headerOverhead so that key+fileHeader+fileData fills exactly one chunk,
            // leaving the trailer for the next chunk
            var fileSize = chunkSize - headerOverhead;
            var fileData = new byte[fileSize];
            new Random(77).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "boundary.bftree");
            File.WriteAllBytes(filePath, fileData);

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileSize);

            var buffer = new byte[chunkSize];

            // First chunk: should contain key header + key + file header + all file data
            var len1 = serializer.MoveNext(buffer);
            ClassicAssert.AreEqual(chunkSize, len1);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Second chunk: should contain trailer only
            var len2 = serializer.MoveNext(buffer);
            var trailerSize = sizeof(ulong) + sizeof(int) + stub.Length;
            ClassicAssert.AreEqual(trailerSize, len2);
            ClassicAssert.IsTrue(serializer.IsComplete);

            // Round-trip
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            var allPayload = new byte[len1 + len2];
            buffer.AsSpan(0, len1).CopyTo(allPayload); // reuse buffer for chunk 2, so must reconstruct

            // Re-run to get clean data
            fs.Seek(0, SeekOrigin.Begin);
            using var serializer2 = new RangeIndexChunkedSerializer(fs, key, stub, fileSize);
            using var deserializer2 = new RangeIndexChunkedDeserializer(manager);

            while (!serializer2.IsComplete)
            {
                var len = serializer2.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer2.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer2.IsComplete);
            ClassicAssert.IsFalse(deserializer2.HasError);
        }

        /// <summary>
        /// Calling Dispose twice should not throw.
        /// </summary>
        [Test]
        public void DoubleDisposeIsIdempotent()
        {
            var filePath = Path.Combine(testDir, "dispose.bftree");
            File.WriteAllBytes(filePath, new byte[16]);

            var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            var serializer = new RangeIndexChunkedSerializer(fs, Encoding.UTF8.GetBytes("k"), CreateStub(), 16);

            serializer.Dispose();
            serializer.Dispose(); // Should not throw
        }

        /// <summary>
        /// Zero-length key should serialize and round-trip correctly.
        /// </summary>
        [Test]
        public void ZeroLengthKeyRoundTrip()
        {
            var fileData = new byte[100];
            new Random(88).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "emptykey.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Array.Empty<byte>();
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(0, deserializer.Key.Length);
        }

        /// <summary>
        /// Verify the xxHash64 checksum in the trailer matches a manually computed hash
        /// over the file data bytes.
        /// </summary>
        [Test]
        public void TrailerChecksumAndStubContentVerification()
        {
            var fileData = new byte[256];
            new Random(44).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "checksum.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("hashkey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            var len = serializer.MoveNext(buffer);
            ClassicAssert.IsTrue(serializer.IsComplete);

            var payload = buffer.AsSpan(0, len);

            // Parse trailer from the end: [8-byte hash][4-byte stubLen][stub]
            var trailerSize = sizeof(ulong) + sizeof(int) + stub.Length;
            var trailerStart = len - trailerSize;

            var hashFromPayload = BinaryPrimitives.ReadUInt64LittleEndian(payload[trailerStart..]);
            var stubLenFromPayload = BinaryPrimitives.ReadInt32LittleEndian(payload[(trailerStart + sizeof(ulong))..]);
            var stubFromPayload = payload[(trailerStart + sizeof(ulong) + sizeof(int))..
                                          (trailerStart + sizeof(ulong) + sizeof(int) + stub.Length)].ToArray();

            // Verify stub content
            ClassicAssert.AreEqual(RangeIndexManager.IndexSizeBytes, stubLenFromPayload);
            ClassicAssert.AreEqual(stub, stubFromPayload);

            // Manually compute xxHash64 over file data bytes in the payload
            var fileDataOffset = sizeof(int) + key.Length + sizeof(long);
            var fileDataFromPayload = payload[fileDataOffset..(fileDataOffset + fileData.Length)];

            var manualHasher = new XxHash64();
            manualHasher.Append(fileDataFromPayload);
            Span<byte> manualHashBytes = stackalloc byte[sizeof(ulong)];
            manualHasher.GetHashAndReset(manualHashBytes);
            var manualHash = BinaryPrimitives.ReadUInt64LittleEndian(manualHashBytes);

            ClassicAssert.AreEqual(manualHash, hashFromPayload);
        }

        /// <summary>
        /// IsComplete should be false during the entire serialization process
        /// and only become true after the final MoveNext emits the trailer.
        /// </summary>
        [Test]
        public void IsCompleteTransitionsCorrectly()
        {
            var fileData = new byte[RangeIndexManager.DefaultMigrationChunkSize + 100];
            new Random(33).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "complete.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("progresskey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            ClassicAssert.IsFalse(serializer.IsComplete);

            var chunkCount = 0;
            while (!serializer.IsComplete)
            {
                if (chunkCount > 0)
                    ClassicAssert.IsFalse(serializer.IsComplete);
                serializer.MoveNext(buffer);
                chunkCount++;
            }

            ClassicAssert.IsTrue(serializer.IsComplete);
            ClassicAssert.Greater(chunkCount, 1);
        }

        /// <summary>
        /// When totalFileBytes is less than the actual file size, the serializer
        /// should only emit the declared number of bytes (truncated prefix).
        /// </summary>
        [Test]
        public void DeclaredSizeSmallerThanActualFileEmitsTruncatedPrefix()
        {
            var fullFileData = new byte[500];
            new Random(55).NextBytes(fullFileData);
            var filePath = Path.Combine(testDir, "shorter.bftree");
            File.WriteAllBytes(filePath, fullFileData);

            var declaredSize = 200L;
            var key = Encoding.UTF8.GetBytes("shortkey");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, declaredSize);

            var len = serializer.MoveNext(buffer);
            ClassicAssert.IsTrue(serializer.IsComplete);

            // Verify file count in payload matches declared size, not actual
            var fileSizeOffset = sizeof(int) + key.Length;
            var fileSizeFromPayload = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(fileSizeOffset));
            ClassicAssert.AreEqual(declaredSize, fileSizeFromPayload);

            // Round-trip through deserializer
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);

            // Verify the temp file contains exactly declaredSize bytes
            var tmpDir = Path.Combine(testDir, "rangeindex", ".migration-tmp");
            var tmpFiles = Directory.GetFiles(tmpDir, "*.bftree");
            ClassicAssert.AreEqual(1, tmpFiles.Length);
            var restoredData = File.ReadAllBytes(tmpFiles[0]);
            ClassicAssert.AreEqual(declaredSize, restoredData.Length);
            ClassicAssert.AreEqual(fullFileData.AsSpan(0, (int)declaredSize).ToArray(), restoredData);
        }

        /// <summary>
        /// When the destination buffer becomes empty exactly when entering the FileData phase
        /// (e.g., the buffer was fully consumed by key header + key + file header), the
        /// serializer should return the bytes written so far without throwing.
        /// </summary>
        [Test]
        public void BufferExhaustedAtFileDataPhaseDoesNotThrow()
        {
            var fileData = new byte[100];
            new Random(42).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "exhausted.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("k");
            var stub = CreateStub();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            // Buffer exactly fits key header (4) + key (1) + file header (8) = 13
            // This leaves 0 bytes for file data
            var exactBuf = new byte[sizeof(int) + key.Length + sizeof(long)];
            var written = serializer.MoveNext(exactBuf);

            // Should have written all 13 bytes without throwing
            ClassicAssert.AreEqual(exactBuf.Length, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Continue with adequate buffer — should complete and round-trip
            var buffer = CreateBuffer();
            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            ClassicAssert.IsTrue(deserializer.ProcessChunk(exactBuf.AsSpan(0, written).ToArray()));

            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
        }

        /// <summary>
        /// Serializing with a very small buffer that forces key data and file data
        /// to span many chunks. The buffer must be at least as large as the trailer
        /// (the largest element that must fit entirely), but forces data to be emitted
        /// in small increments.
        /// </summary>
        [Test]
        public void SmallBufferRoundTrip()
        {
            var fileData = new byte[50];
            new Random(99).NextBytes(fileData);
            var filePath = Path.Combine(testDir, "smallbuf.bftree");
            File.WriteAllBytes(filePath, fileData);

            var key = Encoding.UTF8.GetBytes("abcdef");
            var stub = CreateStub();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using var serializer = new RangeIndexChunkedSerializer(fs, key, stub, fileData.Length);

            var manager = new RangeIndexManager(enabled: true, dataDir: testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager);

            // Buffer must fit the trailer (largest atomic element):
            // sizeof(ulong) + sizeof(int) + stub.Length
            var trailerSize = sizeof(ulong) + sizeof(int) + stub.Length;
            var buffer = new byte[trailerSize];

            using var allChunks = new MemoryStream();
            var chunkCount = 0;

            while (!serializer.IsComplete)
            {
                var len = serializer.MoveNext(buffer);
                if (len > 0)
                    allChunks.Write(buffer, 0, len);
                chunkCount++;

                ClassicAssert.Less(chunkCount, 1000, "Serializer did not complete within expected iterations");
            }

            // With a small buffer, data should span multiple chunks
            ClassicAssert.Greater(chunkCount, 1);

            // Feed entire concatenated payload to deserializer at once
            ClassicAssert.IsTrue(deserializer.ProcessChunk(allChunks.ToArray()));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }
    }
}
