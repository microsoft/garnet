// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Hashing;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
    [TestFixture]
    public class RangeIndexChunkedSerializerTests : TestBase
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
        /// Test helper: drives the serializer with file data from a FileStream, mimicking the
        /// old single-call MoveNext API. Reads file data synchronously and feeds it to the serializer.
        /// May call MoveNext multiple times to handle phase transitions within one "logical" chunk.
        /// </summary>
        private static int SerializerMoveNext(RangeIndexChunkedSerializer serializer, Span<byte> destination, FileStream fileStream)
        {
            var totalWritten = 0;

            while (!serializer.IsComplete && destination.Length > 0)
            {
                if (serializer.NeedsFileData)
                {
                    var fileBuffer = new byte[destination.Length];
                    var maxRead = (int)Math.Min(fileBuffer.Length, serializer.FileDataRemaining);
                    var bytesRead = fileStream.Read(fileBuffer, 0, maxRead);
                    if (bytesRead == 0 && serializer.FileDataRemaining > 0)
                        throw new Exception($"RangeIndex file truncated: {serializer.FileDataRemaining} bytes remaining");
                    serializer.SupplyFileData(fileBuffer.AsMemory(0, bytesRead));
                }

                var written = serializer.MoveNext(destination);
                if (written == 0)
                    break;

                destination = destination[written..];
                totalWritten += written;
            }

            return totalWritten;
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            var len = SerializerMoveNext(serializer, buffer, fs);
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
            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileSize);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            var chunkCount = 0;
            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
                chunkCount++;
            }

            ClassicAssert.Greater(chunkCount, 1);
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Empty file (0 bytes) — deserializer rejects zero file size since BfTree
        /// snapshots always have at least header/metadata bytes.
        /// </summary>
        [Test]
        public void EmptyFileRejectedByDeserializer()
        {
            var key = Encoding.UTF8.GetBytes("emptykey");
            var fileData = Array.Empty<byte>();
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            var len = SerializerMoveNext(serializer, buffer, fs);
            ClassicAssert.Greater(len, 0);
            var payload = buffer.AsSpan(0, len).ToArray();

            // Corrupt a file data byte (after keyLen + key + fileCount)
            var fileDataOffset = sizeof(int) + key.Length + sizeof(long);
            payload[fileDataOffset + 10] ^= 0xFF;

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

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

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// A file-size header that does not fit entirely in one chunk (fewer than 8 bytes
        /// available at WaitingForFileHeader) must transition to Error.
        /// </summary>
        [Test]
        public void SplitFileHeaderIsError()
        {
            var key = Encoding.UTF8.GetBytes("k");

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Chunk 1: full key header + key → deserializer is now at WaitingForFileHeader.
            var keyChunk = new byte[sizeof(int) + key.Length];
            BinaryPrimitives.WriteInt32LittleEndian(keyChunk, key.Length);
            key.CopyTo(keyChunk.AsSpan(sizeof(int)));
            ClassicAssert.IsTrue(deserializer.ProcessChunk(keyChunk));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Chunk 2: only 4 bytes — less than the 8-byte file-size header → Error.
            ClassicAssert.IsFalse(deserializer.ProcessChunk(new byte[sizeof(int)]));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// A trailer chunk smaller than the fixed [8-byte hash][4-byte stubLen] header
        /// must transition to Error.
        /// </summary>
        [Test]
        public void TrailerTooSmallIsError()
        {
            var key = Encoding.UTF8.GetBytes("k");
            var fileData = new byte[] { 0xAB }; // 1-byte file so we reach WaitingForTrailer

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Chunk 1: key header + key + file header + the full file → now at WaitingForTrailer.
            var preTrailer = new byte[sizeof(int) + key.Length + sizeof(long) + fileData.Length];
            var o = 0;
            BinaryPrimitives.WriteInt32LittleEndian(preTrailer.AsSpan(o), key.Length);
            o += sizeof(int);
            key.CopyTo(preTrailer.AsSpan(o));
            o += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(preTrailer.AsSpan(o), fileData.Length);
            o += sizeof(long);
            fileData.CopyTo(preTrailer.AsSpan(o));
            ClassicAssert.IsTrue(deserializer.ProcessChunk(preTrailer));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Chunk 2: a trailer of only 11 bytes — less than the 12-byte [hash][stubLen] header → Error.
            ClassicAssert.IsFalse(deserializer.ProcessChunk(new byte[sizeof(ulong) + sizeof(int) - 1]));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// An I/O failure when opening the temp snapshot file (e.g., parent directory missing)
        /// must transition to the Error state and return false, not throw out of ProcessChunk.
        /// </summary>
        [Test]
        public void FileOpenFailureGoesToErrorState()
        {
            // Temp path under a directory that does not exist → FileStream(Create) throws.
            var badPath = Path.Combine(testDir, "no", "such", "dir", "snapshot.bftree");

            using var deserializer = new RangeIndexChunkedDeserializer(badPath);

            // [4-byte keyLen=1]['k'][8-byte fileSize=10] — reaches the FileStream open in ReceivingFileData.
            var key = Encoding.UTF8.GetBytes("k");
            var payload = new byte[sizeof(int) + key.Length + sizeof(long)];
            BinaryPrimitives.WriteInt32LittleEndian(payload, key.Length);
            key.CopyTo(payload.AsSpan(sizeof(int)));
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(sizeof(int) + key.Length), 10);

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

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

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

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
                ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);

            var tmpDir = Path.Combine(testDir, "migration-tmp");
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, 100);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
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
            var manager = new RangeIndexManager(testDir);
            var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

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

            var tmpDir = Path.Combine(testDir, "migration-tmp");
            var tmpFiles = Directory.GetFiles(tmpDir, "*.bftree");
            ClassicAssert.AreEqual(1, tmpFiles.Length);
            ClassicAssert.IsTrue(File.Exists(tmpFiles[0]));

            deserializer.Dispose();

            ClassicAssert.IsFalse(File.Exists(tmpFiles[0]));
        }

        /// <summary>
        /// Startup cleanup removes migration-tmp directory contents.
        /// </summary>
        [Test]
        public void StartupCleansUpMigrationTmpDir()
        {
            var tmpDir = Path.Combine(testDir, "migration-tmp");
            Directory.CreateDirectory(tmpDir);
            File.WriteAllText(Path.Combine(tmpDir, "orphan.bftree"), "leftover");

            var manager = new RangeIndexManager(testDir);

            ClassicAssert.IsTrue(Directory.Exists(tmpDir));
            ClassicAssert.AreEqual(0, Directory.GetFiles(tmpDir).Length);
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
            var fileData = new byte[] { 0xAB }; // 1-byte file so we reach the trailer

            // Compute hash of file data
            var hasher = new XxHash64();
            hasher.Append(fileData);
            Span<byte> hashBytes = stackalloc byte[sizeof(ulong)];
            hasher.GetHashAndReset(hashBytes);

            // [4-byte keyLen][key][8-byte fileSize][fileData][8-byte hash][4-byte badStubLen][badStub]
            var trailerSize = sizeof(ulong) + sizeof(int) + badStubSize;
            var payload = new byte[sizeof(int) + key.Length + sizeof(long) + fileData.Length + trailerSize];
            var offset = 0;
            BinaryPrimitives.WriteInt32LittleEndian(payload, key.Length);
            offset += sizeof(int);
            key.CopyTo(payload.AsSpan(offset));
            offset += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset), fileData.Length);
            offset += sizeof(long);
            fileData.CopyTo(payload.AsSpan(offset));
            offset += fileData.Length;
            hashBytes.CopyTo(payload.AsSpan(offset));
            offset += sizeof(ulong);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset), badStubSize);
            offset += sizeof(int);
            badStub.CopyTo(payload.AsSpan(offset));

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// A trailer that declares the correct stub length but is truncated (fewer actual
        /// stub bytes than declared) must transition to Error rather than throw.
        /// </summary>
        [Test]
        public void TruncatedStubIsError()
        {
            var key = Encoding.UTF8.GetBytes("truncstub");
            var fileData = new byte[] { 0xAB }; // 1-byte file so we reach the trailer

            var hasher = new XxHash64();
            hasher.Append(fileData);
            Span<byte> hashBytes = stackalloc byte[sizeof(ulong)];
            hasher.GetHashAndReset(hashBytes);

            // Declare stubLen == IndexSizeBytes but supply fewer actual stub bytes (truncated).
            var actualStubBytes = RangeIndexManager.IndexSizeBytes - 1;
            // [4-byte keyLen][key][8-byte fileSize][fileData][8-byte hash][4-byte stubLen][truncated stub]
            var payload = new byte[sizeof(int) + key.Length + sizeof(long) + fileData.Length + sizeof(ulong) + sizeof(int) + actualStubBytes];
            var offset = 0;
            BinaryPrimitives.WriteInt32LittleEndian(payload, key.Length);
            offset += sizeof(int);
            key.CopyTo(payload.AsSpan(offset));
            offset += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset), fileData.Length);
            offset += sizeof(long);
            fileData.CopyTo(payload.AsSpan(offset));
            offset += fileData.Length;
            hashBytes.CopyTo(payload.AsSpan(offset));
            offset += sizeof(ulong);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset), RangeIndexManager.IndexSizeBytes);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Must cleanly report failure (no ArgumentOutOfRangeException).
            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// A trailer that declares the correct stub length but carries extra bytes after the stub
        /// (over-long trailer) must transition to Error — the stream ends after the stub.
        /// </summary>
        [Test]
        public void OverLongTrailerIsError()
        {
            var key = Encoding.UTF8.GetBytes("overlong");
            var fileData = new byte[] { 0xAB }; // 1-byte file so we reach the trailer

            var hasher = new XxHash64();
            hasher.Append(fileData);
            Span<byte> hashBytes = stackalloc byte[sizeof(ulong)];
            hasher.GetHashAndReset(hashBytes);

            // Declare stubLen == IndexSizeBytes but supply extra trailing bytes after the stub.
            const int extraBytes = 3;
            // [4-byte keyLen][key][8-byte fileSize][fileData][8-byte hash][4-byte stubLen][stub][extra]
            var payload = new byte[sizeof(int) + key.Length + sizeof(long) + fileData.Length + sizeof(ulong) + sizeof(int) + RangeIndexManager.IndexSizeBytes + extraBytes];
            var offset = 0;
            BinaryPrimitives.WriteInt32LittleEndian(payload, key.Length);
            offset += sizeof(int);
            key.CopyTo(payload.AsSpan(offset));
            offset += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset), fileData.Length);
            offset += sizeof(long);
            fileData.CopyTo(payload.AsSpan(offset));
            offset += fileData.Length;
            hashBytes.CopyTo(payload.AsSpan(offset));
            offset += sizeof(ulong);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset), RangeIndexManager.IndexSizeBytes);
            // stub bytes + extraBytes are left zero-initialized.

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            var chunkCount = 0;
            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, 64);

            while (!serializer.IsComplete)
                SerializerMoveNext(serializer, buffer, fs);

            Assert.Throws<InvalidOperationException>(() => serializer.MoveNext(buffer));
        }

        /// <summary>
        /// When totalFileBytes exceeds the actual file size, the serializer should throw EndOfStreamException.
        /// </summary>
        [Test]
        public void TruncatedFileThrowsException()
        {
            var filePath = Path.Combine(testDir, "truncated.bftree");
            File.WriteAllBytes(filePath, new byte[50]);

            var key = Encoding.UTF8.GetBytes("k");
            var stub = CreateStub();
            var buffer = CreateBuffer();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            var serializer = new RangeIndexChunkedSerializer(key, stub, totalFileBytes: 1000);

            Assert.Throws<Exception>(() =>
            {
                while (!serializer.IsComplete)
                    SerializerMoveNext(serializer, buffer, fs);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, 32);

            // Buffer too small for 4-byte key header
            var tinyBuf = new byte[3];
            var written = SerializerMoveNext(serializer, tinyBuf, fs);
            ClassicAssert.AreEqual(0, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Retry with adequate buffer — should complete successfully
            var buffer = CreateBuffer();
            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            // Buffer fits key header (4) + key (1) but leaves < 8 bytes for file header
            // sizeof(int) + 1 + 6 = 11 → remaining after key = 6, which is < sizeof(long)
            var smallBuf = new byte[sizeof(int) + key.Length + 6];
            var written = SerializerMoveNext(serializer, smallBuf, fs);

            // Should have written key header + key data only
            ClassicAssert.AreEqual(sizeof(int) + key.Length, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Verify key header was written correctly
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(smallBuf);
            ClassicAssert.AreEqual(key.Length, keyLen);

            // Continue with adequate buffer — should round-trip
            var buffer = CreateBuffer();
            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Feed the first partial chunk
            ClassicAssert.IsTrue(deserializer.ProcessChunk(smallBuf.AsSpan(0, written).ToArray()));

            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            // Buffer fits everything except the trailer: keyHeader(4) + key(1) + fileHeader(8) + fileData(64) + (trailerSize - 1)
            var bufSize = sizeof(int) + key.Length + sizeof(long) + fileData.Length + trailerSize - 1;
            var buf = new byte[bufSize];
            var written = SerializerMoveNext(serializer, buf, fs);

            // Should have written everything except the trailer
            ClassicAssert.AreEqual(sizeof(int) + key.Length + sizeof(long) + fileData.Length, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Next call with adequate buffer should emit the trailer
            var buffer = CreateBuffer();
            var trailerLen = SerializerMoveNext(serializer, buffer, fs);
            ClassicAssert.AreEqual(trailerSize, trailerLen);
            ClassicAssert.IsTrue(serializer.IsComplete);

            // Round-trip through deserializer
            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileSize);

            var buffer = new byte[chunkSize];

            // First chunk: should contain key header + key + file header + all file data
            var len1 = SerializerMoveNext(serializer, buffer, fs);
            ClassicAssert.AreEqual(chunkSize, len1);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Second chunk: should contain trailer only
            var len2 = SerializerMoveNext(serializer, buffer, fs);
            var trailerSize = sizeof(ulong) + sizeof(int) + stub.Length;
            ClassicAssert.AreEqual(trailerSize, len2);
            ClassicAssert.IsTrue(serializer.IsComplete);

            // Round-trip
            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            var allPayload = new byte[len1 + len2];
            buffer.AsSpan(0, len1).CopyTo(allPayload); // reuse buffer for chunk 2, so must reconstruct

            // Re-run to get clean data
            fs.Seek(0, SeekOrigin.Begin);
            var serializer2 = new RangeIndexChunkedSerializer(key, stub, fileSize);
            using var deserializer2 = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            while (!serializer2.IsComplete)
            {
                var len = SerializerMoveNext(serializer2, buffer, fs);
                ClassicAssert.IsTrue(deserializer2.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            }

            ClassicAssert.IsTrue(deserializer2.IsComplete);
            ClassicAssert.IsFalse(deserializer2.HasError);
        }

        /// <summary>
        /// Calling Dispose twice on the migration reader should not throw, and the
        /// temp snapshot file should be deleted after the first Dispose call.
        /// </summary>
        [Test]
        public void DoubleDisposeIsIdempotent()
        {
            var filePath = Path.Combine(testDir, "dispose.bftree");
            File.WriteAllBytes(filePath, new byte[16]);

            var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            var serializer = new RangeIndexChunkedSerializer(Encoding.UTF8.GetBytes("k"), CreateStub(), 16);
            var reader = new RangeIndexMigrationReader(serializer, fs, filePath, readBufferSize: 256);

            reader.Dispose();
            ClassicAssert.IsFalse(File.Exists(filePath), "Temp snapshot file should be deleted on dispose");
            reader.Dispose(); // Should not throw
        }

        /// <summary>
        /// Zero-length key is rejected by the deserializer since keys must have non-zero length.
        /// </summary>
        [Test]
        public void ZeroLengthKeyRejectedByDeserializer()
        {
            var key = Array.Empty<byte>();
            var fileData = new byte[] { 0x01 };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            var len = SerializerMoveNext(serializer, buffer, fs);
            ClassicAssert.IsTrue(serializer.IsComplete);

            var payload = buffer.AsSpan(0, len);

            // Parse trailer from the end: [8-byte hash][4-byte stubLen][stub]
            var trailerSize = sizeof(ulong) + sizeof(int) + stub.Length;
            var trailerStart = len - trailerSize;

            var hashFromPayload = BinaryPrimitives.ReadUInt64LittleEndian(payload[trailerStart..]);
            var stubLenFromPayload = BinaryPrimitives.ReadInt32LittleEndian(payload[(trailerStart + sizeof(ulong))..]);
            var stubFromPayload = payload[(trailerStart + sizeof(ulong) + sizeof(int))..(trailerStart + sizeof(ulong) + sizeof(int) + stub.Length)].ToArray();

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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            ClassicAssert.IsFalse(serializer.IsComplete);

            var chunkCount = 0;
            while (!serializer.IsComplete)
            {
                if (chunkCount > 0)
                    ClassicAssert.IsFalse(serializer.IsComplete);
                SerializerMoveNext(serializer, buffer, fs);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, declaredSize);

            var len = SerializerMoveNext(serializer, buffer, fs);
            ClassicAssert.IsTrue(serializer.IsComplete);

            // Verify file count in payload matches declared size, not actual
            var fileSizeOffset = sizeof(int) + key.Length;
            var fileSizeFromPayload = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(fileSizeOffset));
            ClassicAssert.AreEqual(declaredSize, fileSizeFromPayload);

            // Round-trip through writer
            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);

            // Verify the temp file contains exactly declaredSize bytes
            var tmpDir = Path.Combine(testDir, "migration-tmp");
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            // Buffer exactly fits key header (4) + key (1) + file header (8) = 13
            // This leaves 0 bytes for file data
            var exactBuf = new byte[sizeof(int) + key.Length + sizeof(long)];
            var written = SerializerMoveNext(serializer, exactBuf, fs);

            // Should have written all 13 bytes without throwing
            ClassicAssert.AreEqual(exactBuf.Length, written);
            ClassicAssert.IsFalse(serializer.IsComplete);

            // Continue with adequate buffer — should complete and round-trip
            var buffer = CreateBuffer();
            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsTrue(deserializer.ProcessChunk(exactBuf.AsSpan(0, written).ToArray()));

            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
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
            var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Buffer must fit the trailer (largest atomic element):
            // sizeof(ulong) + sizeof(int) + stub.Length
            var trailerSize = sizeof(ulong) + sizeof(int) + stub.Length;
            var buffer = new byte[trailerSize];

            using var allChunks = new MemoryStream();
            var chunkCount = 0;

            while (!serializer.IsComplete)
            {
                var len = SerializerMoveNext(serializer, buffer, fs);
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

        #region Deserializer state machine corner cases

        /// <summary>
        /// Helper: build a valid serialized payload from raw bytes (no real BfTree needed).
        /// </summary>
        private static byte[] BuildPayload(byte[] key, byte[] fileData, byte[] stub)
        {
            var hasher = new XxHash64();
            hasher.Append(fileData);
            Span<byte> hashBytes = stackalloc byte[sizeof(ulong)];
            hasher.GetHashAndReset(hashBytes);

            var totalLen = sizeof(int) + key.Length + sizeof(long) + fileData.Length
                         + sizeof(ulong) + sizeof(int) + stub.Length;
            var payload = new byte[totalLen];
            var offset = 0;

            BinaryPrimitives.WriteInt32LittleEndian(payload, key.Length);
            offset += sizeof(int);
            key.CopyTo(payload.AsSpan(offset));
            offset += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset), fileData.Length);
            offset += sizeof(long);
            fileData.CopyTo(payload.AsSpan(offset));
            offset += fileData.Length;
            hashBytes.CopyTo(payload.AsSpan(offset));
            offset += sizeof(ulong);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset), stub.Length);
            offset += sizeof(int);
            stub.CopyTo(payload.AsSpan(offset));

            return payload;
        }

        /// <summary>
        /// Chunk ends exactly when fileBytesRemaining hits zero — no trailer in that chunk.
        /// Verifies file is flushed and closed, then trailer parsed from next chunk.
        /// </summary>
        [Test]
        public void FileDataExactlyFillsChunk_TrailerInNextChunk()
        {
            var key = Encoding.UTF8.GetBytes("testkey");
            var fileData = new byte[100];
            new Random(42).NextBytes(fileData);
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            // Split: chunk 1 = key header + key + file header + all file data (no trailer)
            var fileDataEnd = sizeof(int) + key.Length + sizeof(long) + fileData.Length;

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Chunk 1: everything up to end of file data
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(0, fileDataEnd)));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // File should be flushed to disk
            var tmpDir = Path.Combine(testDir, "migration-tmp");
            var tmpFiles = Directory.GetFiles(tmpDir, "*.bftree");
            ClassicAssert.AreEqual(1, tmpFiles.Length);
            var writtenData = File.ReadAllBytes(tmpFiles[0]);
            ClassicAssert.AreEqual(fileData.Length, writtenData.Length);
            ClassicAssert.AreEqual(fileData, writtenData);

            // Chunk 2: trailer only
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(fileDataEnd)));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
            ClassicAssert.AreEqual(stub, deserializer.Stub.ToArray());
        }

        /// <summary>
        /// File data arrives one byte at a time — each chunk has exactly one file byte.
        /// </summary>
        [Test]
        public void FileDataOneBytePerChunk()
        {
            var key = Encoding.UTF8.GetBytes("k");
            var fileData = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            var headerEnd = sizeof(int) + key.Length + sizeof(long);

            // Chunk 1: key header + key + file header
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(0, headerEnd)));

            // Chunks 2-5: one file byte each
            for (var i = 0; i < fileData.Length; i++)
            {
                ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(headerEnd + i, 1)));
                ClassicAssert.IsFalse(deserializer.IsComplete);
            }

            // Final chunk: trailer
            var trailerStart = headerEnd + fileData.Length;
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(trailerStart)));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Empty chunk (zero bytes) during WaitingForTrailer state — should be accepted gracefully.
        /// </summary>
        [Test]
        public void EmptyChunkDuringWaitingForTrailer()
        {
            var key = Encoding.UTF8.GetBytes("k");
            var fileData = new byte[] { 0x01, 0x02 };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            var fileDataEnd = sizeof(int) + key.Length + sizeof(long) + fileData.Length;

            // Send everything up to end of file data
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(0, fileDataEnd)));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Empty chunk — should be accepted, still waiting for trailer
            ClassicAssert.IsTrue(deserializer.ProcessChunk([]));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Now send the trailer
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(fileDataEnd)));
            ClassicAssert.IsTrue(deserializer.IsComplete);
        }

        /// <summary>
        /// Empty chunk (zero bytes) while receiving file data (file not yet fully received),
        /// in ReceivingFileData — should be a graceful no-op, leaving the stream still receivable.
        /// </summary>
        [Test]
        public void EmptyChunkDuringReceivingFileData()
        {
            var key = Encoding.UTF8.GetBytes("mykey");
            var fileData = new byte[] { 0x01, 0x02, 0x03, 0x04 };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Send key + file header + only the first 2 of 4 file bytes — now mid-ReceivingFileData.
            var partialEnd = sizeof(int) + key.Length + sizeof(long) + 2;
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(0, partialEnd)));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Empty chunk — no-op, no error, still receiving file data.
            ClassicAssert.IsTrue(deserializer.ProcessChunk([]));
            ClassicAssert.IsFalse(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);

            // Send the rest (remaining file bytes + trailer) — stream completes.
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(partialEnd)));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Empty chunk (zero bytes) delivered as the very first input, while in WaitingForKeyHeader —
        /// should be a graceful no-op, leaving the stream still receivable.
        /// </summary>
        [Test]
        public void EmptyChunkAtWaitingForKeyHeader()
        {
            var key = Encoding.UTF8.GetBytes("mykey");
            var fileData = new byte[] { 0x01, 0x02 };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Empty chunk before any data — no-op, no error, not complete.
            ClassicAssert.IsTrue(deserializer.ProcessChunk([]));
            ClassicAssert.IsFalse(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);

            // The full stream still completes correctly afterwards.
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Empty chunk (zero bytes) after the full key is received but before the file size header,
        /// while in WaitingForFileHeader — should be a graceful no-op.
        /// </summary>
        [Test]
        public void EmptyChunkDuringWaitingForFileHeader()
        {
            var key = Encoding.UTF8.GetBytes("mykey");
            var fileData = new byte[] { 0x01, 0x02 };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Send exactly the key length header + key bytes — deserializer is now in WaitingForFileHeader.
            var keyEnd = sizeof(int) + key.Length;
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(0, keyEnd)));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Empty chunk — no-op, no error, still waiting for the file header.
            ClassicAssert.IsTrue(deserializer.ProcessChunk([]));
            ClassicAssert.IsFalse(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);

            // Send the rest (file header + file data + trailer) — stream completes.
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(keyEnd)));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Key spans multiple chunks — first chunk has partial key, second has the rest + file + trailer.
        /// </summary>
        [Test]
        public void KeySplitAcrossTwoChunks()
        {
            var key = Encoding.UTF8.GetBytes("longkeyname");
            var fileData = new byte[] { 0xFF };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Chunk 1: key header (4 bytes) + first 3 bytes of key
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(0, sizeof(int) + 3)));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Chunk 2: rest of key + file header + file data + trailer
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(sizeof(int) + 3)));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        /// <summary>
        /// Zero-length file data is rejected — BfTree snapshots always have content.
        /// </summary>
        [Test]
        public void ZeroFileDataRejectedByDeserializer()
        {
            var key = Encoding.UTF8.GetBytes("k");
            var fileData = Array.Empty<byte>();
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// Corrupted file data produces checksum mismatch in the trailer.
        /// </summary>
        [Test]
        public void CorruptedFileDataFailsChecksumInTrailer()
        {
            var key = Encoding.UTF8.GetBytes("k");
            var fileData = new byte[] { 0x01, 0x02, 0x03 };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            // Corrupt a file data byte
            var fileDataOffset = sizeof(int) + key.Length + sizeof(long);
            payload[fileDataOffset] ^= 0xFF;

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            ClassicAssert.IsFalse(deserializer.ProcessChunk(payload));
            ClassicAssert.IsTrue(deserializer.HasError);
        }

        /// <summary>
        /// Key header arrives alone (no key bytes in same chunk) — deserializer should
        /// accept and wait for key data in subsequent chunks.
        /// </summary>
        [Test]
        public void KeyHeaderAloneInChunk()
        {
            var key = Encoding.UTF8.GetBytes("mykey");
            var fileData = new byte[] { 0x01 };
            var stub = CreateStub();
            var payload = BuildPayload(key, fileData, stub);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            // Chunk 1: only the 4-byte key length header, no key bytes
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(0, sizeof(int))));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Empty chunk — no progress, still waiting for key data
            ClassicAssert.IsTrue(deserializer.ProcessChunk([]));
            ClassicAssert.IsFalse(deserializer.IsComplete);

            // Chunk 3: rest of payload
            ClassicAssert.IsTrue(deserializer.ProcessChunk(payload.AsSpan(sizeof(int))));
            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
        }

        #endregion

        #region Reader/serializer parity round-trip

        /// <summary>
        /// Identifies which chunk-producing path drives a round-trip: the synchronous serializer
        /// test helper (<see cref="SerializerMoveNext"/>), or the real async production
        /// <see cref="RangeIndexMigrationReader"/>. Round-trip assertions are shared across both so
        /// the reader path gets the same coverage as the serializer.
        /// </summary>
        public enum ChunkDriver { SerializerHelper, MigrationReader }

        private static byte[] RandomBytes(int length, int seed)
        {
            var bytes = new byte[length];
            new Random(seed).NextBytes(bytes);
            return bytes;
        }

        /// <summary>
        /// Drive a full key + file + stub stream to completion using the chosen
        /// <paramref name="driver"/>, feeding each produced chunk into a fresh deserializer, then
        /// assert the key, stub, and file content all round-trip intact. The two drivers share this
        /// body, so any shape covered here is validated against both the serializer helper and the
        /// real reader.
        /// </summary>
        private async Task AssertRoundTripAsync(ChunkDriver driver, byte[] key, byte[] fileData, int chunkSize)
        {
            var stub = CreateStub();
            var srcPath = Path.Combine(testDir, $"rt-{Guid.NewGuid():N}.bftree");
            File.WriteAllBytes(srcPath, fileData);

            var manager = new RangeIndexManager(testDir);
            using var deserializer = new RangeIndexChunkedDeserializer(manager.DeriveTempMigrationPath());

            var buffer = new byte[chunkSize];

            if (driver == ChunkDriver.SerializerHelper)
            {
                using var fs = new FileStream(srcPath, FileMode.Open, FileAccess.Read);
                var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);
                while (!serializer.IsComplete)
                {
                    var len = SerializerMoveNext(serializer, buffer, fs);
                    ClassicAssert.Greater(len, 0, "serializer made no progress");
                    ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
                }
            }
            else
            {
                // The reader owns the FileStream and srcPath (deletes it on Dispose).
                var serializer = new RangeIndexChunkedSerializer(key, stub, fileData.Length);
                var fs = new FileStream(srcPath, FileMode.Open, FileAccess.Read);
                using var reader = new RangeIndexMigrationReader(serializer, fs, srcPath, readBufferSize: chunkSize);
                while (!reader.IsComplete)
                {
                    var len = await reader.ReadNextChunkAsync(buffer);
                    ClassicAssert.Greater(len, 0, "reader made no progress");
                    ClassicAssert.IsTrue(deserializer.ProcessChunk(buffer.AsSpan(0, len).ToArray()));
                }
            }

            ClassicAssert.IsTrue(deserializer.IsComplete);
            ClassicAssert.IsFalse(deserializer.HasError);
            ClassicAssert.AreEqual(key, deserializer.Key.ToArray());
            ClassicAssert.AreEqual(stub, deserializer.Stub.ToArray());
            ClassicAssert.AreEqual(fileData, File.ReadAllBytes(deserializer.TempPath));
        }

        /// <summary>Small file + large buffer: the whole stream fits in one chunk.</summary>
        [Test]
        public Task RoundTrip_SingleChunk([Values] ChunkDriver driver)
            => AssertRoundTripAsync(driver, Encoding.UTF8.GetBytes("mykey"), RandomBytes(1024, 42), RangeIndexManager.DefaultMigrationChunkSize);

        /// <summary>File spanning several default-size chunks.</summary>
        [Test]
        public Task RoundTrip_MultiChunk([Values] ChunkDriver driver)
            => AssertRoundTripAsync(driver, Encoding.UTF8.GetBytes("largekey"), RandomBytes(RangeIndexManager.DefaultMigrationChunkSize * 3 + 1000, 123), RangeIndexManager.DefaultMigrationChunkSize);

        /// <summary>Key larger than the chunk size — forces the key to span multiple chunks.</summary>
        [Test]
        public Task RoundTrip_KeyLargerThanChunk([Values] ChunkDriver driver)
            => AssertRoundTripAsync(driver, RandomBytes(200, 66), RandomBytes(100, 55), 64);

        /// <summary>Small chunk size with a multi-chunk file (chunk &gt;= trailer size).</summary>
        [Test]
        public Task RoundTrip_SmallChunk([Values] ChunkDriver driver)
            => AssertRoundTripAsync(driver, Encoding.UTF8.GetBytes("k"), RandomBytes(500, 77), 64);

        /// <summary>File data sized so the first chunk is exactly filled by header + file bytes.</summary>
        [Test]
        public Task RoundTrip_FileExactlyFillsFirstChunk([Values] ChunkDriver driver)
            // chunk 64; first-chunk overhead = keyHdr(4) + key(1) + fileHdr(8) = 13 → file 51 fills the chunk exactly.
            => AssertRoundTripAsync(driver, Encoding.UTF8.GetBytes("k"), RandomBytes(51, 88), 64);

        /// <summary>
        /// Reader-specific: when the declared file size exceeds the actual file, the reader hits a
        /// zero-byte read mid-stream and throws (it does not silently produce a short stream).
        /// </summary>
        [Test]
        public void Reader_TruncatedFileThrows()
        {
            var srcPath = Path.Combine(testDir, "reader-trunc.bftree");
            File.WriteAllBytes(srcPath, new byte[10]);

            // Claim 1000 file bytes but only 10 exist on disk.
            var serializer = new RangeIndexChunkedSerializer(Encoding.UTF8.GetBytes("k"), CreateStub(), 1000);
            var fs = new FileStream(srcPath, FileMode.Open, FileAccess.Read);
            using var reader = new RangeIndexMigrationReader(serializer, fs, srcPath, readBufferSize: 256);

            var buffer = new byte[256];
            Assert.ThrowsAsync<Exception>(async () =>
            {
                while (!reader.IsComplete)
                    _ = await reader.ReadNextChunkAsync(buffer);
            });
        }

        /// <summary>
        /// Reader-specific: an already-cancelled token aborts the file read with
        /// <see cref="OperationCanceledException"/>.
        /// </summary>
        [Test]
        public void Reader_CancellationThrows()
        {
            var srcPath = Path.Combine(testDir, "reader-cancel.bftree");
            File.WriteAllBytes(srcPath, RandomBytes(4096, 9));

            var serializer = new RangeIndexChunkedSerializer(Encoding.UTF8.GetBytes("k"), CreateStub(), 4096);
            var fs = new FileStream(srcPath, FileMode.Open, FileAccess.Read);
            using var reader = new RangeIndexMigrationReader(serializer, fs, srcPath, readBufferSize: 64);

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var buffer = new byte[64];
            Assert.CatchAsync<OperationCanceledException>(async () => _ = await reader.ReadNextChunkAsync(buffer, cts.Token));
        }

        /// <summary>
        /// Reader-specific: a non-positive chunk size (internal file-read buffer) is rejected
        /// by the constructor.
        /// </summary>
        [Test]
        public void Reader_NonPositiveChunkSizeThrows()
        {
            var srcPath = Path.Combine(testDir, "reader-chunksize.bftree");
            File.WriteAllBytes(srcPath, new byte[8]);

            using var fs = new FileStream(srcPath, FileMode.Open, FileAccess.Read);
            var serializer = new RangeIndexChunkedSerializer(Encoding.UTF8.GetBytes("k"), CreateStub(), 8);

            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new RangeIndexMigrationReader(serializer, fs, srcPath, readBufferSize: 0));
        }

        /// <summary>
        /// Reader-specific: the forward-progress minimum applies to the <c>destination</c> buffer
        /// (where the trailer is framed). A destination smaller than the trailer size is rejected.
        /// </summary>
        [Test]
        public void Reader_DestinationBelowMinimumThrows()
        {
            var srcPath = Path.Combine(testDir, "reader-destmin.bftree");
            File.WriteAllBytes(srcPath, RandomBytes(64, 7));

            var serializer = new RangeIndexChunkedSerializer(Encoding.UTF8.GetBytes("k"), CreateStub(), 64);
            var fs = new FileStream(srcPath, FileMode.Open, FileAccess.Read);
            // A small internal read buffer is fine; the destination is what must be >= trailer size.
            using var reader = new RangeIndexMigrationReader(serializer, fs, srcPath, readBufferSize: 8);

            ClassicAssert.Greater(RangeIndexChunkedSerializer.MinChunkSize, 0);
            var tooSmall = new byte[RangeIndexChunkedSerializer.MinChunkSize - 1];
            Assert.ThrowsAsync<ArgumentException>(async () => _ = await reader.ReadNextChunkAsync(tooSmall));
        }

        /// <summary>
        /// Reader-specific: a destination of exactly <see cref="RangeIndexChunkedSerializer.MinChunkSize"/>
        /// is accepted and a stream round-trips correctly (the trailer just fits in one chunk).
        /// </summary>
        [Test]
        public Task RoundTrip_ExactMinChunkSize([Values] ChunkDriver driver)
            => AssertRoundTripAsync(driver, Encoding.UTF8.GetBytes("k"), RandomBytes(300, 31), RangeIndexChunkedSerializer.MinChunkSize);

        #endregion
    }
}