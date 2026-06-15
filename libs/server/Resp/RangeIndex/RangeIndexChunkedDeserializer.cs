// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Hashing;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Deserializer that reassembles a RangeIndex key from incoming migration records.
    /// Accumulates file data, validates an xxHash64 checksum, recovers the native BfTree,
    /// and publishes the stub to the store.
    ///
    /// <para>Stream format (across one or more chunks):</para>
    /// <list type="bullet">
    /// <item><c>[4-byte keyLen][key bytes][8-byte fileCount][file bytes][8-byte xxHash64][4-byte stubLen][stub]</c></item>
    /// </list>
    /// <para>Key bytes and file bytes may span multiple chunks.
    /// All other elements (keyLen, fileCount, hash, stubLen, stub) must fit entirely within a single chunk.</para>
    ///
    /// <para>State machine:</para>
    /// <list type="bullet">
    /// <item><c>WaitingForKeyHeader</c> → parses 4-byte key length</item>
    /// <item><c>ReceivingKeyData</c> → accumulates key bytes (may span chunks)</item>
    /// <item><c>WaitingForFileHeader</c> → parses 8-byte file size</item>
    /// <item><c>ReceivingFileData</c> → accumulates file bytes to temp file, updates running hash</item>
    /// <item><c>Complete</c> → trailer parsed, checksum valid, ready to read <see cref="Key"/>, <see cref="Stub"/>, and <see cref="TempPath"/></item>
    /// <item><c>Error</c> → irrecoverable (invalid protocol or checksum mismatch)</item>
    /// <item><c>Disposed</c> → resources released, temp file deleted</item>
    /// </list>
    /// </summary>
    public sealed class RangeIndexChunkedDeserializer : IDisposable
    {
        private enum State : byte
        {
            WaitingForKeyHeader,
            ReceivingKeyData,
            WaitingForFileHeader,
            ReceivingFileData,
            WaitingForTrailer,
            Complete,
            Error,
            Disposed,
        }

        private FileStream stream;
        private readonly string tempPath;
        private readonly ILogger logger;
        private readonly XxHash64 hasher;
        private long fileBytesRemaining;
        private byte[] finalizerStub;
        private byte[] finalizerKey;
        private int keyBytesReceived;
        private State state;

        /// <summary>Whether the stream completed successfully.</summary>
        public bool IsComplete => state == State.Complete;

        /// <summary>Whether the stream encountered an irrecoverable error.</summary>
        public bool HasError => state == State.Error;

        /// <summary>The key bytes extracted from the header. Valid only after <see cref="IsComplete"/>.</summary>
        public ReadOnlySpan<byte> Key => finalizerKey;

        /// <summary>The stub bytes extracted from the trailer. Valid only after <see cref="IsComplete"/>.</summary>
        public ReadOnlySpan<byte> Stub => finalizerStub;

        /// <summary>The temporary file path where file data was written.</summary>
        public string TempPath => tempPath;

        public RangeIndexChunkedDeserializer(string tempPath, ILogger logger = null)
        {
            this.tempPath = tempPath;
            this.logger = logger;
            hasher = new XxHash64();
            state = State.WaitingForKeyHeader;
        }

        /// <summary>Process an incoming record payload.</summary>
        /// <returns><c>true</c> if valid; <c>false</c> if corruption or invalid data detected.</returns>
        public bool ProcessChunk(ReadOnlySpan<byte> data)
        {
            switch (state)
            {
                case State.Error:
                case State.Complete:
                case State.Disposed:
                    return false;

                case State.WaitingForKeyHeader:
                    // Empty chunk is a no-op — not an error, just no data to process yet
                    if (data.Length == 0)
                        return true;

                    // Protocol requires the key length header to fit entirely in one chunk
                    if (data.Length < sizeof(int))
                    {
                        logger?.LogError("RangeIndexChunkedDeserializer: split key length header ({Size} bytes)", data.Length);
                        state = State.Error;
                        return false;
                    }

                    var keyLen = BinaryPrimitives.ReadInt32LittleEndian(data);
                    data = data[sizeof(int)..];

                    if (keyLen <= 0)
                    {
                        logger?.LogError("RangeIndexChunkedDeserializer: invalid key length {KeyLen}", keyLen);
                        state = State.Error;
                        return false;
                    }

                    finalizerKey = new byte[keyLen];
                    keyBytesReceived = 0;
                    state = State.ReceivingKeyData;
                    goto case State.ReceivingKeyData;

                case State.ReceivingKeyData:
                    var n = Math.Min(data.Length, finalizerKey.Length - keyBytesReceived);
                    data[..n].CopyTo(finalizerKey.AsSpan(keyBytesReceived));
                    keyBytesReceived += n;
                    data = data[n..];

                    if (keyBytesReceived < finalizerKey.Length)
                        return true;

                    state = State.WaitingForFileHeader;
                    goto case State.WaitingForFileHeader;

                case State.WaitingForFileHeader:
                    // Empty chunk is a no-op — not an error, just no data to process yet
                    if (data.Length == 0)
                        return true;

                    // Protocol requires the file size header to fit entirely in one chunk
                    if (data.Length < sizeof(long))
                    {
                        logger?.LogError("RangeIndexChunkedDeserializer: split file count header ({Size} bytes)", data.Length);
                        state = State.Error;
                        return false;
                    }

                    fileBytesRemaining = BinaryPrimitives.ReadInt64LittleEndian(data);
                    data = data[sizeof(long)..];

                    if (fileBytesRemaining <= 0)
                    {
                        logger?.LogError("RangeIndexChunkedDeserializer: invalid file size {FileSize}", fileBytesRemaining);
                        state = State.Error;
                        return false;
                    }

                    state = State.ReceivingFileData;
                    try
                    {
                        stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "RangeIndexChunkedDeserializer: failed to create temp file {Path}", tempPath);
                        state = State.Error;
                        return false;
                    }

                    goto case State.ReceivingFileData;

                case State.ReceivingFileData:
                    // Empty chunk is a no-op — not an error, just no data to process yet
                    if (data.Length == 0)
                        return true;

                    try
                    {
                        if (fileBytesRemaining > 0)
                            WriteFileBytes(ref data);

                        if (fileBytesRemaining == 0)
                        {
                            CloseStream();
                            state = State.WaitingForTrailer;
                            goto case State.WaitingForTrailer;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "RangeIndexChunkedDeserializer: failed writing migrated file data");
                        state = State.Error;
                        return false;
                    }

                    return true;

                case State.WaitingForTrailer:
                    // Empty chunk is a no-op — not an error, just no data to process yet
                    if (data.Length == 0)
                        return true;

                    return ParseTrailer(data);

                default:
                    return false;
            }
        }

        private bool ParseTrailer(ReadOnlySpan<byte> data)
        {
            // The trailer must arrive in a single chunk — serializer guarantees this.
            // [8-byte xxHash64][4-byte stubLen][stub]
            if (data.Length < sizeof(ulong) + sizeof(int))
            {
                logger?.LogError("RangeIndexChunkedDeserializer: trailer too small ({Size} bytes)", data.Length);
                state = State.Error;
                return false;
            }

            var receivedHash = BinaryPrimitives.ReadUInt64LittleEndian(data);
            data = data[sizeof(ulong)..];
            var stubLen = BinaryPrimitives.ReadInt32LittleEndian(data);
            data = data[sizeof(int)..];
            if (stubLen != RangeIndexManager.IndexSizeBytes)
            {
                logger?.LogError("RangeIndexChunkedDeserializer: invalid stub size {StubLen}, expected {Expected}", stubLen, RangeIndexManager.IndexSizeBytes);
                state = State.Error;
                return false;
            }

            // After the hash + stubLen header, the remaining bytes must be exactly the stub.
            // A well-formed stream delivers the full trailer in one chunk and ends after the stub,
            // so any other length means the payload is malformed (truncated or has trailing bytes).
            if (data.Length != RangeIndexManager.IndexSizeBytes)
            {
                logger?.LogError("RangeIndexChunkedDeserializer: unexpected trailer stub length ({Available} bytes available, {Expected} expected)", data.Length, RangeIndexManager.IndexSizeBytes);
                state = State.Error;
                return false;
            }

            finalizerStub = data.ToArray();

            Span<byte> computedHashBytes = stackalloc byte[sizeof(ulong)];
            hasher.GetHashAndReset(computedHashBytes);
            var computedHash = BinaryPrimitives.ReadUInt64LittleEndian(computedHashBytes);

            if (receivedHash != computedHash)
            {
                logger?.LogError("RangeIndexChunkedDeserializer: checksum mismatch (received {Received:X16}, computed {Computed:X16})", receivedHash, computedHash);
                state = State.Error;
                return false;
            }

            state = State.Complete;
            return true;
        }

        private void WriteFileBytes(ref ReadOnlySpan<byte> data)
        {
            var count = (int)Math.Min(data.Length, fileBytesRemaining);
            var filePart = data[..count];
            stream.Write(filePart);
            hasher.Append(filePart);
            fileBytesRemaining -= count;
            data = data[count..];
        }

        private void CloseStream()
        {
            var s = stream;
            if (s == null) return;

            // Clear the field first so it is always nulled even if Flush/Dispose throws (Dispose
            // can throw because FileStream flushes buffered bytes during disposal). Callers that
            // care about a flush failure (the ReceivingFileData path) catch and go to State.Error.
            stream = null;
            try
            {
                s.Flush();
            }
            finally
            {
                s.Dispose();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (state == State.Disposed) return;
            state = State.Disposed;

            try
            {
                // The temp file is deleted below regardless, so a flush failure during disposal
                // is irrelevant — and Dispose must never throw. CloseStream's finally still
                // disposes the underlying stream even if Flush throws.
                CloseStream();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "RangeIndexChunkedDeserializer: failed to close stream during dispose (ignored)");
            }

            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "RangeIndexChunkedDeserializer: failed to delete temp file {Path} during dispose (ignored)", tempPath);
            }
        }
    }
}