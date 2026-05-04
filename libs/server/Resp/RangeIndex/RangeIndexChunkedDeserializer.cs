// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Pure state-machine deserializer that parses incoming migration chunks for a RangeIndex key.
    /// Does not perform any I/O — file data bytes are identified by offset and length in the
    /// input buffer, allowing the caller to write them to disk asynchronously.
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
    /// <item><c>ReceivingFileData</c> → identifies file byte ranges, updates running hash</item>
    /// <item><c>Complete</c> → trailer parsed, checksum valid, ready for <see cref="Publish"/></item>
    /// <item><c>Error</c> → irrecoverable (invalid protocol or checksum mismatch)</item>
    /// </list>
    /// </summary>
    public sealed class RangeIndexChunkedDeserializer
    {
        private enum State : byte
        {
            WaitingForKeyHeader,
            ReceivingKeyData,
            WaitingForFileHeader,
            ReceivingFileData,
            Complete,
            Error,
        }

        private readonly RangeIndexManager manager;
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

        /// <summary>Total number of file bytes declared in the stream header.</summary>
        public long TotalFileBytes { get; private set; }

        public RangeIndexChunkedDeserializer(RangeIndexManager manager)
        {
            this.manager = manager;
            hasher = new XxHash64();
            state = State.WaitingForKeyHeader;
        }

        /// <summary>
        /// Process an incoming record payload. File data bytes are not written by this method;
        /// instead, their location within <paramref name="data"/> is returned via
        /// <paramref name="fileDataOffset"/> and <paramref name="fileDataLength"/>.
        /// The caller is responsible for writing those bytes to disk.
        /// </summary>
        /// <param name="data">The incoming chunk data.</param>
        /// <param name="fileDataOffset">Offset within <paramref name="data"/> where file bytes start (0 if none).</param>
        /// <param name="fileDataLength">Number of file data bytes to write (0 if none in this chunk).</param>
        /// <returns><c>true</c> if valid; <c>false</c> if corruption or invalid data detected.</returns>
        public bool ProcessChunk(ReadOnlySpan<byte> data, out int fileDataOffset, out int fileDataLength)
        {
            fileDataOffset = 0;
            fileDataLength = 0;
            var originalLength = data.Length;

            switch (state)
            {
                case State.Error:
                case State.Complete:
                    return false;

                case State.WaitingForKeyHeader:
                    if (data.Length == 0)
                        return true;

                    if (data.Length < sizeof(int))
                    {
                        state = State.Error;
                        return false;
                    }

                    var keyLen = BinaryPrimitives.ReadInt32LittleEndian(data);
                    data = data[sizeof(int)..];

                    if (keyLen < 0)
                    {
                        state = State.Error;
                        return false;
                    }

                    finalizerKey = new byte[keyLen];
                    keyBytesReceived = 0;
                    state = State.ReceivingKeyData;
                    goto case State.ReceivingKeyData;

                case State.ReceivingKeyData:
                    if (keyBytesReceived < finalizerKey.Length)
                    {
                        var n = Math.Min(data.Length, finalizerKey.Length - keyBytesReceived);
                        data[..n].CopyTo(finalizerKey.AsSpan(keyBytesReceived));
                        keyBytesReceived += n;
                        data = data[n..];
                    }

                    if (keyBytesReceived < finalizerKey.Length)
                        return true;

                    state = State.WaitingForFileHeader;
                    if (data.Length == 0)
                        return true;

                    goto case State.WaitingForFileHeader;

                case State.WaitingForFileHeader:
                    if (data.Length == 0)
                        return true;

                    if (data.Length < sizeof(long))
                    {
                        manager.Logger?.LogError("RangeIndexChunkedDeserializer: split file count header ({Size} bytes)", data.Length);
                        state = State.Error;
                        return false;
                    }

                    fileBytesRemaining = BinaryPrimitives.ReadInt64LittleEndian(data);
                    TotalFileBytes = fileBytesRemaining;
                    data = data[sizeof(long)..];

                    if (fileBytesRemaining < 0)
                    {
                        state = State.Error;
                        return false;
                    }

                    state = State.ReceivingFileData;
                    goto case State.ReceivingFileData;

                case State.ReceivingFileData:
                    if (fileBytesRemaining > 0)
                    {
                        var count = (int)Math.Min(data.Length, fileBytesRemaining);
                        hasher.Append(data[..count]);
                        fileBytesRemaining -= count;

                        fileDataOffset = originalLength - data.Length;
                        fileDataLength = count;
                        data = data[count..];
                    }

                    if (fileBytesRemaining == 0 && data.Length > 0)
                        return ParseTrailer(data);

                    return true;

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
                manager.Logger?.LogError("RangeIndexChunkedDeserializer: trailer too small ({Size} bytes)", data.Length);
                state = State.Error;
                return false;
            }

            var receivedHash = BinaryPrimitives.ReadUInt64LittleEndian(data);
            data = data[sizeof(ulong)..];
            var stubLen = BinaryPrimitives.ReadInt32LittleEndian(data);
            data = data[sizeof(int)..];
            if (stubLen != RangeIndexManager.IndexSizeBytes)
            {
                manager.Logger?.LogError("RangeIndexChunkedDeserializer: invalid stub size {StubLen}, expected {Expected}", stubLen, RangeIndexManager.IndexSizeBytes);
                state = State.Error;
                return false;
            }

            finalizerStub = data[..RangeIndexManager.IndexSizeBytes].ToArray();

            Span<byte> computedHashBytes = stackalloc byte[sizeof(ulong)];
            hasher.GetHashAndReset(computedHashBytes);
            var computedHash = BinaryPrimitives.ReadUInt64LittleEndian(computedHashBytes);

            if (receivedHash != computedHash)
            {
                manager.Logger?.LogError("RangeIndexChunkedDeserializer: checksum mismatch (received {Received:X16}, computed {Computed:X16})", receivedHash, computedHash);
                state = State.Error;
                return false;
            }

            state = State.Complete;
            return true;
        }

        /// <summary>
        /// Publish: move the temp file to the key-hashed working path, recover the native BfTree,
        /// and insert the stub into the store via RICREATE RMW.
        /// </summary>
        /// <param name="ctx">The string basic context for store operations.</param>
        /// <param name="tempPath">The temporary file path where the caller wrote the file data.</param>
        public unsafe bool Publish(ref StringBasicContext ctx, string tempPath)
        {
            if (state != State.Complete)
            {
                manager.Logger?.LogError("RangeIndexChunkedDeserializer.Publish: cannot finalize in state {State}", state);
                return false;
            }

            ReadOnlySpan<byte> keyBytes = finalizerKey;

            try
            {
                var workingPath = manager.DeriveWorkingPath(keyBytes);
                Directory.CreateDirectory(Path.GetDirectoryName(workingPath)!);

                // If a data file already exists (e.g., from a previous migration of the same key
                // that was later deleted), remove it so the new snapshot can take its place.
                if (File.Exists(workingPath))
                    File.Delete(workingPath);

                File.Move(tempPath, workingPath);

                ref readonly var srcStub = ref RangeIndexManager.ReadIndex(finalizerStub);

                var bfTree = BfTreeService.RecoverFromSnapshot(
                    workingPath,
                    (StorageBackendType)srcStub.StorageBackend,
                    srcStub.CacheSize,
                    srcStub.MinRecordSize,
                    srcStub.MaxRecordSize,
                    srcStub.MaxKeyLen,
                    srcStub.LeafPageSize);

                Span<byte> newStubBytes = stackalloc byte[RangeIndexManager.IndexSizeBytes];
                finalizerStub.CopyTo(newStubBytes);
                ref var newStub = ref Unsafe.As<byte, RangeIndexManager.RangeIndexStub>(ref MemoryMarshal.GetReference(newStubBytes));
                newStub.TreeHandle = bfTree.NativePtr;
                newStub.Flags = 0;
                newStub.SerializationPhase = 0;

                var parseState = new SessionParseState();
                fixed (byte* stubPtr = newStubBytes)
                {
                    var stubSlice = PinnedSpanByte.FromPinnedPointer(stubPtr, RangeIndexManager.IndexSizeBytes);
                    parseState.InitializeWithArgument(stubSlice);

                    var input = new StringInput(RespCommand.RICREATE, ref parseState);
                    var output = new StringOutput();
                    var pinnedKey = PinnedSpanByte.FromPinnedSpan(keyBytes);
                    var status = ctx.RMW((FixedSpanByteKey)pinnedKey, ref input, ref output);
                    if (status.IsPending)
                        StorageSession.CompletePendingForSession(ref status, ref output, ref ctx);

                    if (status.Record.Created || status.Record.InPlaceUpdated || status.Record.CopyUpdated)
                    {
                        var keyHash = ctx.GetKeyHash((FixedSpanByteKey)pinnedKey);
                        manager.RegisterIndex(bfTree, keyHash, keyBytes);
                    }
                    else
                    {
                        bfTree.Dispose();
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                manager.Logger?.LogError(ex, "RangeIndexChunkedDeserializer.Publish: failed to recover BfTree");
                return false;
            }
        }
    }
}
