// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Hashing;

namespace Garnet.server
{
    /// <summary>
    /// Serializer that produces a stream of migration records for a single RangeIndex key.
    ///
    /// <para>Stream format (across one or more chunks):</para>
    /// <list type="bullet">
    /// <item><c>[4-byte keyLen][key bytes][8-byte fileCount][file bytes][8-byte xxHash64][4-byte stubLen][stub]</c></item>
    /// </list>
    /// <para>Key bytes and file bytes may span multiple chunks.
    /// All other elements (keyLen, fileCount, hash, stubLen, stub) must fit entirely within a single chunk.</para>
    /// <para>The caller provides a destination span on each <see cref="MoveNext"/> call;
    /// the serializer writes at most <c>chunkSize</c> total bytes into it, enabling zero-copy framing.</para>
    /// </summary>
    public sealed class RangeIndexChunkedSerializer : IDisposable
    {
        private FileStream fileStream;
        private readonly byte[] keyBytes;
        private readonly byte[] stubBytes;
        private readonly long totalFileBytes;
        private readonly XxHash64 hasher;
        private int keyBytesEmitted;
        private long fileBytesEmitted;
        private Phase phase;
        private bool disposed;

        private enum Phase : byte { KeyHeader, KeyData, FileHeader, FileData, Trailer, Done }

        /// <summary>Whether the serializer has emitted all data.</summary>
        public bool IsComplete => phase == Phase.Done;

        internal RangeIndexChunkedSerializer(FileStream fileStream, byte[] keyBytes, byte[] stubBytes, long totalFileBytes)
        {
            this.fileStream = fileStream;
            this.keyBytes = keyBytes;
            this.stubBytes = stubBytes;
            this.totalFileBytes = totalFileBytes;
            hasher = new XxHash64();
            phase = Phase.KeyHeader;
        }

        /// <summary>
        /// Advance to the next chunk. Fills <paramref name="destination"/> with as much payload as fits.
        /// Returns the number of bytes written, or 0 if the stream is complete.
        /// </summary>
        public int MoveNext(Span<byte> destination)
        {
            if (phase == Phase.Done)
                throw new InvalidOperationException("Serializer has already completed");

            var initialLength = destination.Length;

            // Key length header (must fit entirely in the current chunk)
            if (phase == Phase.KeyHeader)
            {
                if (destination.Length < sizeof(int))
                    return initialLength - destination.Length;

                BinaryPrimitives.WriteInt32LittleEndian(destination, keyBytes.Length);
                destination = destination[sizeof(int)..];
                phase = Phase.KeyData;
            }

            // Key bytes (may span chunks)
            if (phase == Phase.KeyData)
            {
                var n = Math.Min(keyBytes.Length - keyBytesEmitted, destination.Length);
                keyBytes.AsSpan(keyBytesEmitted, n).CopyTo(destination);
                destination = destination[n..];
                keyBytesEmitted += n;

                if (keyBytesEmitted < keyBytes.Length)
                    return initialLength - destination.Length;

                phase = Phase.FileHeader;
            }

            // File byte count header (must fit entirely in the current chunk)
            if (phase == Phase.FileHeader)
            {
                if (destination.Length < sizeof(long))
                    return initialLength - destination.Length;

                BinaryPrimitives.WriteInt64LittleEndian(destination, totalFileBytes);
                destination = destination[sizeof(long)..];
                phase = Phase.FileData;
            }

            // File bytes (may span chunks)
            if (phase == Phase.FileData)
            {
                if (fileBytesEmitted < totalFileBytes)
                {
                    var maxRead = (int)Math.Min(destination.Length, totalFileBytes - fileBytesEmitted);
                    if (maxRead == 0)
                        return initialLength - destination.Length;

                    var bytesRead = fileStream.Read(destination[..maxRead]);

                    if (bytesRead == 0)
                        throw new EndOfStreamException($"RangeIndex file truncated: expected {totalFileBytes} bytes, got {fileBytesEmitted}");

                    hasher.Append(destination[..bytesRead]);
                    destination = destination[bytesRead..];
                    fileBytesEmitted += bytesRead;
                }

                if (fileBytesEmitted >= totalFileBytes)
                    phase = Phase.Trailer;
            }

            // Trailer (must fit entirely in the current chunk)
            if (phase == Phase.Trailer)
            {
                if (destination.Length < TrailerSize)
                    return initialLength - destination.Length;

                WriteTrailer(destination);
                destination = destination[TrailerSize..];
                phase = Phase.Done;
            }

            return initialLength - destination.Length;
        }

        private int TrailerSize => sizeof(ulong) + sizeof(int) + stubBytes.Length;

        private void WriteTrailer(Span<byte> target)
        {
            // [8-byte xxHash64][4-byte stubLen][stub]
            hasher.GetHashAndReset(target);
            target = target[sizeof(ulong)..];
            BinaryPrimitives.WriteInt32LittleEndian(target, stubBytes.Length);
            target = target[sizeof(int)..];
            stubBytes.CopyTo(target);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            fileStream?.Dispose();
            fileStream = null;
        }
    }
}
