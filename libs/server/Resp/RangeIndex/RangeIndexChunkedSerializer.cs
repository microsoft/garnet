// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace Garnet.server
{
    /// <summary>
    /// Pure state-machine serializer that frames a RangeIndex key, file data, and stub into
    /// a chunked migration stream. Does not perform any I/O — file data is supplied by the
    /// caller via the <c>fileData</c> parameter of <see cref="MoveNext"/>.
    ///
    /// <para>Stream format (across one or more chunks):</para>
    /// <list type="bullet">
    /// <item><c>[4-byte keyLen][key bytes][8-byte fileCount][file bytes][8-byte xxHash64][4-byte stubLen][stub]</c></item>
    /// </list>
    /// <para>Key bytes and file bytes may span multiple chunks.
    /// All other elements (keyLen, fileCount, hash, stubLen, stub) must fit entirely within a single chunk.</para>
    /// <para>The caller provides a destination span on each <see cref="MoveNext"/> call;
    /// the serializer writes at most <c>destination.Length</c> total bytes into it.</para>
    /// </summary>
    public sealed class RangeIndexChunkedSerializer
    {
        private readonly byte[] keyBytes;
        private readonly byte[] stubBytes;
        private readonly long totalFileBytes;
        private readonly XxHash64 hasher;
        private int keyBytesEmitted;
        private long fileBytesEmitted;
        private Phase phase;

        private enum Phase : byte { KeyHeader, KeyData, FileHeader, FileData, Trailer, Done }

        /// <summary>Whether the serializer has emitted all data.</summary>
        public bool IsComplete => phase == Phase.Done;

        /// <summary>Whether the serializer is in the FileData phase and needs file bytes from the caller.</summary>
        public bool NeedsFileData => phase == Phase.FileData && fileBytesEmitted < totalFileBytes;

        /// <summary>Number of file bytes remaining to be emitted.</summary>
        public long FileDataRemaining => totalFileBytes - fileBytesEmitted;

        /// <summary>
        /// Create a serializer for a RangeIndex key.
        /// </summary>
        /// <param name="keyBytes">The key bytes to include in the stream header.</param>
        /// <param name="stubBytes">The stub bytes to include in the stream trailer.</param>
        /// <param name="totalFileBytes">The total number of file data bytes that will be supplied via <see cref="MoveNext"/>.</param>
        public RangeIndexChunkedSerializer(byte[] keyBytes, byte[] stubBytes, long totalFileBytes)
        {
            this.keyBytes = keyBytes;
            this.stubBytes = stubBytes;
            this.totalFileBytes = totalFileBytes;
            hasher = new XxHash64();
            phase = Phase.KeyHeader;
        }

        /// <summary>
        /// Advance to the next chunk. Fills <paramref name="destination"/> with as much payload as fits.
        /// When <see cref="NeedsFileData"/> is true, the caller must supply file data via <paramref name="fileData"/>;
        /// otherwise pass <see cref="ReadOnlySpan{T}.Empty"/>.
        /// Returns the number of bytes written to <paramref name="destination"/>.
        /// </summary>
        /// <param name="destination">Output buffer to write framed data into.</param>
        /// <param name="fileData">File data bytes supplied by the caller (only consumed during the FileData phase).</param>
        /// <param name="fileBytesConsumed">Number of bytes consumed from <paramref name="fileData"/>.</param>
        /// <returns>Number of bytes written to <paramref name="destination"/>.</returns>
        public int MoveNext(Span<byte> destination, ReadOnlySpan<byte> fileData, out int fileBytesConsumed)
        {
            fileBytesConsumed = 0;

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

            // File bytes (may span chunks) — data supplied by caller
            if (phase == Phase.FileData)
            {
                if (fileBytesEmitted < totalFileBytes)
                {
                    var maxCopy = (int)Math.Min(destination.Length, totalFileBytes - fileBytesEmitted);
                    if (maxCopy == 0)
                        return initialLength - destination.Length;

                    // Copy as many file bytes as available and fit in destination
                    var toCopy = Math.Min(maxCopy, fileData.Length);
                    if (toCopy == 0)
                        return initialLength - destination.Length;

                    fileData[..toCopy].CopyTo(destination);
                    hasher.Append(destination[..toCopy]);
                    destination = destination[toCopy..];
                    fileBytesEmitted += toCopy;
                    fileBytesConsumed = toCopy;
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
    }
}
