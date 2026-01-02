// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    static class CheckpointEntryExtensions
    {
        public static void LogCheckpointEntry(this ILogger logger, LogLevel logLevel, string msg, CheckpointEntry entry)
        {
            logger?.Log(logLevel, "\n" +
                "[{msg}]\n" +
                "readers:{readers}\n" +
                "storeVersion: {storeVersion}\n" +
                "storeHlogToken: {storeHlogToken}\n" +
                "storeIndexToken: {storeIndexToken}\n" +
                "storeCheckpointCoveredAofAddress: {storeCheckpointCoveredAofAddress}\n" +
                "------------------------------------------------------------------------\n",
                msg,
                entry._lock,
                entry.metadata.storeVersion,
                entry.metadata.storeHlogToken,
                entry.metadata.storeIndexToken,
                entry.metadata.storeCheckpointCoveredAofAddress);
        }
    }

    sealed class CheckpointEntry
    {
        public CheckpointMetadata metadata;
        public SingleWriterMultiReaderLock _lock;
        public CheckpointEntry next;

        public CheckpointEntry()
        {
            metadata = new(0);
            next = null;
            _lock = new();
        }

        public AofAddress GetMinAofCoveredAddress()
        {
            var minCoveredAofAddress = metadata.storeCheckpointCoveredAofAddress;
            minCoveredAofAddress.MaxExchange(ReplicationManager.kFirstValidAofAddress);
            return minCoveredAofAddress;
        }

        /// <summary>
        /// Indicate addition of new reader by trying to increment reader counter
        /// </summary>
        /// <returns>(true) on success, (false) otherwise</returns>
        public bool TryAddReader()
            => _lock.TryReadLock();

        /// <summary>
        /// Indicate removal of a reader by decrementing reader counter
        /// </summary>
        public void RemoveReader()
            => _lock.ReadUnlock();

        /// <summary>
        /// Suspend addition of new readers by setting the reader counter to int.MinValue
        /// </summary>
        /// <returns>(true) if operation succeeded, (false) otherwise</returns>
        public bool TrySuspendReaders()
            => _lock.IsWriteLocked || _lock.TryWriteLock();

        /// <summary>
        /// Compare tokens for specified CheckpointFileType
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="fileType"></param>
        /// <returns>(true) if token is shared between entries, (false) otherwise</returns>
        /// <exception cref="Exception"></exception>
        public bool ContainsSharedToken(CheckpointEntry entry, CheckpointFileType fileType)
        {
            return fileType switch
            {
                CheckpointFileType.STORE_HLOG => metadata.storeHlogToken.Equals(entry.metadata.storeHlogToken),
                CheckpointFileType.STORE_INDEX => metadata.storeIndexToken.Equals(entry.metadata.storeIndexToken),
                _ => throw new Exception($"Option {fileType} not supported")
            };
        }

        /// <summary>
        /// Serialize CheckpointEntry
        /// </summary>
        /// <returns></returns>
        public byte[] ToByteArray()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms, Encoding.ASCII);
            byte[] byteBuffer;

            // Write checkpoint entry data for main store
            writer.Write(metadata.storeVersion);
            byteBuffer = metadata.storeHlogToken.ToByteArray();
            writer.Write(byteBuffer.Length);
            writer.Write(byteBuffer);
            byteBuffer = metadata.storeIndexToken.ToByteArray();
            writer.Write(byteBuffer.Length);
            writer.Write(byteBuffer);
            metadata.storeCheckpointCoveredAofAddress.Serialize(writer);
            writer.Write(metadata.storePrimaryReplId == null ? 0 : 1);
            if (metadata.storePrimaryReplId != null) writer.Write(metadata.storePrimaryReplId);

            var byteArray = ms.ToArray();
            writer.Dispose();
            ms.Dispose();
            return byteArray;
        }

        /// <summary>
        /// Deserialize CheckpointEntry
        /// </summary>
        /// <param name="serialized"></param>
        /// <returns></returns>
        public static CheckpointEntry FromByteArray(byte[] serialized)
        {
            if (serialized.Length == 0) return null;
            using var ms = new MemoryStream(serialized);
            using var reader = new BinaryReader(ms);
            var cEntry = new CheckpointEntry
            {
                metadata = new(0)
                {
                    storeVersion = reader.ReadInt64(),
                    storeHlogToken = new Guid(reader.ReadBytes(reader.ReadInt32())),
                    storeIndexToken = new Guid(reader.ReadBytes(reader.ReadInt32())),
                    storeCheckpointCoveredAofAddress = AofAddress.Deserialize(reader),
                    storePrimaryReplId = reader.ReadInt32() > 0 ? reader.ReadString() : default
                }
            };

            return cEntry;
        }

        public override string ToString() => $"{metadata},readers={_lock}";
    }
}