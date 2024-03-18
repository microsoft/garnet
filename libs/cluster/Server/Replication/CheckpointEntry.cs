// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Garnet.common;

namespace Garnet.cluster
{
    class CheckpointEntry
    {
        public long storeVersion;
        public Guid storeHlogToken;
        public Guid storeIndexToken;
        public long storeCheckpointCoveredAofAddress;
        public string storePrimaryReplId;

        public long objectStoreVersion;
        public Guid objectStoreHlogToken;
        public Guid objectStoreIndexToken;
        public long objectCheckpointCoveredAofAddress;
        public string objectStorePrimaryReplId;

        public SingleWriterMultiReaderLock _lock;
        public CheckpointEntry next;

        public CheckpointEntry()
        {
            storeVersion = -1;
            storeHlogToken = default;
            storeIndexToken = default;
            storeCheckpointCoveredAofAddress = long.MaxValue;
            storePrimaryReplId = null;

            objectStoreVersion = -1;
            objectStoreHlogToken = default;
            objectStoreIndexToken = default;
            objectCheckpointCoveredAofAddress = long.MaxValue;
            objectStorePrimaryReplId = null;

            next = null;
        }

        public long GetMinAofCoveredAddress()
            => Math.Min(storeCheckpointCoveredAofAddress, objectCheckpointCoveredAofAddress);

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
                CheckpointFileType.STORE_HLOG => storeHlogToken.Equals(entry.storeHlogToken),
                CheckpointFileType.STORE_INDEX => storeIndexToken.Equals(entry.storeIndexToken),
                CheckpointFileType.OBJ_STORE_HLOG => objectStoreHlogToken.Equals(entry.objectStoreHlogToken),
                CheckpointFileType.OBJ_STORE_INDEX => objectStoreIndexToken.Equals(entry.objectStoreIndexToken),
                _ => throw new Exception($"Option {fileType} not supported")
            };
        }

        public string GetCheckpointEntryDump()
        {
            string dump = $"\n" +
                $"storeVersion: {storeVersion}\n" +
                $"storeHlogToken: {storeHlogToken}\n" +
                $"storeIndexToken: {storeIndexToken}\n" +
                $"storeCheckpointCoveredAofAddress: {storeCheckpointCoveredAofAddress}\n" +
                $"------------------------------------------------------------------------\n" +
                $"objectStoreVersion:{objectStoreVersion}\n" +
                $"objectStoreHlogToken:{objectStoreHlogToken}\n" +
                $"objectStoreIndexToken:{objectStoreIndexToken}\n" +
                $"objectCheckpointCoveredAofAddress:{objectCheckpointCoveredAofAddress}\n" +
                $"------------------------------------------------------------------------\n" +
                $"activeReaders:{_lock}";
            return dump;
        }

        public byte[] ToByteArray()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms, Encoding.ASCII);
            byte[] byteBuffer = default;

            //Write checkpoint entry data for main store
            writer.Write(storeVersion);
            byteBuffer = storeHlogToken.ToByteArray();
            writer.Write(byteBuffer.Length);
            writer.Write(byteBuffer);
            byteBuffer = storeIndexToken.ToByteArray();
            writer.Write(byteBuffer.Length);
            writer.Write(byteBuffer);
            writer.Write(storeCheckpointCoveredAofAddress);
            writer.Write(storePrimaryReplId == null ? 0 : 1);
            if (storePrimaryReplId != null) writer.Write(storePrimaryReplId);

            //Write checkpoint entry data for object store
            writer.Write(objectStoreVersion);
            byteBuffer = objectStoreHlogToken.ToByteArray();
            writer.Write(byteBuffer.Length);
            writer.Write(byteBuffer);
            byteBuffer = objectStoreIndexToken.ToByteArray();
            writer.Write(byteBuffer.Length);
            writer.Write(byteBuffer);
            writer.Write(objectCheckpointCoveredAofAddress);
            writer.Write(objectStorePrimaryReplId == null ? 0 : 1);
            if (objectStorePrimaryReplId != null) writer.Write(objectStorePrimaryReplId);

            byte[] byteArray = ms.ToArray();
            writer.Dispose();
            ms.Dispose();
            return byteArray;
        }

        public static CheckpointEntry FromByteArray(byte[] serialized)
        {
            var ms = new MemoryStream(serialized);
            var reader = new BinaryReader(ms);
            var cEntry = new CheckpointEntry
            {
                storeVersion = reader.ReadInt64(),
                storeHlogToken = new Guid(reader.ReadBytes(reader.ReadInt32())),
                storeIndexToken = new Guid(reader.ReadBytes(reader.ReadInt32())),
                storeCheckpointCoveredAofAddress = reader.ReadInt64(),
                storePrimaryReplId = reader.ReadInt32() > 0 ? reader.ReadString() : default,

                objectStoreVersion = reader.ReadInt64(),
                objectStoreHlogToken = new Guid(reader.ReadBytes(reader.ReadInt32())),
                objectStoreIndexToken = new Guid(reader.ReadBytes(reader.ReadInt32())),
                objectCheckpointCoveredAofAddress = reader.ReadInt64(),
                objectStorePrimaryReplId = reader.ReadInt32() > 0 ? reader.ReadString() : default
            };

            reader.Dispose();
            ms.Dispose();
            return cEntry;
        }
    }
}