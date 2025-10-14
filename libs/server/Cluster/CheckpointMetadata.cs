// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    public sealed class CheckpointMetadata
    {
        public long storeVersion;
        public Guid storeHlogToken;
        public Guid storeIndexToken;
        public AofAddress storeCheckpointCoveredAofAddress;
        public string storePrimaryReplId;

        public long objectStoreVersion;
        public Guid objectStoreHlogToken;
        public Guid objectStoreIndexToken;
        public AofAddress objectCheckpointCoveredAofAddress;
        public string objectStorePrimaryReplId;

        public CheckpointMetadata(int sublogCount)
        {
            storeVersion = -1;
            storeHlogToken = default;
            storeIndexToken = default;
            storeCheckpointCoveredAofAddress = AofAddress.Create(sublogCount, 0);
            storePrimaryReplId = null;

            objectStoreVersion = -1;
            objectStoreHlogToken = default;
            objectStoreIndexToken = default;
            objectCheckpointCoveredAofAddress = AofAddress.Create(sublogCount, long.MaxValue);
            objectStorePrimaryReplId = null;
        }

        /// <summary>
        /// ToString implementation
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return
                $"storeVersion={storeVersion}," +
                $"storeHlogToken={storeHlogToken}," +
                $"storeIndexToken={storeIndexToken}," +
                $"storeCheckpointCoveredAofAddress={storeCheckpointCoveredAofAddress}," +
                $"storePrimaryReplId={storePrimaryReplId ?? "(empty)"}," +
                $"objectStoreVersion={objectStoreVersion}," +
                $"objectStoreHlogToken={objectStoreHlogToken}," +
                $"objectStoreIndexToken={objectStoreIndexToken}," +
                $"objectCheckpointCoveredAofAddress={objectCheckpointCoveredAofAddress}," +
                $"objectStorePrimaryReplId={objectStorePrimaryReplId ?? "(empty)"}";
        }
    }
}