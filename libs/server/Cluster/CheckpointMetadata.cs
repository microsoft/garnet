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

        public CheckpointMetadata(int sublogCount)
        {
            storeVersion = -1;
            storeHlogToken = default;
            storeIndexToken = default;
            storeCheckpointCoveredAofAddress = AofAddress.Create(sublogCount, 0);
            storePrimaryReplId = null;
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
                $"storePrimaryReplId={storePrimaryReplId ?? "(empty)"}";
        }
    }
}