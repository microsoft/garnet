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
        public long storeCheckpointCoveredAofAddress;
        public string storePrimaryReplId;

        public CheckpointMetadata()
        {
            storeVersion = -1;
            storeHlogToken = default;
            storeIndexToken = default;
            storeCheckpointCoveredAofAddress = long.MaxValue;
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