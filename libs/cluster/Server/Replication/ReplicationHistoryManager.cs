// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class ReplicationHistory
    {
        public string PrimaryReplId => primary_replid;
        string primary_replid;
        public string PrimaryReplId2 => primary_replid2;
        public string primary_replid2;
        AofAddress replicationOffset;
        public AofAddress replicationOffset2;

        public ReplicationHistory(int aofPhysicalSublogCount)
        {
            primary_replid = Generator.CreateHexId();
            primary_replid2 = String.Empty;
            replicationOffset = AofAddress.Create(aofPhysicalSublogCount, 0);
            replicationOffset2 = AofAddress.Create(aofPhysicalSublogCount, long.MaxValue);
        }

        public ReplicationHistory Copy()
        {
            return new ReplicationHistory(replicationOffset.Length)
            {
                primary_replid = primary_replid,
                primary_replid2 = primary_replid2,
                replicationOffset = replicationOffset,
                replicationOffset2 = replicationOffset2
            };
        }

        public byte[] ToByteArray()
        {
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms, Encoding.ASCII);

            writer.Write(primary_replid);
            writer.Write(primary_replid2);
            replicationOffset.Serialize(writer);
            replicationOffset2.Serialize(writer);

            var byteArray = ms.ToArray();
            return byteArray;
        }

        public static ReplicationHistory FromByteArray(byte[] data)
        {
            using var ms = new MemoryStream(data);
            using var reader = new BinaryReader(ms);

            var primary_replid = reader.ReadString();
            var primary_replid2 = reader.ReadString();
            var replicationOffset = AofAddress.Deserialize(reader);
            var replicationOffset2 = AofAddress.Deserialize(reader);

            return new ReplicationHistory(replicationOffset.Length)
            {
                primary_replid = primary_replid,
                primary_replid2 = primary_replid2,
                replicationOffset = replicationOffset,
                replicationOffset2 = replicationOffset2
            };
        }

        public ReplicationHistory UpdateReplicationId(string primary_replid)
        {
            var newConfig = this.Copy();
            newConfig.primary_replid = primary_replid;
            return newConfig;
        }

        public ReplicationHistory FailoverUpdate(AofAddress replicationOffset2)
        {
            var newConfig = this.Copy();
            newConfig.primary_replid2 = primary_replid;
            newConfig.primary_replid = Generator.CreateHexId();
            newConfig.replicationOffset2 = replicationOffset2;
            return newConfig;
        }
    }

    internal sealed partial class ReplicationManager : IDisposable
    {
        ReplicationHistory currentReplicationConfig;
        readonly IDevice replicationConfigDevice;
        readonly SectorAlignedBufferPool replicationConfigDevicePool;

        private void InitializeReplicationHistory(int aofPhysicalSublogCount)
        {
            currentReplicationConfig = new ReplicationHistory(aofPhysicalSublogCount);
            FlushConfig();
        }

        private void RecoverReplicationHistory()
        {
            var replConfig = ClusterUtils.ReadDevice(replicationConfigDevice, replicationConfigDevicePool, logger);
            currentReplicationConfig = ReplicationHistory.FromByteArray(replConfig);
            //TODO: handle scenario where replica crashed before became a primary and it has two replication ids
            //var current = storeWrapper.clusterManager.CurrentConfig;
            //if(current.GetLocalNodeRole() == NodeRole.REPLICA && !primary_replid2.Equals(Generator.DefaultHexId()))
            //{

            //}
        }

        private void TryUpdateMyPrimaryReplId(string primaryReplicationId)
        {
            while (true)
            {
                var current = currentReplicationConfig;
                var newConfig = current.UpdateReplicationId(primaryReplicationId);
                if (Interlocked.CompareExchange(ref currentReplicationConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
        }

        /// <summary>
        /// Called during failover so replica can generate new replication id and keep track of valid replicationOffset before switching over.
        /// </summary>
        public void TryUpdateForFailover()
        {
            if (!clusterProvider.serverOptions.EnableFastCommit)
            {
                storeWrapper.appendOnlyFile?.Log.Commit();
                storeWrapper.appendOnlyFile?.Log.WaitForCommit();
            }
            while (true)
            {
                var replicationOffset2 = storeWrapper.appendOnlyFile.Log.CommittedUntilAddress;
                var current = currentReplicationConfig;
                var newConfig = current.FailoverUpdate(replicationOffset2);
                if (Interlocked.CompareExchange(ref currentReplicationConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            SetPrimaryReplicationId();
        }

        private void FlushConfig()
        {
            lock (this)
            {
                logger?.LogTrace("Flushing replication history {path}", replicationConfigDevice.FileName);
                ClusterUtils.WriteInto(replicationConfigDevice, replicationConfigDevicePool, 0, currentReplicationConfig.ToByteArray(), logger: logger);
                logger?.LogTrace("Replication history flush completed {path}", replicationConfigDevice.FileName);
            }
        }
    }
}