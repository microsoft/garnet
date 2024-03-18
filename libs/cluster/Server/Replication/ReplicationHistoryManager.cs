// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class ReplicationHistory
    {
        public string primary_replid;
        public string primary_replid2;
        public long replicationOffset;
        public long replicationOffset2;

        public ReplicationHistory()
        {
            primary_replid = Generator.CreateHexId();
            primary_replid2 = String.Empty;
            replicationOffset = 0;
            replicationOffset2 = int.MaxValue;
        }

        public ReplicationHistory Copy()
        {
            return new ReplicationHistory()
            {
                primary_replid = primary_replid,
                primary_replid2 = primary_replid2,
                replicationOffset = replicationOffset,
                replicationOffset2 = replicationOffset2
            };
        }

        public byte[] ToByteArray()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms, Encoding.ASCII);

            writer.Write(primary_replid);
            writer.Write(primary_replid2);
            writer.Write(replicationOffset);
            writer.Write(replicationOffset2);

            byte[] byteArray = ms.ToArray();
            writer.Dispose();
            ms.Dispose();
            return byteArray;
        }

        public static ReplicationHistory FromByteArray(byte[] data)
        {
            var ms = new MemoryStream(data);
            var reader = new BinaryReader(ms);

            var primary_replid = reader.ReadString();
            var primary_replid2 = reader.ReadString();
            var replicationOffset = reader.ReadInt64();
            var replicationOffset2 = reader.ReadInt64();

            reader.Dispose();
            ms.Dispose();
            return new ReplicationHistory()
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

        public ReplicationHistory FailoverUpdate(long replicationOffset2)
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
        readonly SectorAlignedBufferPool pool;

        public void InitializeReplicationHistory()
        {
            currentReplicationConfig = new ReplicationHistory();
            FlushConfig();
        }

        public void RecoverReplicationHistory()
        {
            byte[] replConfig = ClusterUtils.ReadDevice(replicationConfigDevice, pool, logger);
            currentReplicationConfig = ReplicationHistory.FromByteArray(replConfig);
            //TODO: handle scenario where replica crashed before became a primary and it has two replication ids
            //var current = storeWrapper.clusterManager.CurrentConfig;
            //if(current.GetLocalNodeRole() == NodeRole.REPLICA && !primary_replid2.Equals(Generator.DefaultHexId()))
            //{

            //}
        }

        public void TryUpdateMyPrimaryReplId(string primary_replid)
        {
            while (true)
            {
                var current = currentReplicationConfig;
                var newConfig = current.UpdateReplicationId(primary_replid);
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
                storeWrapper.appendOnlyFile?.Commit();
                storeWrapper.appendOnlyFile?.WaitForCommit();
            }
            while (true)
            {
                var replicationOffset2 = storeWrapper.appendOnlyFile.CommittedUntilAddress;
                var current = currentReplicationConfig;
                var newConfig = current.FailoverUpdate(replicationOffset2);
                if (Interlocked.CompareExchange(ref currentReplicationConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
        }

        private void FlushConfig()
        {
            lock (this)
            {
                logger?.LogTrace("Start FlushConfig {path}", replicationConfigDevice.FileName);
                ClusterUtils.WriteInto(replicationConfigDevice, pool, 0, currentReplicationConfig.ToByteArray(), logger: logger);
                logger?.LogTrace("End FlushConfig {path}", replicationConfigDevice.FileName);
            }
        }
    }
}