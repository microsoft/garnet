// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public enum ManagerType : byte
    {
        // IMPORTANT: Any changes to the values of this enum should be reflected in its parser (SessionParseStateExtensions.TryGetManagerType)

        /// <summary>
        /// MigrationManager Buffer Pool
        /// </summary>
        MigrationManager,
        /// <summary>
        /// ReplicationManager BufferPool
        /// </summary>
        ReplicationManager,
        /// <summary>
        /// ServerListener BufferPool
        /// </summary>
        ServerListener,
    }

    /// <summary>
    /// Extension methods for <see cref="ManagerType"/>.
    /// </summary>
    internal static class ManagerTypeExtensions
    {
        public static ReadOnlySpan<byte> ToReadOnlySpan(this ManagerType managerType)
        {
            return managerType switch
            {
                ManagerType.MigrationManager => "GC completed for MigrationManager"u8,
                ManagerType.ReplicationManager => "GC completed for ReplicationManager"u8,
                ManagerType.ServerListener => "GC completed for ServerListener"u8,
                _ => throw new GarnetException()
            };
        }
    }

    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool NetworkPurgeBP()
        {
            // Expecting exactly 1 argument
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PURGEBP));
            }

            if (!parseState.TryGetManagerType(0, out var managerType))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            try
            {
                var success = true;
                switch (managerType)
                {
                    case ManagerType.MigrationManager:
                    case ManagerType.ReplicationManager:
                        success = ClusterPurgeBufferPool(managerType);
                        break;
                    case ManagerType.ServerListener:
                        foreach (var server in storeWrapper.Servers)
                            ((GarnetServerTcp)server).Purge();
                        break;
                    default:
                        success = false;
                        while (!RespWriteUtils.TryWriteError($"ERR Could not purge {managerType}.", ref dcurr, dend))
                            SendAndReset();
                        break;
                }

                if (success)
                {
                    GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true);
                    while (!RespWriteUtils.TryWriteSimpleString(managerType.ToReadOnlySpan(), ref dcurr, dend))
                        SendAndReset();
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "PURGEBP {type}:{managerType}", managerType, managerType.ToString());
                while (!RespWriteUtils.TryWriteError($"ERR {ex.Message}", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            bool ClusterPurgeBufferPool(ManagerType managerType)
            {
                if (clusterSession == null)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED, ref dcurr, dend))
                        SendAndReset();
                    return false;
                }
                storeWrapper.clusterProvider.PurgeBufferPool(managerType);
                return true;
            }

            return true;
        }
    }
}