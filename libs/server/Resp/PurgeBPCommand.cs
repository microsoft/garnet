// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public enum ManagerType : byte
    {
        /// <summary>
        /// MigrationManager Buffer Pool
        /// </summary>
        MM,
        /// <summary>
        /// ReplicationManager BufferPool
        /// </summary>
        RM,
        /// <summary>
        /// ServerSocket BufferPool
        /// </summary>
        SS,
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
                ManagerType.MM => "GC completed for MigrationManager"u8,
                ManagerType.RM => "GC completed for ReplicationManager"u8,
                ManagerType.SS => "GC completed for ServerSocket"u8,
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

            if (!parseState.TryGetEnum<ManagerType>(0, ignoreCase: true, out var managerType) || !Enum.IsDefined(managerType))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            try
            {
                var success = true;
                switch (managerType)
                {
                    case ManagerType.MM:
                    case ManagerType.RM:
                        success = ClusterPurgeBufferPool(managerType);
                        break;
                    case ManagerType.SS:
                        storeWrapper.GetTcpServer().Purge();
                        break;
                    default:
                        success = false;
                        while (!RespWriteUtils.WriteError($"ERR Could not purge {managerType}.", ref dcurr, dend))
                            SendAndReset();
                        break;
                }

                if (success)
                {
                    GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true);
                    while (!RespWriteUtils.WriteSimpleString(managerType.ToReadOnlySpan(), ref dcurr, dend))
                        SendAndReset();
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "PURGEBP {type}:{managerType}", managerType, managerType.ToString());
                while (!RespWriteUtils.WriteError($"ERR {ex.Message}", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            bool ClusterPurgeBufferPool(ManagerType managerType)
            {
                if (clusterSession == null)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED, ref dcurr, dend))
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