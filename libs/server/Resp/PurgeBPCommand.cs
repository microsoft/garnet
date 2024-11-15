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

    internal static class ManagerTypeUtils
    {
        /// <summary>
        /// Parse manager type from span
        /// </summary>
        /// <param name="input">ReadOnlySpan input to parse</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryParseManagerType(ReadOnlySpan<byte> input, out ManagerType value)
        {
            value = default;

            if (input.EqualsUpperCaseSpanIgnoringCase("MIGRATIONMANAGER"u8))
                value = ManagerType.MigrationManager;
            else if (input.EqualsUpperCaseSpanIgnoringCase("REPLICATIONMANAGER"u8))
                value = ManagerType.ReplicationManager;
            else if (input.EqualsUpperCaseSpanIgnoringCase("SERVERLISTENER"u8))
                value = ManagerType.ServerListener;
            else return false;

            return true;
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

            var sbManagerType = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (!ManagerTypeUtils.TryParseManagerType(sbManagerType, out var managerType))
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
                    case ManagerType.MigrationManager:
                    case ManagerType.ReplicationManager:
                        success = ClusterPurgeBufferPool(managerType);
                        break;
                    case ManagerType.ServerListener:
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