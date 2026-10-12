// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.cluster.Server.Replication;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        private bool NetworkTryREPLICAOF(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 2 arguments
            if (parseState.Count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var addressSpan = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var portSpan = parseState.GetArgSliceByRef(1).ReadOnlySpan;

            // Turn of replication and make replica into a primary but do not delete data
            if (addressSpan.EqualsUpperCaseSpanIgnoringCase("NO"u8) &&
                portSpan.EqualsUpperCaseSpanIgnoringCase("ONE"u8))
            {
                if (!clusterProvider.replicationManager.BeginRecovery(RecoveryStatus.ReplicaOfNoOne, upgradeLock: false))
                {
                    logger?.LogError($"{nameof(NetworkTryREPLICAOF)}: {{logMessage}}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                return SuspendOn(ReplicaOfNoOneAsync(), PendingReply.Ok);
            }
            else
            {
                if (!NumUtils.TryParse(portSpan, out int port))
                {
                    var portStr = Encoding.ASCII.GetString(portSpan);
                    logger?.LogWarning($"{nameof(NetworkTryREPLICAOF)} failed to parse port {{port}}", portStr);
                    while (!RespWriteUtils.TryWriteError($"ERR REPLICAOF failed to parse port '{portStr}'", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                var addressStr = Encoding.ASCII.GetString(addressSpan);
                var primaryId = clusterProvider.clusterManager.CurrentConfig.GetWorkerNodeIdFromAddress(addressStr, port) ??
                    clusterProvider.clusterManager.CurrentConfig.GetWorkerNodeIdFromAddressOrHostname(addressStr, port);
                if (primaryId == null)
                {
                    while (!RespWriteUtils.TryWriteError($"ERR I don't know about node {addressStr}:{port}.", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                ReplicateSyncOptions syncOpts = new(
                    primaryId,
                    Background: false,
                    Force: true,
                    TryAddReplica: true,
                    AllowReplicaResetOnFailure: true,
                    UpgradeLock: false
                );

                return SuspendOn(ReplicateAsync(syncOpts));
            }
        }
    }
}