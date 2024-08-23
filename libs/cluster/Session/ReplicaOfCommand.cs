﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        private bool TryREPLICAOF(out bool invalidParameters)
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

            //Turn of replication and make replica into a primary but do not delete data
            if (addressSpan.EqualsUpperCaseSpanIgnoringCase("NO"u8) &&
                portSpan.EqualsUpperCaseSpanIgnoringCase("ONE"u8))
            {
                try
                {
                    if (!clusterProvider.replicationManager.StartRecovery())
                    {
                        logger?.LogError($"{nameof(TryREPLICAOF)}: {{logMessage}}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    clusterProvider.clusterManager.TryResetReplica();
                    clusterProvider.replicationManager.TryUpdateForFailover();
                    UnsafeBumpAndWaitForEpochTransition();
                }
                finally
                {
                    clusterProvider.replicationManager.SuspendRecovery();
                }
            }
            else
            {
                if (!NumUtils.TryParse(portSpan, out int port))
                {
                    var portStr = Encoding.ASCII.GetString(portSpan);
                    logger?.LogWarning($"{nameof(TryREPLICAOF)} failed to parse port {{port}}", portStr);
                    while (!RespWriteUtils.WriteError($"ERR REPLICAOF failed to parse port '{portStr}'", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                var addressStr = Encoding.ASCII.GetString(addressSpan);
                var primaryId = clusterProvider.clusterManager.CurrentConfig.GetWorkerNodeIdFromAddress(addressStr, port);
                if (primaryId == null)
                {
                    while (!RespWriteUtils.WriteError($"ERR I don't know about node {addressStr}:{port}.", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (!clusterProvider.replicationManager.TryBeginReplicate(this, primaryId, background: false, force: true, out var errorMessage))
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                return true;
            }

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}