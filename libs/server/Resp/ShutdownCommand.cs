// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession
    {
        bool shutdownRequested;

        private bool NetworkSHUTDOWN()
        {
            bool? save = null;
            var now = false;
            var force = false;
            var abort = false;
            for (var i = 0; i < parseState.Count; i++)
            {
                var option = parseState.GetArgSliceByRef(i).ReadOnlySpan;
                if (option.EqualsUpperCaseSpanIgnoringCase("SAVE"u8))
                {
                    if (save.HasValue) return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    save = true;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("NOSAVE"u8))
                {
                    if (save.HasValue) return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    save = false;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("NOW"u8))
                {
                    if (now) return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    now = true;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("FORCE"u8))
                {
                    if (force) return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    force = true;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("ABORT"u8))
                {
                    if (abort) return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    abort = true;
                }
                else return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            if (abort)
            {
                if (parseState.Count != 1) return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                lock (storeWrapper.shutdownLock)
                {
                    if (storeWrapper.shutdownCancellation == null || storeWrapper.shutdownFinalizing)
                        return AbortWithErrorMessage("ERR No shutdown in progress."u8);
                    storeWrapper.shutdownCancellation.Cancel();
                    storeWrapper.clientPause.ClearShutdownPause();
                }
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend)) SendAndReset();
                return true;
            }

            if (storeWrapper.ShutdownRequested == null)
                return AbortWithErrorMessage("ERR SHUTDOWN is unavailable without a server host."u8);

            CancellationTokenSource cancellation;
            lock (storeWrapper.shutdownLock)
            {
                if (storeWrapper.shutdownCancellation != null)
                    return AbortWithErrorMessage("ERR Shutdown already in progress."u8);
                cancellation = storeWrapper.shutdownCancellation = new CancellationTokenSource();
            }

            clusterSession?.ReleaseCurrentEpoch();
            try
            {
                storeWrapper.clientPause.SetShutdownPause(false, this);
                cancellation.Token.ThrowIfCancellationRequested();
                if (!now && storeWrapper.clusterProvider != null)
                    storeWrapper.clusterProvider.WaitForReplicaSync(TimeSpan.FromSeconds(10), cancellation.Token);
                cancellation.Token.ThrowIfCancellationRequested();
                storeWrapper.clientPause.SetShutdownPause(true, this);
                lock (storeWrapper.shutdownLock)
                {
                    cancellation.Token.ThrowIfCancellationRequested();
                    storeWrapper.shutdownFinalizing = true;
                }

                if (save ?? !string.IsNullOrEmpty(storeWrapper.serverOptions.AofSizeLimit))
                {
                    try
                    {
                        if (!AsyncUtils.BlockingWait(storeWrapper.TakeCheckpointAsync(false, logger: logger)))
                            throw new InvalidOperationException("A checkpoint is already in progress.");
                    }
                    catch (Exception ex) when (force)
                    {
                        logger?.LogWarning(ex, "SHUTDOWN FORCE is proceeding after a checkpoint failure");
                    }
                }
                try
                {
                    _ = AsyncUtils.BlockingWait(storeWrapper.CommitAOFAsync());
                }
                catch (Exception ex) when (force)
                {
                    logger?.LogWarning(ex, "SHUTDOWN FORCE is proceeding after an AOF commit failure");
                }
                shutdownRequested = true;
            }
            catch (Exception ex)
            {
                lock (storeWrapper.shutdownLock)
                {
                    storeWrapper.clientPause.ClearShutdownPause();
                    storeWrapper.shutdownCancellation = null;
                    storeWrapper.shutdownFinalizing = false;
                }
                cancellation.Dispose();
                logger?.LogWarning(ex, "SHUTDOWN was canceled or failed");
                return AbortWithErrorMessage(ex is OperationCanceledException
                    ? "ERR Shutdown aborted."u8
                    : "ERR Errors trying to SHUTDOWN. Check logs."u8);
            }
            finally
            {
                clusterSession?.AcquireCurrentEpoch();
            }
            return true;
        }
    }
}