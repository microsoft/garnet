// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        private bool WriteOrSendRecord(GarnetClientSession gcs, LocalServerSession localServerSession, PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output, out GarnetStatus status)
        {
            // Must initialize this here because we use the network buffer as output.
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption);

            // Read the value for the key. This will populate output with the entire serialized record.
            status = localServerSession.BasicGarnetApi.Read_UnifiedStore(key, ref input, ref output);

            // Skip (but do not fail) if key NOTFOUND
            return status == GarnetStatus.NOTFOUND ? true : WriteOrSendRecordSpan(gcs, ref output.SpanByteAndMemory);
        }

        /// <summary>
        /// Write a serialized record directly to the client buffer; if there is not enough room, flush the buffer and retry writing.
        /// </summary>
        /// <param name="gcs">The client session</param>
        /// <param name="output">Output buffer from Read(), containing the full serialized record</param>
        /// <returns>True on success, else false</returns>
        private bool WriteOrSendRecordSpan(GarnetClientSession gcs, ref SpanByteAndMemory output)
        {
            // Check if we need to initialize cluster migrate command arguments
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption);

            fixed (byte* ptr = output.Span)
            {
                var serializedRecordLength = new LogRecord((long)ptr).GetSerializedSize();

                // Try to write serialized record to client buffer
                while (!gcs.TryWriteRecordSpan(new(ptr, serializedRecordLength), out var task))
                {
                    // Flush records in the buffer
                    if (!HandleMigrateTaskResponse(task))
                        return false;

                    // Re-initialize cluster migrate command parameters for the next loop iteration
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption);
                }
            }
            return true;
        }

        /// <summary>
        /// Handle response from migrate data task
        /// </summary>
        /// <param name="task"></param>
        /// <returns>True on successful completion of data send, otherwise false</returns>
        public bool HandleMigrateTaskResponse(Task<string> task)
        {
            if (task != null)
            {
                try
                {
                    return task.ContinueWith(resp =>
                    {
                        // Check if setslotsrange executed correctly
                        if (!resp.Result.Equals("OK", StringComparison.Ordinal))
                        {
                            logger?.LogError("ClusterMigrate Keys failed with error:{error}.", resp);
                            Status = MigrateState.FAIL;
                            return false;
                        }
                        return true;
                    }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(_timeout, _cts.Token).Result;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error has occurred");
                    Status = MigrateState.FAIL;
                    return false;
                }
            }
            return true;
        }
    }
}