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
        private bool WriteOrSendKeyValuePair(GarnetClientSession gcs, LocalServerSession localServerSession, PinnedSpanByte key, ref UnifiedStoreInput input, ref GarnetUnifiedStoreOutput output, out GarnetStatus status)
        {
            // Must initialize this here because we use the network buffer as output.
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption);

            // Read the value for the key. This will populate output with the entire serialized record.
            status = localServerSession.BasicGarnetApi.Read_UnifiedStore(key, ref input, ref output);
            var isObject = (output.OutputFlags & OutputFlags.ValueIsObject) == OutputFlags.ValueIsObject;

            return WriteRecord(gcs, ref output.SpanByteAndMemory, status, isObject);
        }

        bool WriteRecord(GarnetClientSession gcs, ref SpanByteAndMemory output, GarnetStatus status, bool isObject)
        {
            // Skip (do not fail) if key NOTFOUND
            if (status == GarnetStatus.NOTFOUND)
                return true;

            return WriteOrSendRecordSpan(gcs, ref output, isObject);
        }

        /// <summary>
        /// Write a serialized record directly to the client buffer; if there is not enough room, flush the buffer and retry writing.
        /// </summary>
        /// <param name="gcs">The client session</param>
        /// <param name="output">Output buffer from Read(), containing the full serialized record</param>
        /// <param name="isObject">Whether the record contains an object</param>
        /// <returns>True on success, else false</returns>
        private bool WriteOrSendRecordSpan(GarnetClientSession gcs, ref SpanByteAndMemory output, bool isObject)
        {
            // Check if we need to initialize cluster migrate command arguments
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption);

            fixed (byte* ptr = output.MemorySpan)
            {
                // Try to write serialized record to client buffer
                while (!gcs.TryWriteRecordSpan(new(ptr, output.Length), isObject, out var task))
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