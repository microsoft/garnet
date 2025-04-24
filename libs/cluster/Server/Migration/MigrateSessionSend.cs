// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Write a serialized record directly to the client buffer; if there is not enough room, flush the buffer and retry writing.
        /// </summary>
        /// <param name="output">Output buffer from Read(), containing the full serialized record</param>
        /// <returns>True on success, else false</returns>
        private bool WriteOrSendRecordSpan(ref SpanByteAndMemory output)
        {
            // Check if we need to initialize cluster migrate command arguments
            if (_gcs.NeedsInitialization)
                _gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true);

            fixed (byte* ptr = output.Memory.Memory.Span)
            {
                // Try to write serialized record to client buffer
                while (!_gcs.TryWriteRecordSpan(new(ptr, output.Length), out var task))
                {
                    // Flush records in the buffer
                    if (!HandleMigrateTaskResponse(task))
                        return false;

                    // Re-initialize cluster migrate command parameters for the next loop iteration
                    _gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true);
                }
            }
            return true;
        }

        /// <summary>
        /// Flush the final partial buffer to the client.
        /// </summary>
        /// <returns>True on success, else false</returns>
        private bool FlushFinalMigrationBuffer()
        {
            // Check if we need to initialize cluster migrate command arguments
            if (_gcs.NeedsInitialization)
                _gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true);
            return HandleMigrateTaskResponse(_gcs.SendAndResetIterationBuffer());
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