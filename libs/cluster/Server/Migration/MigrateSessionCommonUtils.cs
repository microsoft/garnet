// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        private unsafe ValueTask<bool> WriteOrSendRecordAsync(GarnetClientSession gcs, LocalServerSession localServerSession, PinnedSpanByte namespaceBytes, PinnedSpanByte key, ref VectorInput input, ref VectorOutput output, out GarnetStatus status)
        {
            Debug.Assert(namespaceBytes.Length == 1, "Longer namespaces not yet supported");

            // Must initialize this here because we use the network buffer as output.
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

            // Read the value for the key. This will populate output with the entire serialized record.
            var storeStatus = localServerSession.VectorBasicContext.Read(new VectorElementKey(namespaceBytes.ReadOnlySpan[0], key.ReadOnlySpan), ref input, ref output);

            if (storeStatus.IsPending)
            {
                CompletePending(ref storeStatus, ref output, ref localServerSession.VectorBasicContext);
            }

            if (storeStatus.Found)
            {
                status = GarnetStatus.OK;
            }
            else if (storeStatus.IsWrongType)
            {
                status = GarnetStatus.WRONGTYPE;
            }
            else
            {
                status = GarnetStatus.NOTFOUND;
            }

            // Skip (but do not fail) if key NOTFOUND, WRONGTYPE, BADSTATE, etc.
            if (status != GarnetStatus.OK)
            {
                return new(true);
            }

            // Map up any namespaces as needed
            VectorSessionFunctions.UpdateMigratedElementNamespaces(_namespaceMap, ref input, ref output);

            fixed (byte* ptr = output.SpanByteAndMemory.Span)
            {
                return WriteOrSendRecordSpanAsync(gcs, MigrationRecordSpanType.VectorSetElement, new(ptr, output.SpanByteAndMemory.Span.Length));
            }

            // Complete reads that go pending
            static void CompletePending(ref Status status, ref VectorOutput output, ref VectorBasicContext ctx)
            {
                _ = ctx.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                status = completedOutputs.Current.Status;
                output = completedOutputs.Current.Output;
                Debug.Assert(!completedOutputs.Next());
                completedOutputs.Dispose();
            }
        }

        private unsafe ValueTask<bool> WriteOrSendRecordAsync(GarnetClientSession gcs, LocalServerSession localServerSession, PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output, out GarnetStatus status)
        {
            // Must initialize this here because we use the network buffer as output.
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

            // Read the value for the key. This will populate output with the entire serialized record.
            status = localServerSession.BasicGarnetApi.Read_UnifiedStore(key, ref input, ref output);

            // Skip (but do not fail) if key NOTFOUND, WRONGTYPE, BADSTATE, etc.
            if (status != GarnetStatus.OK)
            {
                return new(true);
            }

            fixed (byte* ptr = output.SpanByteAndMemory.Span)
            {
                var serializedRecordLength = new LogRecord((long)ptr).GetSerializedSize();

                ReadOnlySpan<byte> toWrite = new(ptr, serializedRecordLength);

                return WriteOrSendRecordSpanAsync(gcs, MigrationRecordSpanType.LogRecord, toWrite);
            }
        }

        /// <summary>
        /// Write a serialized record directly to the client buffer; if there is not enough room, flush the buffer and retry writing.
        /// </summary>
        /// <param name="gcs">The client session</param>
        /// <param name="type"></param>
        /// <param name="span"></param>
        /// <returns>True on success, else false</returns>
        private ValueTask<bool> WriteOrSendRecordSpanAsync(GarnetClientSession gcs, MigrationRecordSpanType type, ReadOnlySpan<byte> span)
        {
            // Check if we need to initialize cluster migrate command arguments
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

            // Try to write serialized record to client buffer
            if (!gcs.TryWriteRecordSpan(span, type, out var task))
            {
                // Flush records in the buffer and retry
                var handleTask = HandleMigrateTaskResponseAsync(task);
                return new(RetryAsync(gcs, handleTask, span.ToArray()));
            }

            return new(true);

            async Task<bool> RetryAsync(GarnetClientSession gcs, Task<bool> task, byte[] span)
            {
                if (!await task.ConfigureAwait(false))
                {
                    return false;
                }

                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

                if (!gcs.TryWriteRecordSpan(span, type, out _))
                {
                    logger?.LogWarning($"TryWriteRecordSpan failed on retry");
                    return false;
                }

                return true;
            }
        }

        /// <summary>
        /// Handle response from migrate data task
        /// </summary>
        /// <param name="task"></param>
        /// <returns>True on successful completion of data send, otherwise false</returns>
        public async Task<bool> HandleMigrateTaskResponseAsync(Task<string> task)
        {
            if (task != null)
            {
                try
                {
                    var resp = await task.WaitAsync(_timeout, _cts.Token).ConfigureAwait(false);

                    if (!resp.Equals("OK", StringComparison.Ordinal))
                    {
                        logger?.LogError("ClusterMigrate Keys failed with error:{error}.", resp);
                        Status = MigrateState.FAIL;
                        return false;
                    }
                    return true;
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