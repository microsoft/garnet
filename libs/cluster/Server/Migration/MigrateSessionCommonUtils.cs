// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        private ValueTask<bool> WriteOrSendMainStoreKeyValuePairAsync(GarnetClientSession gcs, LocalServerSession localServerSession, ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory o, out GarnetStatus status)
        {
            // Read value for key
            status = localServerSession.BasicGarnetApi.Read_MainStore(ref key, ref input, ref o);

            // Skip if key NOTFOUND
            if (status == GarnetStatus.NOTFOUND)
                return new(true);

            // Get SpanByte from stack if any
            ref var value = ref o.SpanByte;
            if (!o.IsSpanByte)
            {
                // Reinterpret heap memory to SpanByte
                value = ref SpanByte.ReinterpretWithoutLength(o.Memory.Memory.Span);
            }

            // Map up any namespaces as needed
            // TODO: Better way to do "has namespace"
            if (key.MetadataSize == 1)
            {
                var oldNs = key.GetNamespaceInPayload();
                if (_namespaceMap.TryGetValue(oldNs, out var newNs))
                {
                    Debug.Assert(newNs <= byte.MaxValue, "Namespace too large");
                    key.SetNamespaceInPayload((byte)newNs);
                }
            }

            // If expired, skip but do not fail
            if (ClusterSession.Expired(ref value))
            {
                return new(true);
            }

            // Write key to network buffer, potentially flushing if buffer is full
            return WriteOrSendMainStoreKeyValuePairAsync(gcs, ref key, ref value);

            ValueTask<bool> WriteOrSendMainStoreKeyValuePairAsync(GarnetClientSession gcs, ref SpanByte key, ref SpanByte value)
            {
                // Check if we need to initialize cluster migrate command arguments
                if (gcs.NeedsInitialization)
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true, isVectorSets: false);

                // Try write serialized key value to client buffer
                if (!gcs.TryWriteKeyValueSpanByte(ref key, ref value, out var task))
                {
                    // Flush key value pairs in the buffer
                    var handleResponseTask = HandleMigrateTaskResponseAsync(task);

                    // Copy key & value for async completion
                    var keyCopy = new byte[key.TotalSize];
                    var valueCopy = new byte[value.TotalSize];
                    key.CopyTo(keyCopy);
                    value.CopyTo(valueCopy);

                    return new(RetryHelperAsync(handleResponseTask, gcs, _sourceNodeId, _replaceOption, keyCopy, valueCopy, logger));
                }

                return new(true);

                static async Task<bool> RetryHelperAsync(Task<bool> handleResponseTask, GarnetClientSession gcs, string sourceNodeId, bool replaceOption, byte[] keyCopy, byte[] valueCopy, ILogger logger)
                {
                    try
                    {
                        if (await handleResponseTask.ConfigureAwait(false))
                        {
                            gcs.SetClusterMigrateHeader(sourceNodeId, replaceOption, isMainStore: true, isVectorSets: false);

                            unsafe
                            {
                                fixed (byte* keyCopyPtr = keyCopy, valueCopyPtr = valueCopy)
                                {
                                    ref var keyCopyRef = ref SpanByte.Reinterpret(keyCopyPtr);
                                    ref var valueCopyRef = ref SpanByte.Reinterpret(valueCopyPtr);

                                    if (!gcs.TryWriteKeyValueSpanByte(ref keyCopyRef, ref valueCopyRef, out _))
                                    {
                                        logger?.LogCritical($"{nameof(WriteOrSendMainStoreKeyValuePairAsync)} failed on retry");
                                        return false;
                                    }
                                }
                            }

                            return true;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error occurred in WriteOrSendMainStoreKeyValuePairAsync async path");
                    }

                    return false;
                }
            }
        }

        private ValueTask<bool> WriteOrSendObjectStoreKeyValuePairAsync(GarnetClientSession gcs, LocalServerSession localServerSession, ref SpanByte key, out GarnetStatus status)
        {
            var keyByteArray = key.AsReadOnlySpan().ToArray();

            ObjectInput input = default;
            GarnetObjectStoreOutput value = default;
            status = localServerSession.BasicGarnetApi.Read_ObjectStore(ref keyByteArray, ref input, ref value);

            // Skip if key NOTFOUND
            if (status == GarnetStatus.NOTFOUND)
                return new(true);

            if (!ClusterSession.Expired(ref value.GarnetObject))
            {
                var objectData = GarnetObjectSerializer.Serialize(value.GarnetObject);

                return WriteOrSendObjectStoreKeyValuePairAsync(gcs, keyByteArray, objectData, value.GarnetObject.Expiration);
            }

            return new(true);

            async ValueTask<bool> WriteOrSendObjectStoreKeyValuePairAsync(GarnetClientSession gcs, byte[] key, byte[] value, long expiration)
            {
                // Check if we need to initialize cluster migrate command arguments
                if (gcs.NeedsInitialization)
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: false, isVectorSets: false);

                if (!gcs.TryWriteKeyValueByteArray(key, value, expiration, out var task))
                {
                    // Flush key value pairs in the buffer
                    if (!await HandleMigrateTaskResponseAsync(task).ConfigureAwait(false))
                        return false;

                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: false, isVectorSets: false);

                    if (!gcs.TryWriteKeyValueByteArray(key, value, expiration, out _))
                    {
                        logger?.LogCritical($"{nameof(WriteOrSendObjectStoreKeyValuePairAsync)} failed on retry");
                        return false;
                    }
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
                    var res = await task.WaitAsync(_timeout, _cts.Token).ConfigureAwait(false);

                    // Check if setslotsrange executed correctly
                    if (!res.Equals("OK", StringComparison.Ordinal))
                    {
                        logger?.LogError("ClusterMigrate Keys failed with error:{error}.", res);
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