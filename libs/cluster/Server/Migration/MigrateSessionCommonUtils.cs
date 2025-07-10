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
        private bool WriteOrSendMainStoreKeyValuePair(GarnetClientSession gcs, LocalServerSession localServerSession, ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory o, out GarnetStatus status)
        {
            // Read value for key
            status = localServerSession.BasicGarnetApi.Read_MainStore(ref key, ref input, ref o);

            // Skip if key NOTFOUND
            if (status == GarnetStatus.NOTFOUND)
                return true;

            // Get SpanByte from stack if any
            ref var value = ref o.SpanByte;
            if (!o.IsSpanByte)
            {
                // Reinterpret heap memory to SpanByte
                value = ref SpanByte.ReinterpretWithoutLength(o.Memory.Memory.Span);
            }

            // Write key to network buffer if it has not expired
            if (!ClusterSession.Expired(ref value) && !WriteOrSendMainStoreKeyValuePair(gcs, ref key, ref value))
                return false;

            return true;

            bool WriteOrSendMainStoreKeyValuePair(GarnetClientSession gcs, ref SpanByte key, ref SpanByte value)
            {
                // Check if we need to initialize cluster migrate command arguments
                if (gcs.NeedsInitialization)
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true);

                // Try write serialized key value to client buffer
                while (!gcs.TryWriteKeyValueSpanByte(ref key, ref value, out var task))
                {
                    // Flush key value pairs in the buffer
                    if (!HandleMigrateTaskResponse(task))
                        return false;

                    // re-initialize cluster migrate command parameters
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true);
                }
                return true;
            }
        }

        private bool WriteOrSendObjectStoreKeyValuePair(GarnetClientSession gcs, LocalServerSession localServerSession, ref ArgSlice key, out GarnetStatus status)
        {
            var keyByteArray = key.ToArray();

            ObjectInput input = default;
            GarnetObjectStoreOutput value = default;
            status = localServerSession.BasicGarnetApi.Read_ObjectStore(ref keyByteArray, ref input, ref value);

            // Skip if key NOTFOUND
            if (status == GarnetStatus.NOTFOUND)
                return true;

            if (!ClusterSession.Expired(ref value.GarnetObject))
            {
                var objectData = GarnetObjectSerializer.Serialize(value.GarnetObject);

                if (!WriteOrSendObjectStoreKeyValuePair(gcs, keyByteArray, objectData, value.GarnetObject.Expiration))
                    return false;
            }

            return true;

            bool WriteOrSendObjectStoreKeyValuePair(GarnetClientSession gcs, byte[] key, byte[] value, long expiration)
            {
                // Check if we need to initialize cluster migrate command arguments
                if (gcs.NeedsInitialization)
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: false);

                while (!gcs.TryWriteKeyValueByteArray(key, value, expiration, out var task))
                {
                    // Flush key value pairs in the buffer
                    if (!HandleMigrateTaskResponse(task))
                        return false;
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: false);
                }
                return true;
            }
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