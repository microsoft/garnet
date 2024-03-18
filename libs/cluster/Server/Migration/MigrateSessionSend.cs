// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Write main store key-value pair directly to client buffer or flush buffer to make space and try again writing.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns>True on success, else false</returns>
        private bool WriteOrSendMainStoreKeyValuePair(ref SpanByte key, ref SpanByte value)
        {
            // Check if we need to initialize cluster migrate command arguments
            if (_gcs.InitMigrateCommand)
                _gcs.SetClusterMigrate(_sourceNodeId, Encoding.ASCII.GetBytes(_replaceOption ? "T" : "F"), Encoding.ASCII.GetBytes("SSTORE"));

            // Try write serialized key value to client buffer
            while (!_gcs.TryWriteKeyValueSpanByte(ref key, ref value, out var task))
            {
                // Flush key value pairs in the buffer
                if (!HandleMigrateTaskResponse(task))
                    return false;

                // re-initialize cluster migrate command parameters
                _gcs.SetClusterMigrate(_sourceNodeId, Encoding.ASCII.GetBytes(_replaceOption ? "T" : "F"), Encoding.ASCII.GetBytes("SSTORE"));
            }
            return true;
        }

        /// <summary>
        /// Write object store key-value pair directly to client buffer or flush buffer to make space and try again writing.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="expiration"></param>
        /// <returns></returns>
        private bool WriteOrSendObjectStoreKeyValuePair(byte[] key, byte[] value, long expiration)
        {
            // Check if we need to initialize cluster migrate command arguments
            if (_gcs.InitMigrateCommand)
                _gcs.SetClusterMigrate(_sourceNodeId, Encoding.ASCII.GetBytes(_replaceOption ? "T" : "F"), Encoding.ASCII.GetBytes("OSTORE"));

            while (!_gcs.TryWriteKeyValueByteArray(key, value, expiration, out var task))
            {
                // Flush key value pairs in the buffer
                if (!HandleMigrateTaskResponse(task))
                    return false;
                _gcs.SetClusterMigrate(_sourceNodeId, Encoding.ASCII.GetBytes(_replaceOption ? "T" : "F"), Encoding.ASCII.GetBytes("OSTORE"));
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
                    bool status = task.ContinueWith(resp =>
                    {
                        // Check if setslotsrange executed correctly
                        if (!resp.Result.Equals("OK"))
                        {
                            logger?.LogError("TrySetSlot error: {error}", resp);
                            Status = MigrateState.FAIL;
                            return false;
                        }
                        return true;
                    }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(_timeout, _cts.Token).Result;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error has occurred");
                    return false;
                }
            }
            return true;
        }
    }
}