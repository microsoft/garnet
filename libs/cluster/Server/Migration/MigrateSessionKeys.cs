// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Method used to migrate individual keys from main store to target node.
        /// Used for MIGRATE KEYS option
        /// </summary>
        /// <param name="keysWithSize">List of pairs of address to the network receive buffer, key size </param>
        /// <param name="objectStoreKeys">Output keys not found in main store so we can scan the object store next</param>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromMainStore(ref List<(long, long)> keysWithSize, out List<(long, long)> objectStoreKeys)
        {
            int bufferSize = 1 << 10;
            SectorAlignedMemory buffer = new(bufferSize, 1);
            objectStoreKeys = new();

            try
            {
                // 4 byte length of input
                // 1 byte RespCommand
                // 1 byte RespInputFlags
                int inputSize = sizeof(int) + RespInputHeader.Size;
                byte* pbCmdInput = stackalloc byte[inputSize];

                ////////////////
                // Build Input//
                ////////////////
                byte* pcurr = pbCmdInput;
                *(int*)pcurr = inputSize - sizeof(int);
                pcurr += sizeof(int);
                // 1. Header
                ((RespInputHeader*)(pcurr))->SetHeader(RespCommandAccessor.MIGRATE, 0);

                byte* bufPtr = buffer.GetValidPointer();
                byte* bufPtrEnd = bufPtr + bufferSize;
                for (int i = 0; i < keysWithSize.Count; i++)
                {
                    // 1. Prepare key pointers
                    var tuple = keysWithSize[i];
                    byte* keyPtr = (byte*)((IntPtr)tuple.Item1).ToPointer();
                    int ksize = (int)tuple.Item2;
                    keyPtr -= sizeof(int);
                    *(int*)keyPtr = ksize;

                    // 2. Read value for key
                    var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                    var status = localServerSession.BasicGarnetApi.Read_MainStore(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

                    // 3. Check if found
                    if (status == GarnetStatus.NOTFOUND) // All keys must exist
                    {
                        // Add to different list to check object store
                        objectStoreKeys.Add(keysWithSize[i]);
                        continue;
                    }

                    //4. Make value SpanByte
                    keyPtr = (byte*)((IntPtr)tuple.Item1).ToPointer();
                    SpanByte key = SpanByte.FromPointer(keyPtr, ksize);
                    SpanByte value;
                    MemoryHandle memoryHandle = default;
                    if (!o.IsSpanByte)
                    {
                        memoryHandle = o.Memory.Memory.Pin();
                        value = SpanByte.FromPinnedMemory(o.Memory.Memory);
                    }
                    else
                        value = o.SpanByte;

                    if (!ClusterSession.Expired(ref value) && !WriteOrSendMainStoreKeyValuePair(ref key, ref value))
                        return false;

                    if (!o.IsSpanByte)
                    {
                        memoryHandle.Dispose();
                        o.Memory.Dispose();
                    }
                }

                // Flush data in client buffer
                if (!HandleMigrateTaskResponse(_gcs.SendAndResetMigrate()))
                    return false;
                return true;
            }
            finally
            {
                buffer.Dispose();
            }
        }

        /// <summary>
        /// Method used to migrate individual keys from object store to target node.
        /// Used for MIGRATE KEYS option
        /// </summary>
        /// <param name="objectStoreKeys">List of pairs of address to the network receive buffer, key size that were not found in main store</param>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromObjectStore(ref List<(long, long)> objectStoreKeys)
        {
            for (int i = 0; i < objectStoreKeys.Count; i++)
            {
                var tuple = objectStoreKeys[i];
                byte* keyPtr = (byte*)((IntPtr)tuple.Item1).ToPointer();
                int ksize = (int)tuple.Item2;

                byte[] key = new byte[ksize];
                Marshal.Copy((IntPtr)keyPtr, key, 0, ksize);

                SpanByte input = default;
                GarnetObjectStoreOutput value = default;
                var status = localServerSession.BasicGarnetApi.Read_ObjectStore(ref key, ref input, ref value);
                if (status == GarnetStatus.NOTFOUND)
                    continue;

                if (!ClusterSession.Expired(ref value.garnetObject))
                {
                    byte[] objectData = clusterProvider.storeWrapper.SerializeGarnetObject(value.garnetObject);

                    if (!WriteOrSendObjectStoreKeyValuePair(key, objectData, value.garnetObject.Expiration))
                        return false;
                }
            }

            // Flush data in client buffer
            if (!HandleMigrateTaskResponse(_gcs.SendAndResetMigrate()))
                return false;
            return true;
        }

        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// Used for MIGRATE KEYS option
        /// </summary>
        public bool MigrateKeys()
        {
            var keysWithSize = _keysWithSize;
            try
            {
                CheckConnection();
                _gcs.InitMigrateBuffer();
                if (!MigrateKeysFromMainStore(ref keysWithSize, out var objectStoreKeys))
                    return false;

                if (!clusterProvider.serverOptions.DisableObjects && objectStoreKeys.Count > 0)
                {
                    _gcs.InitMigrateBuffer();
                    if (!MigrateKeysFromObjectStore(ref objectStoreKeys))
                        return false;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
            }
            return true;
        }

        /// <summary>
        /// Delete local copy of keys if _copyOption is set to false.
        /// </summary>                
        public void DeleteKeys(List<(long, long)> keysWithSize)
        {
            if (_copyOption)
                return;

            for (int i = 0; i < keysWithSize.Count; i++)
            {
                var tuple = keysWithSize[i];
                byte* keyPtr = (byte*)((IntPtr)tuple.Item1).ToPointer();
                int ksize = (int)tuple.Item2;
                keyPtr -= sizeof(int);
                *(int*)keyPtr = ksize;
                localServerSession.BasicGarnetApi.DELETE(ref Unsafe.AsRef<SpanByte>(keyPtr));
            }
        }
    }
}