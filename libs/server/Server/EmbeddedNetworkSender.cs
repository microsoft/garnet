// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#nullable disable

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.networking;

namespace Garnet.server
{
    unsafe class NetworkBuffer : IDisposable
    {
        public readonly byte[] buffer;
        public readonly byte* bufferPtr;
        public int currOffset;

        public NetworkBuffer(int size)
        {
            buffer = GC.AllocateArray<byte>(size, true);
            bufferPtr = (byte*)Unsafe.AsPointer(ref buffer[0]);
            currOffset = 0;
        }

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// Dummy network sender that reads from a fixed in-memory buffer
    /// </summary>
    internal unsafe class EmbeddedNetworkSender : INetworkSender
    {
        /// <summary>
        /// Max size settings of the in-memory sender buffer
        /// </summary>
        readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Max size of the server buffer in bytes
        /// </summary>
        readonly int serverBufferSize;

        public readonly SimpleObjectPool<NetworkBuffer> networkBufferPool;

        public NetworkBuffer buffer;

        /// <summary>
        /// Create a new dummy network sender with a simple in-memory buffer
        /// </summary>
        public EmbeddedNetworkSender()
        {
            maxSizeSettings = new MaxSizeSettings();
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            networkBufferPool = new SimpleObjectPool<NetworkBuffer>(() => new NetworkBuffer(serverBufferSize));
        }

        public ArgSlice GetResponse()
            => new ArgSlice(buffer.bufferPtr, buffer.currOffset);

        public void CompleteResponseProcessing()
        {
            buffer.currOffset = 0;
            networkBufferPool.Return(buffer);
        }

        public MaxSizeSettings GetMaxSizeSettings => maxSizeSettings;

        public string RemoteEndpointName => "";

        public string LocalEndpointName => "";

        public bool IsLocalConnection()
        {
            return true;
        }

        /// <summary>
        /// Cleanup this DummyNetworkSender instance
        /// </summary>
        public void Dispose()
        {
        }

        /// <inheritdoc />
        public void DisposeNetworkSender(bool waitForSendCompletion)
        {
        }

        /// <inheritdoc />
        public void Enter() { }

        /// <inheritdoc />
        public void EnterAndGetResponseObject(out byte* head, out byte* tail)
        {
            buffer = networkBufferPool.Checkout();
            head = buffer.bufferPtr + buffer.currOffset;
            tail = buffer.bufferPtr + buffer.buffer.Length;
        }

        /// <inheritdoc />
        public void Exit() { }

        /// <inheritdoc />
        public void ExitAndReturnResponseObject()
        {
        }

        /// <inheritdoc />
        public void GetResponseObject()
        {
        }

        /// <inheritdoc />
        public unsafe byte* GetResponseObjectHead()
        {
            return buffer.bufferPtr + buffer.currOffset;
        }

        /// <inheritdoc />
        public unsafe byte* GetResponseObjectTail()
        {
            return buffer.bufferPtr + buffer.buffer.Length;
        }

        /// <inheritdoc />
        public void ReturnResponseObject()
        {
        }

        /// <inheritdoc />
        public void SendCallback(object context)
        {
        }

        /// <inheritdoc />
        public bool SendResponse(int offset, int size)
        {
            buffer.currOffset += size;
            if (buffer.currOffset > buffer.buffer.Length)
            {
                throw new InvalidOperationException("Buffer overflow in EmbeddedNetworkSender");
            }
            return true;
        }

        /// <inheritdoc />
        public void SendResponse(byte[] buffer, int offset, int count, object context)
        {
            throw new InvalidOperationException("not expected");
        }

        /// <inheritdoc />
        public void Throttle()
        {
        }

        /// <inheritdoc />
        public bool TryClose()
        {
            return false;
        }
    }
}