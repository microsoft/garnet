// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.networking;

namespace Embedded.perftest
{
    /// <summary>
    /// Dummy network sender that reads from a fixed in-memory buffer
    /// </summary>
    unsafe class DummyNetworkSender : INetworkSender
    {
        /// <summary>
        /// Max size settings of the in-memory sender buffer
        /// </summary>
        readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Max size of the server buffer in bytes
        /// </summary>
        readonly int serverBufferSize;

        /// <summary>
        /// The in-memory sender buffer
        /// </summary>
        byte[] buffer;

        /// <summary>
        /// Pointer to the head of the sender buffer
        /// </summary>
        byte* bufferPtr;

        /// <summary>
        /// Create a new dummy network sender with a simple in-memory buffer
        /// </summary>
        public DummyNetworkSender()
        {
            maxSizeSettings = new MaxSizeSettings();
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            buffer = GC.AllocateArray<byte>(serverBufferSize, true);
            bufferPtr = (byte*)Unsafe.AsPointer(ref buffer[0]);
        }

        public MaxSizeSettings GetMaxSizeSettings => maxSizeSettings;

        public string RemoteEndpointName => "";

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
        public void GetResponseObject()
        {
        }

        /// <inheritdoc />
        public unsafe byte* GetResponseObjectHead()
        {
            return bufferPtr;
        }

        /// <inheritdoc />
        public unsafe byte* GetResponseObjectTail()
        {
            return bufferPtr + buffer.Length;
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
            return true;
        }

        /// <inheritdoc />
        public void SendResponse(byte[] buffer, int offset, int count, object context)
        {
        }

        /// <inheritdoc />
        public void Throttle()
        {
        }

        /// <inheritdoc />
        public INetworkSender Clone() { return this; }
    }
}