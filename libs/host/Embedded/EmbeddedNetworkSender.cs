// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#nullable disable

using System;
using System.Runtime.CompilerServices;
using Garnet.networking;

namespace Embedded.server
{
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

        /// <summary>
        /// The in-memory sender buffer
        /// </summary>
        byte[] buffer;

        /// <summary>
        /// Pointer to the head of the sender buffer
        /// </summary>
        byte* bufferPtr;

        int currOffset;

        /// <summary>
        /// Create a new dummy network sender with a simple in-memory buffer
        /// </summary>
        public EmbeddedNetworkSender()
        {
            maxSizeSettings = new MaxSizeSettings();
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            buffer = GC.AllocateArray<byte>(serverBufferSize, true);
            bufferPtr = (byte*)Unsafe.AsPointer(ref buffer[0]);
        }

        public ReadOnlySpan<byte> GetResponse()
        {
            var _offset = currOffset;
            currOffset = 0;
            return new ReadOnlySpan<byte>(buffer, 0, _offset);
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
            head = bufferPtr + currOffset;
            tail = bufferPtr + buffer.Length;
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
            return bufferPtr + currOffset;
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
            currOffset += size;
            return true;
        }

        /// <inheritdoc />
        public void SendResponse(byte[] buffer, int offset, int count, object context)
        {
            currOffset += count;
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