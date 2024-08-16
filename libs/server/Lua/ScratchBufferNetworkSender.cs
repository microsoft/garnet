// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.networking;

namespace Garnet.server
{
    /// <summary>
    /// Dummy network sender that reads from a fixed in-memory buffer
    /// </summary>
    unsafe class ScratchBufferNetworkSender : INetworkSender
    {
        readonly ScratchBufferManager scratchBufferManager;

        /// <summary>
        /// Max size settings of the in-memory sender buffer
        /// </summary>
        readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Max size of the server buffer in bytes
        /// </summary>
        readonly int serverBufferSize;

        /// <summary>
        /// Create a new dummy network sender with a simple in-memory buffer
        /// </summary>
        public ScratchBufferNetworkSender()
        {
            maxSizeSettings = new MaxSizeSettings();
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            scratchBufferManager = new();
        }

        public ArgSlice GetResponse()
            => scratchBufferManager.ViewFullArgSlice();

        public void Reset()
            => scratchBufferManager.Reset();

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
        public void Enter() { }

        /// <inheritdoc />
        public void EnterAndGetResponseObject(out byte* head, out byte* tail)
        {
            var remain = scratchBufferManager.ViewRemainingArgSlice(serverBufferSize);
            head = remain.ptr;
            tail = remain.ptr + remain.length;
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
            var remain = scratchBufferManager.ViewRemainingArgSlice(serverBufferSize);
            return remain.ptr;
        }

        /// <inheritdoc />
        public unsafe byte* GetResponseObjectTail()
        {
            var remain = scratchBufferManager.ViewRemainingArgSlice(serverBufferSize);
            return remain.ptr + remain.length;
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
            scratchBufferManager.MoveOffset(offset + size);
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
    }
}