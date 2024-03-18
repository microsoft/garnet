// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking
{
    /// <summary>
    /// NetworkSenderBase class 
    /// </summary>
    public abstract class NetworkSenderBase : INetworkSender
    {
        /// <summary>
        /// Max size settings
        /// </summary>
        protected readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// serverBufferSize
        /// </summary>
        protected readonly int serverBufferSize;

        /// <summary>
        /// NetworkSenderBase constructor
        /// </summary>
        /// <param name="maxSizeSettings"></param>        
        public NetworkSenderBase(MaxSizeSettings maxSizeSettings)
        {
            this.maxSizeSettings = maxSizeSettings;
            this.serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
        }

        /// <summary>
        /// NetworkSenderBase constructor
        /// </summary>
        /// <param name="serverBufferSize"></param>        
        public NetworkSenderBase(int serverBufferSize)
        {
            this.serverBufferSize = serverBufferSize;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public MaxSizeSettings GetMaxSizeSettings => this.maxSizeSettings;

        /// <inheritdoc />
        public abstract string RemoteEndpointName { get; }

        /// <inheritdoc />
        public abstract void GetResponseObject();

        /// <inheritdoc />
        public abstract void ReturnResponseObject();

        /// <inheritdoc />
        public virtual unsafe byte* GetResponseObjectHead() { return null; }

        /// <inheritdoc />
        public virtual unsafe byte* GetResponseObjectTail() { return null; }

        /// <inheritdoc />
        public abstract bool SendResponse(int offset, int size);

        /// <inheritdoc />
        public abstract void SendResponse(byte[] buffer, int offset, int count, object context);

        /// <inheritdoc />
        public abstract void SendCallback(object context);

        /// <inheritdoc />
        public abstract void Dispose();

        /// <inheritdoc />
        public abstract void DisposeNetworkSender(bool waitForSendCompletion);

        /// <inheritdoc />
        public abstract void Throttle();
    }
}