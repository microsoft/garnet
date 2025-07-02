// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Garnet.networking;

namespace Garnet.server
{
    class SessionPacket : IDisposable
    {
        public ArgSlice request;
        public int readHead;
        public NetworkBuffer response;
        public SimpleObjectPool<NetworkBuffer> responsePool;
        public SemaphoreSlim completed;
        public INetworkSender responseSender;

        public void Dispose()
        {
            completed?.Dispose();
        }

        public void CompleteResponse()
        {
            response.currOffset = 0;
            responsePool.Return(response);
            response = default;
            responsePool = default;
        }
    }
}