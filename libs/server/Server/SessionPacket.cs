// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
using Garnet.networking;

namespace Garnet.server
{
    class SessionPacket
    {
        public ArgSlice request;
        public int readHead;
        public NetworkBuffer response;
        public SimpleObjectPool<NetworkBuffer> responsePool;
        public SemaphoreSlim completed;
        public INetworkSender responseSender;
    }
}