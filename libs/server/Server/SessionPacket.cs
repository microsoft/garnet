// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.networking;

namespace Garnet.server
{
    class SessionPacket
    {
        public byte[] request;
        public byte[] response;
        public SemaphoreSlim completed;
        public INetworkSender responseSender;
    }
}