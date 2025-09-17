// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    internal struct ConnectionInfo
    {
        public long ping;
        public long pong;
        public bool connected;
        public long lastIO;

        public ConnectionInfo()
        {
            ping = 0;
            pong = 0;
            connected = false;
            lastIO = 0;
        }
    }
}