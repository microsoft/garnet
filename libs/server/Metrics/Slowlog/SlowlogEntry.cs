// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Each entry in the slow log
    /// </summary>
    internal struct SlowLogEntry
    {
        public int Id;
        public int Timestamp;
        public int Duration;
        public RespCommand Command;
        public byte[] Arguments;
        public string ClientIpPort;
        public string ClientName;
    }
}