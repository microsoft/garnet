// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    internal struct SlowLogEntry
    {
        public int Id;
        public int Timestamp;
        public int Duration;
        public List<string> Arguments;
        public string ClientIpPort;
        public string ClientName;
    }
}