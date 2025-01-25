// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common
{
    public struct ReplicationInfo
    {
        public long offset;
        public long lag;
        public string address;
        public string state;
        public int port;

        public override string ToString()
        {
            return $"ip={address},port={port},state={state},offset={offset},lag={lag}";
        }
    }
}
