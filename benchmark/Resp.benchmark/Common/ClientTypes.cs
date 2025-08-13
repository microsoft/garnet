// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    public enum ClientType : byte
    {
        LightClient,
        SERedis,
        GarnetClientSession,
        GarnetClient,
        InProc
    }
}