// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Embedded.perftest
{
    /// <summary>
    /// Garnet operations to execute in the benchmark 
    /// </summary>
    public enum OperationType : byte
    {
        PING, DBSIZE, CLIENT, ECHO, SET, GET
    }
}