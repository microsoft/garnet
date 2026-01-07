// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Transaction group contains logAccessMap and list of operations associated with this Txn
    /// </summary>
    /// <param name="sublogIdx"></param>
    /// <param name="logAccessMap"></param>
    public class TransactionGroup(int sublogIdx, byte logAccessMap)
    {
        public readonly int sublogIdx = sublogIdx;
        public readonly byte logAccessCount = logAccessMap;

        public List<byte[]> operations = [];

        public void Clear() => operations.Clear();
    }
}