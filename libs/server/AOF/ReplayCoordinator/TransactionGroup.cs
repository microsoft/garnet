// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Transaction group contains logAccessMap and list of operations associated with this Txn
    /// </summary>
    public class TransactionGroup
    {
        public List<byte[]> operations = [];

        public void Clear() => operations.Clear();
    }
}
