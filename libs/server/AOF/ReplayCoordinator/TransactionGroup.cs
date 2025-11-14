// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Transaction group contains list of operations associated with a given transaction
    /// </summary>
    public class TransactionGroup
    {
        /// <summary>
        /// Transaction operation buffer
        /// </summary>
        public List<byte[]> operations = [];

        /// <summary>
        /// Clear the underlying buffer that holds the individual transaction operations
        /// </summary>
        public void Clear() => operations.Clear();
    }
}