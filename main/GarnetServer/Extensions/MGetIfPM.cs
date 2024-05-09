﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Garnet.server;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom transaction MGETIFPM - get multiple keys whose values match with the given prefix
    /// 
    /// Format: MGETIFPM prefix key1 key2 ...
    /// Output: array of matching key-value pairs
    /// 
    /// Description: Perform a non-transactional multi-get with value condition (prefix match) for the given set of keys
    /// </summary>
    sealed class MGetIfPM : CustomTransactionProcedure
    {
        /// <summary>
        /// No transactional phase, skip Prepare
        /// </summary>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
            => false;

        /// <summary>
        /// Main will not be called because Prepare returns false
        /// </summary>
        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
            => throw new InvalidOperationException();

        /// <summary>
        /// Perform the MGETIFPM operation
        /// </summary>
        public override void Finalize<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;

            // Read prefix
            var prefix = GetNextArg(input, ref offset);

            // Read key, check condition, add to output
            ArgSlice key;
            List<ArgSlice> values = [];
            while ((key = GetNextArg(input, ref offset)).Length > 0)
            {
                if (api.GET(key, out var value) == GarnetStatus.OK)
                {
                    if (value.ReadOnlySpan.StartsWith(prefix.ReadOnlySpan))
                    {
                        values.Add(key);
                        values.Add(value);
                    }
                }
            }

            // Return the matching key-value pairs as an array of bulk strings
            WriteBulkStringArray(ref output, values);
        }
    }
}