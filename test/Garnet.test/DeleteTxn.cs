﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom transaction Delete key/object
    /// 
    /// Format: DeleteTxn 1 key
    /// 
    /// Description: Delete key
    /// </summary>
    sealed class DeleteTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            AddKey(GetNextArg(ref procInput.parseState, procInput.parseStateFirstArgIdx, ref offset), LockType.Exclusive, false);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput.parseState, procInput.parseStateFirstArgIdx, ref offset);
            api.DELETE(key, StoreType.Main);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}