// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    class TxnCustomCmd : CustomTransactionProcedure
    {
        private CustomObjectCommand customObjectCommand;

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref procInput, ref offset);
            _ = GetNextArg(ref procInput, ref offset); // mainStoreValue

            AddKey(mainStoreKey, LockType.Exclusive, StoreType.Main);

            var myDictKey = GetNextArg(ref procInput, ref offset);
            AddKey(myDictKey, LockType.Exclusive, StoreType.Object);

            if (!ParseCustomObjectCommand("MYDICTSET", out customObjectCommand))
                return false;

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref procInput, ref offset);
            var mainStoreValue = GetNextArg(ref procInput, ref offset);

            api.SET(mainStoreKey, mainStoreValue);

            var myDictKey = GetNextArg(ref procInput, ref offset);
            var myDictField = GetNextArg(ref procInput, ref offset);
            var myDictValue = GetNextArg(ref procInput, ref offset);

            var args = new PinnedSpanByte[2];
            args[0] = myDictField;
            args[1] = myDictValue;

            ExecuteCustomObjectCommand(api, customObjectCommand, myDictKey, args, out var _output);

            WriteSimpleString(ref output, "OK");
        }
    }
}