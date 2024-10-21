// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//using System;
//using System.Diagnostics;
//using Garnet.common;
//using Tsavorite.core;

//namespace Garnet.server
//{
//    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
//    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

//    sealed partial class StorageSession : IDisposable
//    {
//        private bool TryCustomObjectCommand<TGarnetApi>(byte* ptr, byte* end, RespCommand cmd, byte subid, CommandType type, ref TGarnetApi storageApi)
//            where TGarnetApi : IGarnetAdvancedApi
//        {
//            var keyBytes = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

//            // Prepare input
//            var input = new ObjectInput
//            {
//                header = new RespInputHeader
//                {
//                    cmd = cmd,
//                    SubId = subid
//                },
//                parseState = parseState,
//                parseStateStartIdx = 1
//            };

//            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

//            GarnetStatus status;

//            if (type == CommandType.ReadModifyWrite)
//            {
//                status = storageApi.RMW_ObjectStore(ref keyBytes, ref input, ref output);
//                Debug.Assert(!output.spanByteAndMemory.IsSpanByte);

//                switch (status)
//                {
//                    case GarnetStatus.WRONGTYPE:
//                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
//                            SendAndReset();
//                        break;
//                    default:
//                        if (output.spanByteAndMemory.Memory != null)
//                            SendAndReset(output.spanByteAndMemory.Memory, output.spanByteAndMemory.Length);
//                        else
//                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
//                                SendAndReset();
//                        break;
//                }
//            }
//            else
//            {
//                status = storageApi.Read_ObjectStore(ref keyBytes, ref input, ref output);
//                Debug.Assert(!output.spanByteAndMemory.IsSpanByte);

//                switch (status)
//                {
//                    case GarnetStatus.OK:
//                        if (output.spanByteAndMemory.Memory != null)
//                            SendAndReset(output.spanByteAndMemory.Memory, output.spanByteAndMemory.Length);
//                        else
//                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
//                                SendAndReset();
//                        break;
//                    case GarnetStatus.NOTFOUND:
//                        Debug.Assert(output.spanByteAndMemory.Memory == null);
//                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
//                            SendAndReset();
//                        break;
//                    case GarnetStatus.WRONGTYPE:
//                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
//                            SendAndReset();
//                        break;
//                }
//            }

//            return true;
//        }


//        public unsafe GarnetStatus CustomObjectCommand<TObjectContext>(ArgSlice key, ArgSlice[] elements, ListOperation lop, out int itemsDoneCount, ref TObjectContext objectStoreContext)
//  where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
//        {
//            itemsDoneCount = 0;

//            if (key.Length == 0 || elements.Length == 0)
//                return GarnetStatus.OK;

//            // Prepare the parse state
//            var parseState = new SessionParseState();
//            ArgSlice[] parseStateBuffer = default;
//            parseState.InitializeWithArguments(ref parseStateBuffer, elements);

//            // Prepare the input
//            var input = new ObjectInput
//            {
//                header = new RespInputHeader
//                {
//                    type = GarnetObjectType.List,
//                    ListOp = lop,
//                },
//                parseState = parseState,
//                parseStateStartIdx = 0,
//            };

//            var arrKey = key.ToArray();
//            var status = RMWObjectStoreOperation(arrKey, ref input, out var output, ref objectStoreContext);

//            itemsDoneCount = output.result1;
//            itemBroker.HandleCollectionUpdate(arrKey);
//            return status;
//        }

//    }
//}
