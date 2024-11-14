// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    struct GarnetDualInputConverter : IDualInputConverter<SpanByte, SpanByte, byte[], ObjectInput, GarnetObjectStoreOutput>
    {
        public unsafe void ConvertForRead(ref SpanByte key1, ref SpanByte input1, out byte[] key2, out ObjectInput input2, out GarnetObjectStoreOutput output2)
        {
            ConvertKey(ref key1, out key2);

            var inputPtr = (RespInputHeader*)(input1.ToPointer() + sizeof(int));
            input2 = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = (*inputPtr).cmd switch
                    {
                        RespCommand.PERSIST => GarnetObjectType.Persist,
                        RespCommand.PTTL => GarnetObjectType.PTtl,
                        RespCommand.TTL => GarnetObjectType.Ttl,
                        RespCommand.PEXPIRE => GarnetObjectType.Expire,
                        RespCommand.EXPIRE => GarnetObjectType.Expire,
                        _ => throw new GarnetException($"Unexpected dual input: {(*inputPtr).cmd}")
                    }
                }
            };

            output2 = new() { spanByteAndMemory = new() };
        }

        public void ConvertForUpsert(ref SpanByte key1, ref SpanByte input1, out byte[] key2, out ObjectInput input2, out GarnetObjectStoreOutput output2)
            => ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);

        public void ConvertForRMW(ref SpanByte key1, ref SpanByte input1, out byte[] key2, out ObjectInput input2, out GarnetObjectStoreOutput output2)
            => ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);

        public void ConvertKey(ref SpanByte key1, out byte[] key2) => key2 = key1.ToByteArray();
    }
}