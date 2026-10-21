// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal unsafe struct RecordUpsertInput : IStoreInput
    {
        internal UnifiedInput input;

        public readonly int SerializedLength => input.SerializedLength + 2 * sizeof(long);

        public int CopyTo(byte* destination, int length)
        {
            int inputLength = input.CopyTo(destination, length - 2 * sizeof(long));
            *(long*)(destination + inputLength) = input.importedETag;
            *(long*)(destination + inputLength + sizeof(long)) = input.importedExpiration;
            return inputLength + 2 * sizeof(long);
        }

        public int DeserializeFrom(byte* source)
        {
            int inputLength = input.DeserializeFrom(source);
            ReadOptionals(source + inputLength);
            return inputLength + 2 * sizeof(long);
        }

        internal int DeserializeFrom(byte* source, int length)
        {
            if (length < default(UnifiedInput).SerializedLength + 2 * sizeof(long))
                throw new GarnetException("Truncated record upsert metadata");
            ValidateInput(new ReadOnlySpan<byte>(source, length - 2 * sizeof(long)));
            int inputLength = input.DeserializeFrom(source);
            if (inputLength != length - 2 * sizeof(long))
                throw new GarnetException("Invalid record upsert metadata length");
            ReadOptionals(source + inputLength);
            return length;
        }

        static void ValidateInput(ReadOnlySpan<byte> bytes)
        {
            if (BinaryPrimitives.ReadInt32LittleEndian(bytes) != RespInputHeader.Size)
                throw new GarnetException("Invalid record upsert input header length");

            int offset = sizeof(int) + RespInputHeader.Size + sizeof(long);
            int argumentCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(offset));
            offset += sizeof(int);
            if (argumentCount < 0 || argumentCount > (bytes.Length - offset) / sizeof(int))
                throw new GarnetException("Invalid record upsert argument count");

            for (int index = 0; index < argumentCount; index++)
            {
                if (bytes.Length - offset < sizeof(int))
                    throw new GarnetException("Truncated record upsert argument length");
                int argumentLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(offset));
                offset += sizeof(int);
                if (argumentLength < 0 || argumentLength > bytes.Length - offset)
                    throw new GarnetException("Invalid record upsert argument length");
                offset += argumentLength;
            }
            if (offset != bytes.Length)
                throw new GarnetException("Invalid record upsert input length");
        }

        void ReadOptionals(byte* source)
        {
            input.isRecordImport = true;
            input.importedETag = *(long*)source;
            input.importedExpiration = *(long*)(source + sizeof(long));
        }
    }
}
