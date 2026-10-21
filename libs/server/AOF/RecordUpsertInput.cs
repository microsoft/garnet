// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
            int inputLength = input.DeserializeFrom(source);
            if (inputLength != length - 2 * sizeof(long))
                throw new GarnetException("Invalid record upsert metadata length");
            ReadOptionals(source + inputLength);
            return length;
        }

        void ReadOptionals(byte* source)
        {
            input.isRecordImport = true;
            input.importedETag = *(long*)source;
            input.importedExpiration = *(long*)(source + sizeof(long));
        }
    }
}
