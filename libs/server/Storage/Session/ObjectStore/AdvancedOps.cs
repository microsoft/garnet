// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus RMW_ObjectStore<TKeyLocker, TEpochGuard>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            if (status.Found)
                return output.spanByteAndMemory.Length == 0 ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_ObjectStore<TKeyLocker, TEpochGuard>(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            if (status.Found)
                return output.spanByteAndMemory.Length == 0 ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
            return GarnetStatus.NOTFOUND;
        }
    }
}