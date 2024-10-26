// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - Basic Operations such as GET and SET that target the Object Store rather than Main Store
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET<TKeyLocker, TEpochGuard>(byte[] key, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(key, ref output, default);
            if (status.IsPending)
                CompletePending<TKeyLocker>(ref status, ref output);

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus GET<TKeyLocker, TEpochGuard>(ref HashEntryInfo hei, byte[] key, ref GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            ObjectInput input = default;
            ReadOptions readOptions = default;
            var status = ObjectContext.Read<TKeyLocker>(ref hei, ref key, ref input, ref output, ref readOptions, recordMetadata: out _, userContext: default);
            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus SET<TKeyLocker, TEpochGuard>(byte[] key, IGarnetObject value)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            dualContext.Upsert<TKeyLocker, TEpochGuard>(key, value);
            return GarnetStatus.OK;
        }

        public GarnetStatus SET<TKeyLocker>(ref HashEntryInfo hei, byte[] key, IGarnetObject value)
            where TKeyLocker : struct, ISessionLocker
        {
            ObjectInput input = default;
            ObjectContext.Upsert<TKeyLocker>(ref hei, ref key, ref input, ref value, output: out _, recordMetadata: out _, userContext: default);
            return GarnetStatus.OK;
        }
    }
}