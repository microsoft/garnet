// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Garnet API implementation
    /// </summary>
    public partial struct GarnetApi<TStringContext, TObjectContext, TUnifiedContext> : IGarnetApi, IGarnetWatchApi
        where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
    {
        #region GET

        /// <inheritdoc />
        public GarnetStatus GETETAG(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region MEMORY

        /// <inheritdoc />
        public GarnetStatus MEMORYUSAGE(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region TYPE

        /// <inheritdoc />
        public GarnetStatus TYPE(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region TTL

        /// <inheritdoc />
        public GarnetStatus TTL(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region EXPIRETIME

        /// <inheritdoc />
        public GarnetStatus EXPIRETIME(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion

        #region EXISTS

        /// <inheritdoc />
        public GarnetStatus EXISTS(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.Read_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus EXISTS(PinnedSpanByte key)
            => storageSession.EXISTS(key, ref unifiedContext);

        #endregion

        #region DELETE

        /// <inheritdoc />
        public GarnetStatus DELETE(PinnedSpanByte key)
            => storageSession.DELETE(key, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus DELIFEXPIM(PinnedSpanByte key)
            => storageSession.DELIFEXPIM(key, ref unifiedContext);

        #endregion

        #region RENAME

        /// <inheritdoc />
        public GarnetStatus RENAME(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.RENAME(key, ref input, ref output);
        
        #endregion

        #region EXPIRE

        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.RMW_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        /// <inheritdoc />
        public unsafe GarnetStatus EXPIRE(PinnedSpanByte key, PinnedSpanByte expiryMs, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiryMs, out timeoutSet, expireOption, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus EXPIRE(PinnedSpanByte key, TimeSpan expiry, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIRE(key, expiry, out timeoutSet, expireOption, ref unifiedContext);

        #endregion

        #region EXPIREAT

        /// <inheritdoc />
        public GarnetStatus EXPIREAT(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIREAT(key, expiryTimestamp, out timeoutSet, expireOption, ref unifiedContext);

        /// <inheritdoc />
        public GarnetStatus PEXPIREAT(PinnedSpanByte key, long expiryTimestamp, out bool timeoutSet, ExpireOption expireOption = ExpireOption.None)
            => storageSession.EXPIREAT(key, expiryTimestamp, out timeoutSet, expireOption, ref unifiedContext, milliseconds: true);

        #endregion

        #region PERSIST

        /// <inheritdoc />
        public unsafe GarnetStatus PERSIST(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output)
            => storageSession.RMW_UnifiedStore(key, ref input, ref output, ref unifiedContext);

        #endregion
    }
}