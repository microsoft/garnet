// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    /// <summary>
    /// Session provider for Garnet, based on
    /// [K, V, I, O, C] = [SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long]
    /// </summary>
    public sealed class GarnetProvider : TsavoriteKVProviderBase<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, MainStoreFunctions, MainStoreAllocator, SpanByteServerSerializer>
    {
        readonly StoreWrapper storeWrapper;

        long lastSessionId;

        /// <summary>
        /// StoreWrapper
        /// </summary>
        internal StoreWrapper StoreWrapper => storeWrapper;

        internal CollectionItemBroker itemBroker;

        /// <summary>
        /// Create SpanByte TsavoriteKV backend for Garnet
        /// </summary>
        /// <param name="storeWrapper"></param>
        /// <param name="broker"></param>
        /// <param name="maxSizeSettings"></param>        
        public GarnetProvider(StoreWrapper storeWrapper,
            SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> broker = null,
            CollectionItemBroker itemBroker = null,
            MaxSizeSettings maxSizeSettings = default)
            : base(storeWrapper.store, new(), broker, false, maxSizeSettings)
        {
            this.storeWrapper = storeWrapper;
            this.itemBroker = itemBroker;
        }

        /// <summary>
        /// Start
        /// </summary>
        public void Start()
            => storeWrapper.Start();

        /// <summary>
        /// Recover
        /// </summary>
        public void Recover()
            => storeWrapper.Recover();

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            storeWrapper.Dispose();
        }

        /// <inheritdoc />
        public override SpanByteFunctionsForServer<long> GetFunctions() => new();

        /// <inheritdoc />
        public override IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender)
            => (wireFormat == WireFormat.ASCII)
                ? new RespServerSession(Interlocked.Increment(ref lastSessionId), networkSender, storeWrapper, broker, itemBroker, null, true)
                : throw new GarnetException($"Unsupported wireFormat {wireFormat}");
    }
}