// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Session provider for Garnet
    /// </summary>
    public sealed class GarnetProvider : TsavoriteKVProviderBase<PinnedSpanByte, SpanByteAndMemory, StoreFunctions, StoreAllocator>
    {
        readonly StoreWrapper storeWrapper;

        long lastSessionId;

        /// <summary>
        /// StoreWrapper
        /// </summary>
        internal StoreWrapper StoreWrapper => storeWrapper;

        /// <summary>
        /// Create SpanByte TsavoriteKV backend for Garnet
        /// </summary>
        /// <param name="storeWrapper"></param>
        /// <param name="broker"></param>
        /// <param name="maxSizeSettings"></param>        
        public GarnetProvider(StoreWrapper storeWrapper, SubscribeBroker broker = null, MaxSizeSettings maxSizeSettings = default)
            : base(broker, maxSizeSettings)
        {
            this.storeWrapper = storeWrapper;
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
        public override IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender)
            => (wireFormat == WireFormat.ASCII)
                ? new RespServerSession(Interlocked.Increment(ref lastSessionId), networkSender, storeWrapper, broker, authenticator: null, enableScripts: true)
                : throw new GarnetException($"Unsupported wireFormat {wireFormat}");
    }
}