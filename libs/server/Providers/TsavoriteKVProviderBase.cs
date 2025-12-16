// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Abstract session provider for TsavoriteKV store
    /// </summary>
    public abstract class TsavoriteKVProviderBase<TInput, TOutput, TStoreFunctions, TAllocator> : ISessionProvider
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Store
        /// </summary>
        protected readonly TsavoriteKV<TStoreFunctions, TAllocator> store;

        /// <summary>
        /// Broker
        /// </summary>
        protected readonly SubscribeBroker broker;

        /// <summary>
        /// Size settings
        /// </summary>
        protected readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Create TsavoriteKV backend
        /// </summary>
        /// <param name="broker"></param>
        /// <param name="maxSizeSettings"></param>
        public TsavoriteKVProviderBase(SubscribeBroker broker = null, MaxSizeSettings maxSizeSettings = default)
        {
            this.broker = broker;
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
        }

        /// <summary>
        /// Get MaxSizeSettings
        /// </summary>
        public MaxSizeSettings GetMaxSizeSettings => this.maxSizeSettings;

        /// <inheritdoc />
        public abstract IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender);
    }
}