﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Abstract session provider for TsavoriteKV store based on
    /// [K, V, I, O, F, P]
    /// </summary>
    public abstract class TsavoriteKVProviderBase<TKey, TValue, TInput, TOutput, TSessionFunctions, TStoreFunctions, TAllocator, TParameterSerializer> : ISessionProvider
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, long>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        where TParameterSerializer : IServerSerializer<TKey, TValue, TInput, TOutput>
    {
        /// <summary>
        /// Store
        /// </summary>
        protected readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;

        /// <summary>
        /// Serializer
        /// </summary>
        protected readonly TParameterSerializer serializer;

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
        /// <param name="serializer"></param>
        /// <param name="broker"></param>
        /// <param name="maxSizeSettings"></param>
        public TsavoriteKVProviderBase(TParameterSerializer serializer,
                SubscribeBroker broker = null, MaxSizeSettings maxSizeSettings = default)
        {
            this.broker = broker;
            this.serializer = serializer;
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
        }

        /// <summary>
        /// Get MaxSizeSettings
        /// </summary>
        public MaxSizeSettings GetMaxSizeSettings => this.maxSizeSettings;

        /// <summary>
        /// GetFunctions() for custom functions provided by the client
        /// </summary>
        /// <returns></returns>
        public abstract TSessionFunctions GetFunctions();

        /// <inheritdoc />
        public abstract IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender);
    }
}