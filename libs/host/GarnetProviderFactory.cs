// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Options;
using Tsavorite.core;

namespace Garnet;

public class GarnetProviderFactory
{
    readonly GarnetServerOptions options;
    readonly SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscribeBroker;
    readonly StoreWrapper storeWrapper;

    public GarnetProviderFactory(
        IOptions<GarnetServerOptions> options,
        SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscribeBroker,
        StoreWrapper storeWrapper)
    {
        this.options = options.Value;
        this.subscribeBroker = subscribeBroker;
        this.storeWrapper = storeWrapper;
    }

    public GarnetProvider Create()
    {
        if (options.DisablePubSub)
        {
            return new GarnetProvider(storeWrapper, null);
        }

        return new GarnetProvider(storeWrapper, subscribeBroker);
    }
}