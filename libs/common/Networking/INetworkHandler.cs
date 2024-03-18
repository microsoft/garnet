// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.networking
{
    /// <summary>
    /// Network handler interface
    /// </summary>
    public interface INetworkHandler : IDisposable
    {
        /// <summary>
        /// Get session
        /// </summary>
        IMessageConsumer Session { get; }
    }
}