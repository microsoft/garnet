// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.client
{
    /// <summary>
    /// GarnetClient disposed exception
    /// </summary>
    public sealed class GarnetClientDisposedException : ObjectDisposedException
    {
        internal GarnetClientDisposedException() : base("GarnetClient")
        {
        }
    }

    /// <summary>
    /// GarnetClient timeout exception
    /// </summary>
    public sealed class GarnetClientTimeoutException : TimeoutException
    {
        internal GarnetClientTimeoutException() : base("GarnetClient")
        {
        }
    }

    /// <summary>
    /// GarnetClient socket disposed exception
    /// </summary>
    public sealed class GarnetClientSocketDisposedException : ObjectDisposedException
    {
        internal GarnetClientSocketDisposedException() : base("GarnetClient Socket")
        {
        }
    }
}