// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.test.epoch
{
    /// <summary>
    /// Scopes a protected region to a <c>using</c> block, so that a failing assertion still suspends.
    /// </summary>
    internal static class EpochProtection
    {
        internal static Scope ProtectedScope(this LightEpoch epoch)
        {
            epoch.Resume();
            return new Scope(epoch);
        }

        internal readonly struct Scope : IDisposable
        {
            readonly LightEpoch epoch;

            internal Scope(LightEpoch epoch) => this.epoch = epoch;

            public void Dispose() => epoch.Suspend();
        }
    }
}