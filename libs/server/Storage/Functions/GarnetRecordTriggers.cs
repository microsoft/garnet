// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Available for app-level
    /// resource cleanup (e.g. <see cref="System.IDisposable.Dispose"/> on value objects
    /// holding external resources). Currently a no-op — Garnet's IHeapObject implementations
    /// (Hash/List/Set/SortedSet) hold no external resources.
    /// </summary>
    public readonly struct GarnetRecordTriggers : IRecordTriggers
    {
    }
}