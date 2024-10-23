// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession
    {
        void CompletePending<TKeyLocker, TEpochGuard>(out Status status, out GarnetObjectStoreOutput output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Object store
            TEpochGuard.EndUnsafe(ref dualContext.KernelSession);
            StartPendingMetrics();

            _ = ObjectContext.CompletePendingWithOutputs<TKeyLocker>(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();

            StopPendingMetrics();
            TEpochGuard.BeginUnsafe(ref dualContext.KernelSession);
        }
    }
}