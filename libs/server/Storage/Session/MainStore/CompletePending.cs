// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession
    {
        void CompletePending<TKeyLocker>(out Status status, out SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
        {
            // Main store
            var suspended = Kernel.Epoch.ThisInstanceProtected();
            if (suspended)
                dualContext.KernelSession.EndUnsafe();
            StartPendingMetrics();

            _ = MainContext.CompletePendingWithOutputs<TKeyLocker>(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();

            StopPendingMetrics();
            if (suspended)
                dualContext.KernelSession.BeginUnsafe();
        }
    }
}