// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// The basic Epoch thread entry/exit management interface
    /// </summary>
    public interface IEpochGuard<TKernelSession>
        where TKernelSession : IKernelSession
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        static abstract void BeginUnsafe(ref TKernelSession kernelSession);

        /// <summary>Leave (suspend) the epoch.</summary>
        static abstract void EndUnsafe(ref TKernelSession kernelSession);
    }

    /// <summary>
    /// Perform epoch safety operations when entering/exiting an API 
    /// </summary>
    public struct SafeEpochGuard<TKernelSession> : IEpochGuard<TKernelSession>
        where TKernelSession : IKernelSession
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref TKernelSession kernelSession) => kernelSession.BeginUnsafe();

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref TKernelSession kernelSession) => kernelSession.EndUnsafe();
    }

    /// <summary>
    /// Do not perform epoch safety operations when entering/exiting an API (assumes a higher code region has done so)
    /// </summary>
    public struct UnsafeEpochGuard<TKernelSession> : IEpochGuard<TKernelSession>
        where TKernelSession : IKernelSession
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public static void BeginUnsafe(ref TKernelSession kernelSession) { }

        /// <summary>Leave (suspend) the epoch.</summary>
        public static void EndUnsafe(ref TKernelSession kernelSession) { }
    }
}
