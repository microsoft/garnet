// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// The basic Epoch thread entry/exit management interface
    /// </summary>
    public interface IEpochThread<TKernelSession>
        where TKernelSession : IKernelSession
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        void BeginUnsafe(ref TKernelSession kernelSession);

        /// <summary>Leave (suspend) the epoch.</summary>
        void EndUnsafe(ref TKernelSession kernelSession);
    }

    /// <summary>
    /// Perform epoch safety operations when entering/exiting an API 
    /// </summary>
    public struct SafeEpochThread<TKernelSession> : IEpochThread<TKernelSession>
        where TKernelSession : IKernelSession
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public void BeginUnsafe(ref TKernelSession kernelSession) => kernelSession.BeginUnsafe();

        /// <summary>Leave (suspend) the epoch.</summary>
        public void EndUnsafe(ref TKernelSession kernelSession) => kernelSession.EndUnsafe();
    }

    /// <summary>
    /// Do not perform epoch safety operations when entering/exiting an API (assumes a higher code region has done so)
    /// </summary>
    public struct UnsafeEpochThread<TKernelSession> : IEpochThread<TKernelSession>
        where TKernelSession : IKernelSession
    {
        /// <summary>Acquire (resume) the epoch.</summary>
        public void BeginUnsafe(ref TKernelSession kernelSession) { }

        /// <summary>Leave (suspend) the epoch.</summary>
        public void EndUnsafe(ref TKernelSession kernelSession) { }
    }
}
