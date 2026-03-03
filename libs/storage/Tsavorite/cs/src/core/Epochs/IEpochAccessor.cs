// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// API to pulse epochs
    /// </summary>
    public interface IEpochAccessor
    {
        /// <summary>
        /// Try to suspend the epoch, if it is currently held. If the epoch is not currently held, 
        /// this method will return false and the caller should not attempt to resume the epoch.
        /// </summary>
        /// <returns></returns>
        bool TrySuspend();

        /// <summary>
        /// Resume holding the epoch. This should only be called if TrySuspend returned true.
        /// </summary>
        void Resume();
    }
}