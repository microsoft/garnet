// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking
{
    /// <summary>
    /// Notified by a sender when send capacity returns after <see cref="INetworkSender.SendResponseNoWait"/> saturated it.
    /// </summary>
    public interface ISendCapacityWaiter
    {
        /// <summary>
        /// Called once when the sends outstanding drop below the throttle limit. Runs on whichever thread
        /// completes the send, which can be a network IO thread, so it must not block, and it must not throw:
        /// the waiters registered with the same sender are called one after another, and a throw would leave
        /// the rest uncalled.
        /// </summary>
        void OnSendCapacity();
    }
}