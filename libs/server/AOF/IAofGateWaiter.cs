// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Waits for the AOF's replication-lag gate to admit appends again, after registering with
    /// <see cref="AofBackpressure.TryRegisterWaiter"/>.
    /// </summary>
    internal interface IAofGateWaiter
    {
        /// <summary>
        /// Called once per registration, from the gate's poller thread, when the gate admits appends again or the
        /// AOF shuts down. The waiter must not block and must not call back into the gate: it only has to arrange
        /// for its owner to run again.
        /// </summary>
        void OnGateOpen();
    }
}
