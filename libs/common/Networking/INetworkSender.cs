// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.networking
{
    /// <summary>
    /// Interface for Network Sender
    /// </summary>
    public interface INetworkSender : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        MaxSizeSettings GetMaxSizeSettings { get; }

        /// <summary>
        /// Remote endpoint name
        /// </summary>
        string RemoteEndpointName { get; }

        /// <summary>
        /// Local endpoint name
        /// </summary>
        string LocalEndpointName { get; }

        /// <summary>
        /// Is the remote endpoint local?
        /// </summary>
        /// <returns></returns>
        bool IsLocalConnection();

        /// <summary>
        /// Enter exclusive use of network sender.
        /// </summary>
        void Enter();

        /// <summary>
        /// Enter exclusive use of network sender. Allocate and get response object pointers.
        /// </summary>
        /// <param name="head"></param>
        /// <param name="tail"></param>
        unsafe void EnterAndGetResponseObject(out byte* head, out byte* tail);

        /// <summary>
        /// Exit exclusive use of network sender.
        /// </summary>
        void Exit();

        /// <summary>
        /// Exit exclusive use of network sender. Free response object.
        /// </summary>
        void ExitAndReturnResponseObject();

        /// <summary>
        /// Allocate a new response object
        /// </summary>
        void GetResponseObject();

        /// <summary>
        /// Free response object
        /// </summary>
        void ReturnResponseObject();

        /// <summary>
        /// Get current response object head ptr;
        /// </summary>
        /// <returns></returns>
        unsafe byte* GetResponseObjectHead();

        /// <summary>
        /// Get current response object tail ptr;
        /// </summary>
        /// <returns></returns>
        unsafe byte* GetResponseObjectTail();

        /// <summary>
        /// Send payload stored at response object, from offset to offset + size
        /// </summary>
        /// <param name="offset">Offset of response from which to start sending</param>
        /// <param name="size">Number of bytes to send, starting from offset</param>
        /// <returns>Whether the send succeeded</returns>
        bool SendResponse(int offset, int size);

        /// <summary>
        /// Send payload stored at response object, from offset to offset + size, without ever waiting for send
        /// capacity. A caller that sends this way bounds what it has outstanding by checking the result and
        /// parking on <see cref="TryRegisterSendCapacityWaiter"/> before producing more output, which keeps a
        /// slow reader from holding the thread the send was issued from.
        /// </summary>
        /// <param name="offset">Offset of response from which to start sending</param>
        /// <param name="size">Number of bytes to send, starting from offset</param>
        /// <returns>Whether the sends outstanding have reached the throttle limit, so the caller should wait for capacity</returns>
        bool SendResponseNoWait(int offset, int size);

        /// <summary>
        /// Whether the sends issued with <see cref="SendResponseNoWait"/> that are still outstanding have reached the throttle limit
        /// </summary>
        bool IsSendSaturated { get; }

        /// <summary>
        /// Arranges for <paramref name="waiter"/> to be notified when the sends outstanding next drop below the
        /// throttle limit. A sender holds any number of waiters, because the session that owns the connection
        /// is not the only one that waits for it: a publisher waits for the subscribers it delivers to. Each
        /// waiter is notified once, and every waiter registered when capacity returns is notified, so a waiter
        /// that does not use the capacity cannot strand another.
        /// </summary>
        /// <param name="waiter">The waiter to notify</param>
        /// <returns>
        /// True if the waiter is registered and will be notified, false if capacity is already available, in
        /// which case the waiter will not be called
        /// </returns>
        bool TryRegisterSendCapacityWaiter(ISendCapacityWaiter waiter);

        /// <summary>
        /// Takes back a registration made with <see cref="TryRegisterSendCapacityWaiter"/> whose wait ended some
        /// other way, so a connection that never regains capacity does not keep the waiters of sessions that
        /// have gone.
        /// </summary>
        /// <param name="waiter">The waiter to take back</param>
        /// <returns>
        /// True if the waiter was still registered and will not be called, false if it had already been taken
        /// for notification or was not registered
        /// </returns>
        bool WithdrawSendCapacityWaiter(ISendCapacityWaiter waiter);

        /// <summary>
        /// Send response (caller owns buffer space)
        /// </summary>
        void SendResponse(byte[] buffer, int offset, int count, object context);

        /// <summary>
        /// Send response (caller owns buffer space)
        /// </summary>
        void SendCallback(object context);

        /// <summary>
        /// Dispose, optionally waiting for ongoing outgoing calls to complete
        /// </summary>
        void DisposeNetworkSender(bool waitForSendCompletion);

        /// <summary>
        /// Throttle mechanism for preventing too many sends outstanding (blocking)
        /// </summary>
        void Throttle();

        /// <summary>
        /// Forcibly close the underlying network connection.
        /// 
        /// Returns true if the caller is the first to successfully call this method.
        /// </summary>
        bool TryClose();
    }
}