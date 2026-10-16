// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// Set when the last reply the session handed to the socket left the sender at its limit of
        /// outstanding sends, and cleared once the session has waited that out. Checked before each command.
        /// </summary>
        /// <remarks>
        /// The session never waits for the socket inside a send, because a send runs on the thread that
        /// handles the connection's network callbacks, and a wait there ties that thread to a client that is
        /// slow to read. Instead the send reports that the limit was reached, and the session parks before
        /// it starts the next command. A command that overshoots the limit does so by at most its own reply.
        /// </remarks>
        bool sendSaturated;

        /// <summary>
        /// Reusable wait for a session parked until the socket takes the replies it has outstanding, released
        /// when this session is torn down.
        /// </summary>
        /// <remarks>
        /// Created on first use, and deliberately not disposed, for the same reasons as
        /// <see cref="SessionDelay"/>.
        /// </remarks>
        internal SendCapacityWait SendCapacityWait
        {
            get
            {
                var wait = sendCapacityWait;
                if (wait is null)
                {
                    wait = new SendCapacityWait();
                    var existing = Interlocked.CompareExchange(ref sendCapacityWait, wait, null);
                    if (existing is not null)
                        wait = existing;
                    else if (sessionTornDown)
                        wait.Cancel();
                }
                return wait;
            }
        }

        SendCapacityWait sendCapacityWait;

        /// <summary>
        /// Parks the session until the sender has capacity for more replies, when it has none, so a client that
        /// does not read cannot make the session queue replies without bound.
        /// </summary>
        /// <returns>True if the session parked, in which case the caller must stop processing the batch.</returns>
        /// <remarks>
        /// Called between commands, where the session holds no state the park would have to preserve: the
        /// per-batch scope is released for the wait and retaken on resume, and the rest of the batch is
        /// still in the receive buffer. A consume that cannot park does not wait here, and the replies it
        /// produces are bounded by the batch it was given.
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        bool ParkForSendCapacity()
        {
            if (!CanSuspend || !networkSender.IsSendSaturated)
            {
                sendSaturated = false;
                return false;
            }

            ValueTask body;
            using (BeginAsyncCommand())
                body = WaitForSendCapacityAsync();

            _ = CompleteAsyncCommand(body);
            return asyncSuspended;
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask WaitForSendCapacityAsync()
        {
            var wait = SendCapacityWait;

            // A completion that frees capacity can precede the registration, or be a stale one for an earlier
            // wait, so the state is re-read around every wait rather than trusted from the signal alone.
            while (networkSender.IsSendSaturated)
            {
                wait.Arm();
                if (!networkSender.TryRegisterSendCapacityWaiter(wait))
                    break;

                await wait.WaitAsync().ConfigureAwait(false);
            }

            sendSaturated = false;
        }
    }
}