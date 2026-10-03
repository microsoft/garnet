// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    /// <summary>
    /// TCP network handler
    /// </summary>
    /// <typeparam name="TServerHook"></typeparam>
    /// <typeparam name="TNetworkSender"></typeparam>
    public abstract class TcpNetworkHandlerBase<TServerHook, TNetworkSender> : NetworkHandler<TServerHook, TNetworkSender>, ISessionParkHost, IThreadPoolWorkItem
        where TServerHook : IServerHook
        where TNetworkSender : INetworkSender
    {
        readonly ILogger logger;
        readonly Socket socket;
        readonly string remoteEndpointName;
        readonly string localEndpointName;
        readonly bool isLocalConnection;
        int closeRequested;

        /// <summary>
        /// Receive state handed over while the session is parked, and picked back up by the resume.
        /// </summary>
        SocketAsyncEventArgs parkedReceiveArgs;

        /// <summary>
        /// Rendezvous between the two events that must both happen before a parked session can resume: the
        /// receive path handing off its receive state, and the blocking operation completing. Each increments
        /// this once, and whichever arrives second schedules the resume, so it runs exactly once and never
        /// before the receive stack has unwound.
        /// </summary>
        int parkRendezvous;

        /// <summary>
        /// Latched when the connection is terminally closed by a path that does not run handler teardown,
        /// so the park can reconcile it after publishing its state. See <see cref="AbortPark"/>.
        /// </summary>
        int parkAbortRequested;


        const int ParkRendezvousComplete = 2;

        /// <summary>
        /// Constructor
        /// </summary>
        public TcpNetworkHandlerBase(TServerHook serverHook, TNetworkSender networkSender, Socket socket, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null)
            : base(serverHook, networkSender, networkBufferSettings, networkPool, useTLS, messageConsumer: messageConsumer, logger: logger)
        {
            this.logger = logger;
            this.socket = socket;
            var remoteEndpoint = socket.RemoteEndPoint;
            this.remoteEndpointName = remoteEndpoint?.ToString() ?? string.Empty;
            this.localEndpointName = socket.LocalEndPoint?.ToString() ?? string.Empty;
            this.isLocalConnection = remoteEndpoint is IPEndPoint ip
                ? IPAddress.IsLoopback(ip.Address)
                : remoteEndpoint is UnixDomainSocketEndPoint;
            this.closeRequested = 0;

            AllocateNetworkReceiveBuffer();
        }

        /// <inheritdoc />
        public override string RemoteEndpointName => remoteEndpointName;

        /// <inheritdoc />
        public override string LocalEndpointName => localEndpointName;

        /// <inheritdoc />
        public override bool IsLocalConnection() => isLocalConnection;

        /// <inheritdoc />
        public override void Start(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Network_After_TcpNetworkHandlerBase_Start_Server);
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override Task StartAsync(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Network_After_TcpNetworkHandlerBase_Start_Server);
            return base.StartAsync(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override void Start(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            base.Start(tlsOptions, remoteEndpointName, token);
        }

        /// <inheritdoc />
        public override async Task StartAsync(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (token == default && cancellationTokenSource != null) token = cancellationTokenSource.Token;
            Start(tlsOptions != null);
            await base.StartAsync(tlsOptions, remoteEndpointName, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override bool TryClose()
        {
            // Only one caller gets to invoke Close, as we'd expect subsequent ones to fail and throw
            if (Interlocked.CompareExchange(ref closeRequested, 1, 0) != 0)
            {
                return false;
            }

            try
            {
                // This close should cause all outstanding requests to fail.
                // 
                // We don't distinguish between clients closing their end of the Socket
                // and us forcing it closed on request.
                socket.Close();
            }
            catch
            {
                // Best effort, just swallow any exceptions
            }

            return true;
        }

        /// <summary>
        /// Start the socket receive - after this call, NetworkHandler may get disposed at any time
        /// such as when the socket is disconnected.
        /// </summary>
        /// <param name="useTLS"></param>
        void Start(bool useTLS)
        {
            var receiveEventArgs = new SocketAsyncEventArgs { AcceptSocket = socket };
            receiveEventArgs.SetBuffer(networkReceiveBuffer, 0, networkReceiveBuffer.Length);
            receiveEventArgs.Completed += useTLS ? RecvEventArgCompletedWithTLS : RecvEventArgCompletedWithoutTLS;

            // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            try
            {
                if (!socket.ReceiveAsync(receiveEventArgs))
                {
                    if (useTLS)
                        _ = Task.Run(() => RecvEventArgCompletedWithTLS(null, receiveEventArgs));
                    else
                        _ = Task.Run(() => RecvEventArgCompletedWithoutTLS(null, receiveEventArgs));
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred at Start.ReceiveAsync");
                Dispose(receiveEventArgs);
            }
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            try
            {
                if (socket.Connected)
                {
                    // Gracefully shutdown the socket connection
                    socket.Shutdown(SocketShutdown.Both);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error shutting down socket");
            }
            finally
            {
                // Always close the socket to release the resources
                socket.Close();
                // Dispose of the socket to free up unmanaged resources
                socket.Dispose();
            }
            DisposeImpl();
        }

        /// <summary>
        /// Dispose resources - call ONLY if Start was not called on network handler
        /// </summary>
        public void DisposeResources()
        {
            DisposeImpl();
        }

        void Dispose(SocketAsyncEventArgs e)
        {
            try
            {
                if (e.AcceptSocket.Connected)
                {
                    e.AcceptSocket.Shutdown(SocketShutdown.Both);
                }
            }
            catch (Exception ex)
            {
                logger?.LogTrace(ex, "Error shutting down accept socket during SAEA dispose");
            }
            finally
            {
                e.AcceptSocket.Close();
                e.AcceptSocket.Dispose();
            }
            DisposeImpl();
            e.Dispose();
        }

        void RecvEventArgCompletedWithTLS(object sender, SocketAsyncEventArgs e) =>
            _ = ReceiveLoopWithTLSAsync(e, armParkOnPark: true);

        void RecvEventArgCompletedWithoutTLS(object sender, SocketAsyncEventArgs e) =>
            HandleReceiveWithoutTLS(sender, e);

        /// <inheritdoc />
        public bool CanParkSession => TransportSupportsParking;

        /// <inheritdoc />
        public void ParkSession()
        {
            parkRendezvous = 0;
            sessionParked = true;
        }

        /// <inheritdoc />
        public void UnparkSession()
        {
            if (Interlocked.Increment(ref parkRendezvous) == ParkRendezvousComplete)
                ScheduleResume();
        }

        /// <inheritdoc />
        public void AbortPark()
        {
            Interlocked.Exchange(ref parkAbortRequested, 1);
            AbortParkedSession();
        }

        /// <inheritdoc />
        public void ClosePark()
        {
            Interlocked.Exchange(ref parkAbortRequested, 1);

            // The same stand-in ArmPark makes when it observes a latched close, for the case where the
            // release is known to be missing rather than merely unobservable: the operation was consumed
            // before it started, so nothing else will ever contribute. Latching first is what makes this
            // safe -- the close is monotonic, so this park has no successor whose count a late contribution
            // could contaminate -- and only the increment landing exactly on the target schedules anything,
            // so racing ArmPark's own stand-in is harmless.
            if (Interlocked.Increment(ref parkRendezvous) == ParkRendezvousComplete)
                ScheduleResume();
        }

        /// <summary>
        /// Whether this connection has been terminally closed, by either handler teardown or an external
        /// close such as <c>CLIENT KILL</c>.
        /// </summary>
        bool ParkAbortPending => parkAbortRequested != 0 || IsDisposed || serverHook.Disposed;

        private void HandleReceiveWithoutTLS(object sender, SocketAsyncEventArgs e)
        {
            if (ReceiveLoopWithoutTLS(e))
                ArmPark(e);
        }

        /// <summary>
        /// Hands the connection's receive state to the park and arms the resume.
        /// </summary>
        /// <remarks>
        /// <paramref name="e"/> is exactly what the receive callback returned without: the accept socket and
        /// the pinned receive buffer still holding whatever the client pipelined behind the blocking command.
        /// Holding it on the handler rather than on the receive thread's stack is what lets that thread
        /// leave. Only the thread that owns message processing touches this field, and only while the session
        /// is parked, so it needs no synchronization.
        /// </remarks>
        void ArmPark(SocketAsyncEventArgs e)
        {
            parkedReceiveArgs = e;
            if (Interlocked.Increment(ref parkRendezvous) == ParkRendezvousComplete)
            {
                ScheduleResume();
                return;
            }

            // Closes the window where the connection was torn down between the session parking and the
            // receive state arriving here, which would otherwise hold that state until the operation finished
            // on its own. Every terminal close publishes its flag with an interlocked write before it reads
            // the park state, and the increment above is the matching barrier on this side, so at least one
            // of the two always observes the other. Both observing it is harmless: the context completes once.
            if (!ParkAbortPending)
                return;

            AbortParkedSession();

            // Stand in for the release unconditionally. Whether the abort above found an operation says
            // nothing about whether a release is coming: teardown can consume an outcome without releasing,
            // and a completion can hold a claim it has not published yet, and neither is distinguishable
            // from here. Standing in regardless is what keeps the receive state from being stranded, and it
            // is safe because only the increment landing exactly on the target schedules anything -- an
            // extra contribution is simply ignored. It is safe *here* specifically because the close that
            // got us here is latched and monotonic, so this park has no successor whose count a late
            // contribution could contaminate.
            if (Interlocked.Increment(ref parkRendezvous) == ParkRendezvousComplete)
                ScheduleResume();
        }

        /// <summary>
        /// Queues the resume onto the thread pool. Never runs it inline: the caller may be an unrelated
        /// session's thread, a timer thread, or the thread tearing this connection down, none of which may be
        /// taken over by this connection's work.
        /// </summary>
        void ScheduleResume() => ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        /// <summary>
        /// Records that a park's receive state -- the accept socket and the pinned receive buffer -- was
        /// taken back off the handler by a resume.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The counterpart to the release <see cref="ClosePark"/> stands in for. An abandoned park that
        /// never rendezvous never schedules the resume that reaches here, so a count that stops short of the
        /// number of parks torn down is the strand itself, observed rather than inferred.
        /// </para>
        /// <para>
        /// Counted at the handoff rather than at each disposal, because the resume can release the state
        /// through several routes: the two teardown checks around the resume body, the receive loop it
        /// re-enters when a read completes synchronously, and that loop's failure handler. Counting them
        /// individually makes the total depend on which route a given teardown happens to take. Once the
        /// field has been read and cleared the state is on this frame and the park cannot strand it, which
        /// is the property being counted.
        /// </para>
        /// </remarks>
        [Conditional("DEBUG")]
        static void NoteParkedReceiveStateReclaimed() => ParkDiagnostics.NoteReceiveStateReclaimed();

        /// <summary>
        /// Runs the resume for a session whose blocking operation has finished. Queued as a work item rather
        /// than allocated as a continuation, so a park costs nothing on the managed heap.
        /// </summary>
        void IThreadPoolWorkItem.Execute()
        {
            if (UsesTls)
            {
                // The TLS resume has to re-enter an async reader loop, so it cannot run on this frame. The
                // work item is still what got us onto the thread pool, so the park itself allocated nothing;
                // this path allocates a state machine per resume, as every TLS receive already does.
                _ = ResumeWithTLSAsync();
                return;
            }

            var e = parkedReceiveArgs;
            parkedReceiveArgs = null;
            NoteParkedReceiveStateReclaimed();

            // Claim the connection before reading its teardown state, so teardown either observes this and
            // leaves reclamation to us, or completes first and is observed below. Either way nothing is
            // freed while this thread is using it.
            AcquireResumeLease();
            var leaseHeld = true;

            try
            {
                if (ParkAbortPending)
                {
                    // Torn down while parked. The reply is moot; release the operation and the receive state.
                    DiscardParkedSession();
                    leaseHeld = false;
                    ReleaseResumeLease();
                    Dispose(e);
                    return;
                }

                // Emits the parked command's reply, then consumes commands already buffered behind it.
                OnNetworkResumeWithoutTLS();

                // A session may park again while resuming: a pipelined second blocking command, or one that
                // re-arms its own wait. Re-arming schedules another work item, so this never recurses.
                if (sessionParked)
                {
                    leaseHeld = false;
                    ReleaseResumeLease();
                    ArmPark(e);
                    return;
                }

                // Re-check before handing the buffer to the kernel: a close may have landed during the
                // resume above, and releasing the lease is what lets its deferred reclamation run.
                if (ParkAbortPending)
                {
                    leaseHeld = false;
                    ReleaseResumeLease();
                    Dispose(e);
                    return;
                }

                e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);

                if (e.AcceptSocket.ReceiveAsync(e))
                {
                    // Pending in the kernel. The connection is quiescent on this thread, so reclamation may
                    // proceed; the IO completion owns it from here.
                    leaseHeld = false;
                    ReleaseResumeLease();
                    return;
                }

                // Completed synchronously -- bytes were already buffered. Draining them reads the receive
                // buffer and runs the session, so the lease must still be held across it.
                var parkedAgain = ReceiveLoopWithoutTLS(e);

                // Released before re-arming: a park schedules the next resume, which takes the lease itself.
                leaseHeld = false;
                ReleaseResumeLease();

                if (parkedAgain)
                    ArmPark(e);
            }
            catch (Exception ex)
            {
                if (leaseHeld)
                {
                    leaseHeld = false;
                    ReleaseResumeLease();
                }
                HandleReceiveFailure(ex, e);
            }
            finally
            {
                if (leaseHeld)
                    ReleaseResumeLease();
            }
        }

        /// <summary>
        /// The TLS counterpart of <see cref="IThreadPoolWorkItem.Execute"/>. Same sequence, and the same
        /// lease discipline; it differs only in that re-entering the reader and draining a synchronous
        /// receive are both awaited.
        /// </summary>
        /// <remarks>
        /// Nothing awaits this. It is started from the work item that the park scheduled, and every failure
        /// path inside it ends in <see cref="HandleReceiveFailure"/>, so a fault cannot escape unobserved.
        /// </remarks>
        async ValueTask ResumeWithTLSAsync()
        {
            var e = parkedReceiveArgs;
            parkedReceiveArgs = null;
            NoteParkedReceiveStateReclaimed();

            AcquireResumeLease();
            var leaseHeld = true;

            try
            {
                if (ParkAbortPending)
                {
                    // Torn down while parked. The reply is moot; release the operation and the receive state.
                    DiscardParkedSession();
                    leaseHeld = false;
                    ReleaseResumeLease();
                    Dispose(e);
                    return;
                }

                // Emits the parked command's reply, then consumes commands already buffered behind it --
                // both the plaintext already decrypted and any ciphertext that arrived before the park.
                await OnNetworkResumeWithTLSAsync().ConfigureAwait(false);

                // A session may park again while resuming: a pipelined second blocking command, or one that
                // re-arms its own wait. Re-arming schedules another work item, so this never recurses.
                if (sessionParked)
                {
                    leaseHeld = false;
                    ReleaseResumeLease();
                    ArmPark(e);
                    return;
                }

                // Re-check before handing the buffer to the kernel: a close may have landed during the
                // resume above, and releasing the lease is what lets its deferred reclamation run.
                if (ParkAbortPending)
                {
                    leaseHeld = false;
                    ReleaseResumeLease();
                    Dispose(e);
                    return;
                }

                e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);

                if (e.AcceptSocket.ReceiveAsync(e))
                {
                    // Pending in the kernel. The connection is quiescent on this thread, so reclamation may
                    // proceed; the IO completion owns it from here.
                    leaseHeld = false;
                    ReleaseResumeLease();
                    return;
                }

                // Completed synchronously -- bytes were already buffered. Draining them reads the receive
                // buffer and runs the session, so the lease must still be held across it.
                var parkedAgain = await ReceiveLoopWithTLSAsync(e).ConfigureAwait(false);

                // Released before re-arming: a park schedules the next resume, which takes the lease itself.
                leaseHeld = false;
                ReleaseResumeLease();

                if (parkedAgain)
                    ArmPark(e);
            }
            catch (Exception ex)
            {
                if (leaseHeld)
                {
                    leaseHeld = false;
                    ReleaseResumeLease();
                }
                HandleReceiveFailure(ex, e);
            }
            finally
            {
                if (leaseHeld)
                    ReleaseResumeLease();
            }
        }

        /// <summary>
        /// Drains the socket into the session until the receive goes asynchronous, the connection is torn
        /// down, or the session parks.
        /// </summary>
        /// <returns>
        /// True if the loop stopped because the session parked, in which case the caller owns
        /// <paramref name="e"/> and must resume the session. False if there is nothing left to do on this
        /// thread.
        /// </returns>
        bool ReceiveLoopWithoutTLS(SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || serverHook.Disposed)
                    {
                        // No more things to receive
                        Dispose(e);
                        return false;
                    }
                    OnNetworkReceiveWithoutTLS(e.BytesTransferred);
                    if (sessionParked)
                        return true;
                    e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);
                } while (!e.AcceptSocket.ReceiveAsync(e));
                return false;
            }
            catch (Exception ex)
            {
                HandleReceiveFailure(ex, e);
                return false;
            }
        }

        /// <summary>
        /// The TLS counterpart of <see cref="ReceiveLoopWithoutTLS"/>, with the same contract: it drains the
        /// socket into the session until the receive goes asynchronous, the connection is torn down, or the
        /// session parks.
        /// </summary>
        /// <remarks>
        /// This is the receive path's only async frame, which is why it arms the park itself rather than
        /// reporting upwards and letting a wrapper do it: a wrapper would be a second state machine, and an
        /// ordinary TLS receive that suspends would then box twice where it used to box once. The resume
        /// path cannot use <paramref name="armParkOnPark"/> because it has to drop its lease between the two
        /// steps, so it passes false and arms the park itself.
        /// </remarks>
        /// <param name="e">Receive args owned by this loop.</param>
        /// <param name="armParkOnPark">Whether to arm the park here when the session parks.</param>
        /// <returns>
        /// True if the loop stopped because the session parked, in which case the caller owns
        /// <paramref name="e"/> and must resume the session. False if there is nothing left to do on this
        /// thread.
        /// </returns>
        async ValueTask<bool> ReceiveLoopWithTLSAsync(SocketAsyncEventArgs e, bool armParkOnPark = false)
        {
            try
            {
                do
                {
                    if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || serverHook.Disposed)
                    {
                        // No more things to receive
                        Dispose(e);
                        return false;
                    }
                    var receiveTask = OnNetworkReceiveWithTLSAsync(e.BytesTransferred);
                    if (!receiveTask.IsCompletedSuccessfully)
                    {
                        await receiveTask.ConfigureAwait(false);
                    }
                    if (sessionParked)
                    {
                        if (armParkOnPark)
                            ArmPark(e);
                        return true;
                    }
                    e.SetBuffer(networkReceiveBuffer, networkBytesRead, networkReceiveBuffer.Length - networkBytesRead);
                } while (!e.AcceptSocket.ReceiveAsync(e));
                return false;
            }
            catch (Exception ex)
            {
                HandleReceiveFailure(ex, e);
                return false;
            }
        }

        void HandleReceiveFailure(Exception ex, SocketAsyncEventArgs e)
        {
            if (ex is ObjectDisposedException ex2 && ex2.ObjectName == "System.Net.Sockets.Socket")
                logger?.LogTrace("Accept socket was disposed at RecvEventArg_Completed");
            else
                logger?.LogError(ex, "An error occurred at RecvEventArg_Completed");
            Dispose(e);
        }

        unsafe void AllocateNetworkReceiveBuffer()
        {
            networkReceiveBufferEntry = networkPool.Get(BaseReceiveBufferSize, PoolEntryBufferType.NetworkReceiveBuffer);
            networkReceiveBuffer = networkReceiveBufferEntry.entry;
            networkReceiveBufferPtr = networkReceiveBufferEntry.entryPtr;
        }
    }
}