// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.networking
{
    /// <summary>
    /// Network handler
    /// </summary>
    public abstract partial class NetworkHandler<TServerHook, TNetworkSender> : NetworkSenderBase, INetworkHandler
        where TServerHook : IServerHook
        where TNetworkSender : INetworkSender
    {
        /// <summary>
        /// Server hook
        /// </summary>
        protected readonly TServerHook serverHook;

        /// <summary>
        /// Network buffer settings used to allocate send and receive buffers
        /// </summary>
        protected readonly NetworkBufferSettings networkBufferSettings;

        /// <summary>
        /// Process-wide buffer budget this connection's pool participates in. Cached rather than reached
        /// through <see cref="networkPool"/> because <see cref="BaseReceiveBufferSize"/> reads it on every
        /// receive.
        /// </summary>
        readonly NetworkBufferBudget budget;

        /// <summary>
        /// Configured base sizes, cached off <see cref="networkBufferSettings"/> for the same reason. Reaching
        /// through the settings object instead puts a dependent load on every receive, which measures ~2% on
        /// Network.BasicOperations.InlinePing.
        /// </summary>
        readonly int configuredReceiveBufferSize, configuredSendBufferSize;

        /// <summary>
        /// Size for a new TLS plaintext send buffer. Send buffers never grow -- an oversized response is
        /// chunked through whatever buffer it was given -- so the size is safe to adapt, and must be: the pool
        /// measures a returned entry of this type against the send target, so an unadapted allocation would be
        /// over-target under pressure, dropped on return, and freshly pinned on the next connect.
        /// </summary>
        protected int BaseSendBufferSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => budget.ClampSendBufferSize(configuredSendBufferSize);
        }

        /// <summary>
        /// Size a new receive buffer starts at, and the size a grown one shrinks back toward. This is the
        /// configured <see cref="NetworkBufferSettings.initialReceiveBufferSize"/> until the process-wide
        /// budget is under pressure, at which point it steps down toward the configured floor so that the
        /// aggregate across all connections stays near the budget.
        /// </summary>
        /// <remarks>
        /// Only the <em>base</em> size is governed. Demand-driven doubling is never clamped, so a connection
        /// that needs a large buffer still gets one; pressure changes what a connection starts and settles at,
        /// never what it is allowed to reach. When the budget is disabled this is exactly the configured size.
        /// </remarks>
        protected int BaseReceiveBufferSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => budget.ClampReceiveBufferSize(configuredReceiveBufferSize);
        }

        /// <summary>
        /// Network pool used to allocated send and receive buffers
        /// </summary>
        protected readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// Pool entry
        /// </summary>
        protected PoolEntry networkReceiveBufferEntry;

        /// <summary>
        /// Buffer that receives data directly from network
        /// This is allocated and populated by derived classes
        /// </summary>
        protected byte[] networkReceiveBuffer;

        /// <summary>
        /// Pointer to buffer that receives data directly from network
        /// This is allocated and populated by derived classes
        /// </summary>
        protected unsafe byte* networkReceiveBufferPtr;

        /// <summary>
        /// Bytes read and read head for network buffer
        /// </summary>
        protected int networkBytesRead, networkReadHead;

        /// <summary>
        /// Number of consecutive receives that must fit in a smaller buffer before a grown receive buffer is
        /// released back to the pool while the process is under no memory pressure. Generous, so a connection
        /// whose payloads are large but recurring keeps its buffer through the burst. Under pressure
        /// <see cref="PressureShrinkHysteresis"/> applies instead.
        /// </summary>
        const int ShrinkHysteresis = 256;

        /// <summary>
        /// Hysteresis applied instead of <see cref="ShrinkHysteresis"/> while the process-wide budget is
        /// binding. Pressure is sticky -- the target stays below the ceiling for as long as the connections
        /// are live -- so releasing on the first small receive would reallocate a pinned buffer on every large
        /// request of an alternating workload. A short countdown converges the aggregate quickly, and each
        /// large receive resets it, so an alternating workload never trips it.
        /// </summary>
        const int PressureShrinkHysteresis = 8;

        int networkShrinkCountdown = ShrinkHysteresis;
        int transportShrinkCountdown = ShrinkHysteresis;

        /// <summary>
        /// Buffer that application reads data from
        /// </summary>
        PoolEntry transportReceiveBufferEntry;
        /// <summary>
        /// Transport receive buffer
        /// </summary>
        protected byte[] transportReceiveBuffer;
        unsafe byte* transportReceiveBufferPtr;

        /// <summary>
        /// Bytes read by application from transport buffer
        /// </summary>
        int transportBytesRead, transportReadHead;

        /* Buffer that application writes data to */
        readonly PoolEntry transportSendBufferEntry;
        readonly byte[] transportSendBuffer;
        readonly unsafe byte* transportSendBufferPtr;

        /* Wrapper for buffer used to write directly to the network */
        readonly TNetworkSender networkSender;

        IMessageConsumer session;

        /// <summary>
        /// <see cref="session"/> when it supports parking, else null. Cached at bind time so the receive path
        /// never type-tests.
        /// </summary>
        IParkableMessageConsumer parkableSession;

        /// <summary>
        /// Set while the session is parked on a blocking operation. Only the thread that owns message
        /// processing touches it -- the receive thread when parking, the resume work item when unparking --
        /// and those two are ordered by the park rendezvous, so it needs no synchronization. Teardown runs on
        /// foreign threads and so must never read it; those paths go through the session's own interlocked
        /// claim on the parked operation instead.
        /// </summary>
        protected bool sessionParked;

        /// <summary>
        /// Guards connection-owned state against being reclaimed while a parked session's resume is using it.
        /// The low bits count the resumes currently holding it; <see cref="ResumeLeaseReclaimDeferred"/>
        /// records that teardown arrived while at least one did, and handed reclamation to the last one out.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Without this, teardown and resume race: the resume passes its disposed check, teardown frees the
        /// receive buffer entry and disposes the session, and the resume then reads freed memory. Notification
        /// ordering alone cannot close that window -- any check the resume makes is check-then-act -- so
        /// reclamation itself has to be deferred. Only the park paths touch this, so a connection that never
        /// parks pays nothing.
        /// </para>
        /// <para>
        /// It counts rather than flags because two resumes can overlap. A resume that hands its buffer to an
        /// asynchronous receive still has to drop the lease afterwards, and that receive may complete, park,
        /// and schedule the next resume in between. A flag would let the first resume's release free a lease
        /// the second is relying on, and teardown would then reclaim underneath it.
        /// </para>
        /// </remarks>
        int resumeLease;

        /// <summary>
        /// Set when teardown found the lease held. The last resume to leave performs the reclamation.
        /// </summary>
        const int ResumeLeaseReclaimDeferred = 1 << 30;

        /// <summary>
        /// Covers the resume-owner count, excluding <see cref="ResumeLeaseReclaimDeferred"/>.
        /// </summary>
        const int ResumeLeaseOwnerMask = ResumeLeaseReclaimDeferred - 1;

        /// <summary>
        /// Whether this transport could suspend its receive loop for the session it is bound to. Only
        /// handlers that implement <see cref="ISessionParkHost"/> can act on it.
        /// </summary>
        /// <remarks>
        /// The test is on the session rather than the handler because the only reader is
        /// <c>TcpNetworkHandlerBase.CanParkSession</c>, and that type is itself an
        /// <see cref="ISessionParkHost"/>, so the two coincide there. A handler that is *not* a park host
        /// leaves the session's own <c>parkHost</c> null (see <see cref="BindParkableSession"/>), and the
        /// session refuses to park on that ground instead -- which is what keeps the embedded handler used by
        /// the benchmarks off this path. A new handler type that reads this property without implementing
        /// <see cref="ISessionParkHost"/> would be reading it for a question it does not answer.
        /// </remarks>
        protected bool TransportSupportsParking => parkableSession != null;

        /// <summary>
        /// Whether this connection runs the TLS receive path, which parks and resumes through the reader
        /// loop rather than through the synchronous receive.
        /// </summary>
        protected bool UsesTls => sslStream != null;

        /// <summary>
        /// Carries the reader's <c>retry</c> flag across a park. The flag means "the transport buffer was
        /// just doubled, read again into the enlarged buffer" and is the reader's only record that more
        /// plaintext is available without more ciphertext arriving. A park that dropped it would leave the
        /// decrypted remainder stuck in the transport buffer until the client happened to send more.
        /// </summary>
        bool tlsReaderRetryAfterPark;

        /// <inheritdoc />
        public IMessageConsumer Session => session;

        readonly ILogger logger;

        /* TLS related fields */
        readonly SslStream sslStream;
        readonly SemaphoreSlim receivedData, expectingData;
        protected readonly CancellationTokenSource cancellationTokenSource;

        // Stream reader status: Rest = 0, Active = 1, Waiting = 2
        volatile TlsReaderStatus readerStatus;

        // Number of times Dispose has been called
        int disposeCount;

        /// <summary>
        /// Constructor
        /// </summary>
        public unsafe NetworkHandler(TServerHook serverHook, TNetworkSender networkSender, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null)
            : base(networkPool.MinAllocationSize)
        {
            this.logger = logger;
            this.serverHook = serverHook;
            this.networkSender = networkSender;
            this.session = messageConsumer;
            this.readerStatus = TlsReaderStatus.Rest;
            this.networkBufferSettings = networkBufferSettings;
            this.networkPool = networkPool;
            this.budget = networkPool.Budget;
            this.configuredReceiveBufferSize = networkBufferSettings.initialReceiveBufferSize;
            this.configuredSendBufferSize = networkBufferSettings.sendBufferSize;

            if (!useTLS)
            {
                sslStream = null;
                transportReceiveBuffer = networkReceiveBuffer;
                transportReceiveBufferPtr = networkReceiveBufferPtr;
            }
            else
            {
                // TLS mode, we start in active reader status to handle authentication phase
                readerStatus = TlsReaderStatus.Active;

                sslStream = new SslStream(new NetworkHandlerStream(this, logger));

                receivedData = new SemaphoreSlim(0);
                expectingData = new SemaphoreSlim(0);
                cancellationTokenSource = new();

                transportReceiveBufferEntry = this.networkPool.Get(BaseReceiveBufferSize, PoolEntryBufferType.TransportReceiveBuffer);
                transportReceiveBuffer = transportReceiveBufferEntry.entry;
                transportReceiveBufferPtr = transportReceiveBufferEntry.entryPtr;

                transportSendBufferEntry = this.networkPool.Get(BaseSendBufferSize, PoolEntryBufferType.TransportSendBuffer);
                transportSendBuffer = transportSendBufferEntry.entry;
                transportSendBufferPtr = transportSendBufferEntry.entryPtr;
            }

            BindParkableSession();
        }

        /// <summary>
        /// Begin (background) network handler.
        /// 
        /// Blocks until auth completes.
        /// </summary>
        public virtual void Start(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslServerAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslServerAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;

            // Can't use SslStream's sync methods for auth, so we must block
            AsyncUtils.BlockingWait(AuthenticateAsServerAsync(tlsOptions, remoteEndpointName, token));
        }

        /// <summary>
        /// Begin async network handler.
        /// </summary>
        public virtual async Task StartAsync(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslServerAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslServerAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;

            await AuthenticateAsServerAsync(tlsOptions, remoteEndpointName, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Async (background) authentication of TLS as server
        /// </summary>
        /// <param name="tlsOptions"></param>
        /// <param name="remoteEndpointName"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        async Task AuthenticateAsServerAsync(SslServerAuthenticationOptions tlsOptions, string remoteEndpointName, CancellationToken token = default)
        {
            Debug.Assert(readerStatus == TlsReaderStatus.Active);
            try
            {
                await sslStream.AuthenticateAsServerAsync(tlsOptions, token).ConfigureAwait(false);

                if (token.IsCancellationRequested) throw new TaskCanceledException("AuthenticateAsServerAsync was cancelled");

                logger?.LogDebug("Completed server TLS authentication for {remoteEndpoint}", remoteEndpointName);
                // Display the properties and settings for the authenticated stream.
                if (logger != null && logger.IsEnabled(LogLevel.Trace))
                    LogSecurityInfo(sslStream, remoteEndpointName, logger);

                // There may be extra bytes left over after auth, we need to process them (non-blocking) before returning
                var result = sslStream.ReadAsync(new Memory<byte>(transportReceiveBuffer, transportBytesRead, transportReceiveBuffer.Length - transportBytesRead), cancellationTokenSource.Token);
                _ = SslReaderAsync(result.AsTask(), cancellationTokenSource.Token);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error has occurred");
                readerStatus = TlsReaderStatus.Rest;
                if (expectingData.CurrentCount == 0) expectingData.Release();
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Begin (background) network handler.
        /// 
        /// Blocks until auth completes.
        /// </summary>
        public virtual void Start(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslClientAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslClientAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;

            // Can't use SslStream's sync methods for auth, so we must block
            AsyncUtils.BlockingWait(AuthenticateAsClientAsync(tlsOptions, remoteEndpointName, token));
        }

        /// <summary>
        /// Begin async network handler (including auth).
        /// 
        /// When tasks completes, authentication has also completed.
        /// </summary>
        public virtual async Task StartAsync(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslClientAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslClientAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;

            await AuthenticateAsClientAsync(tlsOptions, remoteEndpointName, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Authenticate TLS as client, update authState when done
        /// </summary>
        async Task AuthenticateAsClientAsync(SslClientAuthenticationOptions sslClientOptions, string remoteEndpointName, CancellationToken token)
        {
            Debug.Assert(readerStatus == TlsReaderStatus.Active);
            try
            {
                await sslStream.AuthenticateAsClientAsync(sslClientOptions, token).ConfigureAwait(false);

                if (token.IsCancellationRequested) throw new TaskCanceledException("AuthenticateAsClientAsync was cancelled");

                logger?.LogDebug("Completed client TLS authentication for {remoteEndpoint}", remoteEndpointName);
                // Display the properties and settings for the authenticated stream.
                if (logger != null && logger.IsEnabled(LogLevel.Trace))
                    LogSecurityInfo(sslStream, remoteEndpointName, logger);

                // There may be extra bytes left over after auth, we need to process them (non-blocking) before returning
                var result = sslStream.ReadAsync(new Memory<byte>(transportReceiveBuffer, transportBytesRead, transportReceiveBuffer.Length - transportBytesRead), cancellationTokenSource.Token);
                _ = SslReaderAsync(result.AsTask(), cancellationTokenSource.Token);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error has occurred");
                readerStatus = TlsReaderStatus.Rest;
                if (expectingData.CurrentCount == 0) expectingData.Release();
                Dispose();
                throw;
            }
        }

        public unsafe void OnNetworkReceiveWithoutTLS(int bytesTransferred)
        {
            networkBytesRead += bytesTransferred;
            transportReceiveBuffer = networkReceiveBuffer;
            transportReceiveBufferPtr = networkReceiveBufferPtr;
            transportBytesRead = networkBytesRead;

            // Occupancy before processing is the capacity this pass needed. Process consumes and compacts the
            // buffer in place, so sampling afterwards reads a fully consumed request as a zero-byte one.
            var demand = networkBytesRead;

            // Process non-TLS code on the synchronous thread
            Process();

            EndTransformNetworkToTransport();
            UpdateNetworkBuffers(demand);
        }

        /// <summary>
        /// Resumes a parked session on the non-TLS path: lets it emit the reply for the command that blocked
        /// and then consume whatever pipelined bytes were already buffered when it parked. No bytes have been
        /// received since the park, so this is the receive pass that never happened.
        /// </summary>
        protected unsafe void OnNetworkResumeWithoutTLS()
        {
            sessionParked = false;

            transportReceiveBuffer = networkReceiveBuffer;
            transportReceiveBufferPtr = networkReceiveBufferPtr;
            transportBytesRead = networkBytesRead;

            var demand = networkBytesRead;

            transportReadHead += parkableSession.ResumeParkedMessages(
                transportReceiveBufferPtr + transportReadHead, transportBytesRead - transportReadHead);
            ShiftTransportReceiveBuffer();

            EndTransformNetworkToTransport();
            UpdateNetworkBuffers(demand);
        }

        /// <summary>
        /// Resumes a parked session on the TLS path. The reply and the commands pipelined behind it are
        /// already decrypted and sitting in the transport buffer, so they are drained first; only then does
        /// the reader go back for ciphertext that arrived before the park.
        /// </summary>
        /// <remarks>
        /// The two buffers are distinct under TLS, which is the whole difference from the plaintext resume.
        /// Draining the transport buffer cannot be folded into the reader loop, because that loop is driven
        /// by unconsumed *ciphertext* and there may be none: a client that pipelined two commands inside one
        /// TLS record leaves the second as plaintext with no ciphertext behind it, and a reader-only resume
        /// would never look at it.
        /// <para>
        /// Re-entering the reader is the ordinary <see cref="TlsReaderStatus.Rest"/> entry, so the status
        /// protocol and the <c>expectingData</c> handshake are exactly those of a normal receive pass.
        /// </para>
        /// </remarks>
        protected async ValueTask OnNetworkResumeWithTLSAsync()
        {
            // Sampled before anything is consumed, for the same reason the receive path samples it there.
            var demand = networkBytesRead;

            DrainParkedTransportMessages();

            // A second blocking command pipelined behind the first parks again right here, with ciphertext
            // still unread. Leaving it unread is correct: it stays in the network buffer until this session
            // resumes again.
            if (!sessionParked && (networkBytesRead > networkReadHead || tlsReaderRetryAfterPark))
            {
                // Consume the flag before the call, not after. Read may re-park synchronously and publish a
                // fresh value on the way out, and may also hand off to SslReaderAsync and publish from there
                // after it has returned; clearing afterwards would drop either one and lose a transport
                // buffer that still needs to grow.
                var retryAfterPark = tlsReaderRetryAfterPark;
                tlsReaderRetryAfterPark = false;

                readerStatus = TlsReaderStatus.Active;
                Read(retryAfterPark);
                while (readerStatus == TlsReaderStatus.Active)
                    await expectingData.WaitAsync(cancellationTokenSource.Token).ConfigureAwait(false);
            }

            UpdateNetworkBuffers(demand);
        }

        /// <summary>
        /// Emits the parked command's reply and consumes whatever was already decrypted behind it. Separate
        /// from <see cref="OnNetworkResumeWithTLSAsync"/> only because a pointer cannot be taken in an
        /// <see langword="async"/> method.
        /// </summary>
        unsafe void DrainParkedTransportMessages()
        {
            sessionParked = false;

            transportReadHead += parkableSession.ResumeParkedMessages(
                transportReceiveBufferPtr + transportReadHead, transportBytesRead - transportReadHead);
            ShiftTransportReceiveBuffer();
        }

        /// <summary>
        /// Whether <see cref="DisposeImpl"/> has been entered. The resume path checks this after waking so it
        /// discards a reply for a connection that was torn down while parked.
        /// </summary>
        protected bool IsDisposed => disposeCount > 0;

        /// <summary>
        /// Abandons a parked operation so this connection's resume path runs now rather than whenever the
        /// operation would have finished on its own. Called when the connection is closed out from under a
        /// parked session, which would otherwise keep its receive state -- including the
        /// <c>SocketAsyncEventArgs</c> the resume path is holding -- alive for the rest of the wait.
        /// </summary>
        /// <remarks>
        /// Best-effort, and says nothing about whether a release is coming: see
        /// <see cref="IParkableMessageConsumer.AbortParkedOperation"/>.
        /// </remarks>
        protected void AbortParkedSession()
        {
            if (parkableSession == null)
                return;

            try
            {
                parkableSession.AbortParkedOperation();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Error aborting parked operation");
            }
        }

        /// <summary>
        /// Takes the lease that keeps teardown from reclaiming connection state underneath a resume. Must be
        /// paired with <see cref="ReleaseResumeLease"/> on every path, and callers must re-check
        /// <see cref="IsDisposed"/> *after* it returns: teardown publishes <c>disposeCount</c> before reading
        /// the lease, and this publishes the lease before the caller reads <c>disposeCount</c>, so at least
        /// one of the two sees the other.
        /// </summary>
        protected void AcquireResumeLease()
            => Interlocked.Increment(ref resumeLease);

        /// <summary>
        /// Drops the resume lease, performing the reclamation teardown skipped if it ran while the lease was
        /// held.
        /// </summary>
        protected void ReleaseResumeLease()
        {
            // Reclaim only when this was the last owner out and teardown left the work behind. The exchange
            // to zero claims it, so overlapping releases cannot both reclaim.
            if (Interlocked.Decrement(ref resumeLease) == ResumeLeaseReclaimDeferred &&
                Interlocked.CompareExchange(ref resumeLease, 0, ResumeLeaseReclaimDeferred) == ResumeLeaseReclaimDeferred)
                ReclaimConnectionResources();
        }

        /// <summary>
        /// Releases the parked operation's state after the resume observes that the connection is gone. The
        /// session has normally released it already by then; this covers a teardown that raced the park and
        /// so ran before the session had anything to release.
        /// </summary>
        protected void DiscardParkedSession()
        {
            if (parkableSession == null)
                return;

            try
            {
                parkableSession.DiscardParkedOperation();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Error discarding parked operation");
            }
        }

        /// <summary>
        /// Caches the parking interface for <see cref="session"/>, if it implements one. Called once per
        /// session, never from the receive path.
        /// </summary>
        void BindParkableSession()
        {
            parkableSession = session as IParkableMessageConsumer;

            // Null for handlers that cannot suspend a receive loop, which is what keeps a session from
            // parking against one.
            parkableSession?.SetParkHost(this as ISessionParkHost);
        }

        /// <summary>
        /// On network receive
        /// </summary>
        /// <param name="bytesTransferred">Number of bytes transferred</param>
        public async ValueTask OnNetworkReceiveWithTLSAsync(int bytesTransferred)
        {
            // Wait for SslStream async processing to complete, if any (e.g., authentication phase)
            while (readerStatus == TlsReaderStatus.Active)
                await expectingData.WaitAsync(cancellationTokenSource.Token).ConfigureAwait(false);

            // Increment network bytes read
            networkBytesRead += bytesTransferred;

            // Occupancy before the reader consumes ciphertext is the capacity this pass needed.
            var demand = networkBytesRead;

            switch (readerStatus)
            {
                case TlsReaderStatus.Rest:
                    readerStatus = TlsReaderStatus.Active;
                    Read();
                    while (readerStatus == TlsReaderStatus.Active)
                        await expectingData.WaitAsync(cancellationTokenSource.Token).ConfigureAwait(false);
                    break;
                case TlsReaderStatus.Waiting:
                    // We have a ReadAsync task waiting for new data, set it to active status
                    readerStatus = TlsReaderStatus.Active;

                    // Unblock the asynchronous ReadAsync task
                    _ = receivedData.Release();

                    while (readerStatus == TlsReaderStatus.Active)
                        await expectingData.WaitAsync(cancellationTokenSource.Token).ConfigureAwait(false);
                    break;
                default:
                    ThrowInvalidOperationException($"Unexpected reader status {readerStatus}");
                    break;
            }

            Debug.Assert(readerStatus != TlsReaderStatus.Active);
            UpdateNetworkBuffers(demand);
        }

        /// <param name="demand">
        /// Bytes the receive buffer held for this pass, sampled before any of them were consumed. This is the
        /// capacity the traffic needed; the unconsumed remainder left afterwards is not.
        /// </param>
        void UpdateNetworkBuffers(int demand)
        {
            // Shift network buffer after processing is done
            if (networkReadHead > 0)
                ShiftNetworkReceiveBuffer();

            // Double network buffer if out of space after processing is complete
            if (networkBytesRead == networkReceiveBuffer.Length)
            {
                DoubleNetworkReceiveBuffer();
                networkShrinkCountdown = ShrinkHysteresis;
            }
            else if (networkReceiveBuffer.Length > BaseReceiveBufferSize)
            {
                // Guarded here rather than inside the callee so a buffer still at its base size -- every pass
                // on a connection that has never grown -- costs one comparison and makes no call.
                TryShrinkNetworkReceiveBuffer(demand);
            }
        }

        /// <summary>
        /// Decides whether a grown receive buffer should be released, and what size should replace it. The
        /// network and transport buffers run the same policy over their own occupancy and countdown, so both
        /// go through here.
        /// </summary>
        /// <remarks>
        /// Three regimes, checked cheapest first. A buffer larger than
        /// <see cref="NetworkBufferSettings.maxReceiveBufferSize"/> is released immediately because the pool
        /// cannot recycle it and it would otherwise be pinned indefinitely by an idle connection. While the
        /// process-wide budget is binding, shrinking is immediate so the aggregate converges rather than
        /// waiting out a per-connection countdown. Otherwise the buffer is kept until
        /// <see cref="ShrinkHysteresis"/> consecutive receives have fit comfortably in the smaller size.
        /// <para>
        /// The countdown bookkeeping and the budget's shrink accounting both happen here, leaving a caller
        /// with nothing to do but swap the buffer.
        /// </para>
        /// </remarks>
        /// <param name="current">Size of the buffer being considered.</param>
        /// <param name="buffered">Bytes still buffered, which the replacement has to be able to hold.</param>
        /// <param name="demand">
        /// Bytes the buffer held for this pass, sampled before consumed bytes were shifted away. This is the
        /// capacity the traffic needed; the residual left after processing is not.
        /// </param>
        /// <param name="countdown">The caller's shrink hysteresis countdown, updated in place.</param>
        /// <param name="target">Size to replace the buffer with, when this returns <see langword="true"/>.</param>
        /// <returns><see langword="true"/> when the caller should replace its buffer with a <paramref name="target"/>-byte one.</returns>
        bool TryPlanReceiveBufferShrink(int current, int buffered, int demand, ref int countdown, out int target)
        {
            var baseSize = BaseReceiveBufferSize;
            target = current;
            if (current <= baseSize)
                return false;

            if (current > networkBufferSettings.maxReceiveBufferSize)
            {
                // Above the pool's largest size class, so this buffer cannot be recycled on return. Release it
                // without waiting out the hysteresis, sized to what is still buffered rather than to the
                // pass's demand, which is already consumed.
                target = TargetReceiveBufferSize(buffered, baseSize, current);

                // The floor applies only while the budget is slack, where it saves a connection that needs the
                // capacity on every request from re-growing through every size class. While the budget is
                // binding, a buffer that size is above the adapted target, so the pool discards it on return
                // and the pressure countdown shrinks it out again within a few receives.
                if (!budget.IsUnderPressure)
                    target = Math.Max(networkBufferSettings.maxReceiveBufferSize, target);

                countdown = ShrinkHysteresis;
                if (target >= current)
                    return false;

                budget.RecordIdleShrink();
                return true;
            }

            target = TargetReceiveBufferSize(demand, baseSize, current);
            if (target >= current)
            {
                countdown = ShrinkHysteresis;
                return false;
            }

            var underPressure = budget.IsUnderPressure;
            // Clamp rather than reload, so pressure arriving mid-countdown converges promptly instead of
            // waiting out however much of the idle countdown was left.
            if (underPressure && countdown > PressureShrinkHysteresis)
                countdown = PressureShrinkHysteresis;

            if (--countdown > 0)
                return false;

            countdown = ShrinkHysteresis;
            if (underPressure)
                budget.RecordPressureShrink();
            else
                budget.RecordIdleShrink();
            return true;
        }

        /// <summary>
        /// Releases a grown network receive buffer once the connection's traffic no longer needs it, so that a
        /// single large payload does not permanently inflate the per-connection footprint. See
        /// <see cref="TryPlanReceiveBufferShrink"/> for the policy.
        /// </summary>
        /// <param name="demand">
        /// Bytes the buffer held for this pass, sampled before consumed bytes were shifted away. This is the
        /// capacity the traffic needed; the residual left after processing is not.
        /// </param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void TryShrinkNetworkReceiveBuffer(int demand)
        {
            if (TryPlanReceiveBufferShrink(networkReceiveBuffer.Length, networkBytesRead, demand, ref networkShrinkCountdown, out var target))
                ShrinkNetworkReceiveBuffer(target);
        }

        /// <summary>
        /// Smallest power-of-two size, at least <paramref name="minSize"/> and at most <paramref name="currentSize"/>,
        /// that leaves 2x headroom over the bytes currently buffered.
        /// </summary>
        static int TargetReceiveBufferSize(int bytesBuffered, int minSize, int currentSize)
        {
            var target = (long)minSize;
            var needed = 2L * bytesBuffered;
            while (target < currentSize && target < needed)
                target <<= 1;
            return (int)Math.Min(target, currentSize);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowInvalidOperationException(string message)
            => throw new InvalidOperationException(message);

        unsafe void Process()
        {
            if (transportBytesRead > 0)
            {
                if (session != null || TryCreateSession())
                    TryProcessRequest();
            }
        }

        unsafe bool TryCreateSession()
        {
            if (!serverHook.TryCreateMessageConsumer(new Span<byte>(transportReceiveBufferPtr, transportBytesRead), GetNetworkSender(), out session))
                return false;

            BindParkableSession();
            return true;
        }

        /// <summary>
        /// Get network sender for this handler
        /// </summary>
        public INetworkSender GetNetworkSender() => sslStream == null ? networkSender : this;

        void Read(bool initialRetry = false)
        {
            bool retry = initialRetry;

            // Stops on a park so no further ciphertext is decrypted and handed to the session: the parked
            // command's reply has to reach the wire before anything pipelined behind it is even parsed.
            while ((networkBytesRead > networkReadHead || retry) && !sessionParked)
            {
                retry = false;
                var result = sslStream.ReadAsync(new Memory<byte>(transportReceiveBuffer, transportBytesRead, transportReceiveBuffer.Length - transportBytesRead), cancellationTokenSource.Token);
                if (result.IsCompletedSuccessfully)
                {
                    // blocking is unavoidable here, but safe since we've checked IsCompletedSuccessfully
                    transportBytesRead += AsyncUtils.BlockingWait(result);

                    // Occupancy before processing is the capacity this pass needed; the residual after it is not.
                    var transportDemand = transportBytesRead;

                    // Read task has control, process the decrypted transport bytes
                    Process();

                    // Shift bytes in transport buffer
                    if (transportReadHead > 0)
                        ShiftTransportReceiveBuffer();

                    // Double the transport buffer if needed
                    if (transportBytesRead == transportReceiveBuffer.Length)
                    {
                        DoubleTransportReceiveBuffer();
                        retry = true;
                    }
                    else
                    {
                        TryShrinkTransportReceiveBuffer(transportDemand);
                    }
                }
                else
                {
                    // Rare case: Our read has gone async, we need to invoke the async read processing code.
                    _ = SslReaderAsync(result.AsTask(), cancellationTokenSource.Token);
                    return;
                }
            }
            if (sessionParked)
                tlsReaderRetryAfterPark = retry;
            readerStatus = TlsReaderStatus.Rest;
            // We do not release expectingData here because it is the synchronous code path (i.e., there is no waiter)
        }

        async Task SslReaderAsync(Task<int> readTask, CancellationToken token = default)
        {
            try
            {
                bool retry = false;
                int count = await readTask.ConfigureAwait(false);

                Debug.Assert(readerStatus == TlsReaderStatus.Active);

                transportBytesRead += count;

                // Occupancy before processing is the capacity this pass needed; the residual after it is not.
                var transportDemand = transportBytesRead;

                // Read task has control, process the decrypted transport bytes
                Process();

                // Shift bytes in transport buffer, Process would not have shifted
                // as we are in active state
                if (transportReadHead > 0)
                    ShiftTransportReceiveBuffer();

                // Double the transport buffer if needed
                if (transportBytesRead == transportReceiveBuffer.Length)
                {
                    DoubleTransportReceiveBuffer();
                    retry = true;
                }
                else
                {
                    TryShrinkTransportReceiveBuffer(transportDemand);
                }
                // If more work, passthrough to the general SslReaderAsync, else this task is done.
                // NOTE: we must propagate the `retry` flag (which signals "the transport buffer was just doubled,
                // attempt another read into the freshly-enlarged buffer"). If we chained without it, the new
                // SslReaderLoopAsync would start with retry=false and, when networkBytesRead==networkReadHead,
                // exit its loop immediately without ever issuing the follow-up read, leaving the half-parsed
                // payload stuck in the transport buffer until more network bytes happen to arrive.
                if (!sessionParked && (networkBytesRead > networkReadHead || retry))
                {
                    _ = SslReaderLoopAsync(retry, token);
                }
                else
                {
                    if (sessionParked)
                        tlsReaderRetryAfterPark = retry;
                    readerStatus = TlsReaderStatus.Rest;
                    if (expectingData.CurrentCount == 0) expectingData.Release();
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception has occurred during NetworkHandler.SslReaderAsync(Task)");
                readerStatus = TlsReaderStatus.Rest;
                if (expectingData.CurrentCount == 0) expectingData.Release();
                Dispose();
            }
        }

        async Task SslReaderLoopAsync(bool initialRetry, CancellationToken token = default)
        {
            Debug.Assert(readerStatus == TlsReaderStatus.Active);

            try
            {
                bool retry = initialRetry;
                while ((networkBytesRead > networkReadHead || retry) && !sessionParked)
                {
                    retry = false;
                    Debug.Assert(readerStatus == TlsReaderStatus.Active);
                    int count = await sslStream.ReadAsync(new Memory<byte>(transportReceiveBuffer, transportBytesRead, transportReceiveBuffer.Length - transportBytesRead), token).ConfigureAwait(false);
                    Debug.Assert(readerStatus == TlsReaderStatus.Active);

                    transportBytesRead += count;

                    // Occupancy before processing is the capacity this pass needed; the residual after it is not.
                    var transportDemand = transportBytesRead;

                    // Read task has control, process the decrypted transport bytes
                    Process();

                    // Shift bytes in transport buffer, Process would not have shifted
                    // as we are in active state
                    if (transportReadHead > 0)
                        ShiftTransportReceiveBuffer();

                    // Double the transport buffer if needed
                    if (transportBytesRead == transportReceiveBuffer.Length)
                    {
                        DoubleTransportReceiveBuffer();
                        retry = true;
                    }
                    else
                    {
                        TryShrinkTransportReceiveBuffer(transportDemand);
                    }
                }

                // Normal exit, or a park: either way hand control back to OnNetworkReceiveWithTLSAsync.
                if (sessionParked)
                    tlsReaderRetryAfterPark = retry;
                readerStatus = TlsReaderStatus.Rest;
                if (expectingData.CurrentCount == 0) expectingData.Release();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception has occurred during SslReaderLoopAsync");
                // Wake the receive-loop waiter BEFORE Dispose() tears down the semaphore.
                readerStatus = TlsReaderStatus.Rest;
                if (expectingData.CurrentCount == 0) expectingData.Release();
                Dispose();
            }
        }

        unsafe void EndTransformNetworkToTransport()
        {
            // With non-TLS logic, network buffer is set back to the transport buffer after processing
            if (sslStream == null)
            {
                networkBytesRead = transportBytesRead;
            }
        }

        unsafe bool TryProcessRequest()
        {
            transportReadHead += session.TryConsumeMessages(transportReceiveBufferPtr + transportReadHead, transportBytesRead - transportReadHead);

            // We cannot shift or double transport buffer if a read may be waiting on
            // the old transport buffer and offset.
            if (readerStatus == TlsReaderStatus.Rest)
            {
                ShiftTransportReceiveBuffer();
            }
            return true;
        }

        unsafe void DoubleNetworkReceiveBuffer()
        {
            var tmp = networkPool.Get(networkReceiveBuffer.Length * 2, PoolEntryBufferType.DoubleNetworkReceiveBuffer);
            Array.Copy(networkReceiveBuffer, tmp.entry, networkReceiveBuffer.Length);
            networkReceiveBufferEntry.Dispose();
            networkReceiveBufferEntry = tmp;
            networkReceiveBuffer = tmp.entry;
            networkReceiveBufferPtr = tmp.entryPtr;
            RefreshNonTlsTransportAlias();
        }

        /// <summary>
        /// Without TLS the transport buffer is the network buffer. The aliases are refreshed at the top of
        /// every receive, but a resize leaves them pointing at the entry that was just returned to the pool --
        /// which both keeps the old pinned array rooted until the next receive and leaves a stale pointer into
        /// memory another connection may already have been handed.
        /// </summary>
        unsafe void RefreshNonTlsTransportAlias()
        {
            if (sslStream != null)
                return;

            transportReceiveBuffer = networkReceiveBuffer;
            transportReceiveBufferPtr = networkReceiveBufferPtr;
        }

        // NoInlining as this should be a rare call if Garnet is properly configured
        [MethodImpl(MethodImplOptions.NoInlining)]
        unsafe void ShrinkNetworkReceiveBuffer(int newSize)
        {
            Debug.Assert(networkReadHead == 0, "Shouldn't call if remaining data not already moved to head of receive buffer");
            Debug.Assert(networkBytesRead <= newSize, "Shrink target must hold the bytes already buffered");

            var tmp = networkPool.Get(newSize, PoolEntryBufferType.ShrinkNetworkReceiveBuffer);
            if (networkBytesRead > 0)
            {
                Array.Copy(networkReceiveBuffer, tmp.entry, networkBytesRead);
            }

            networkReceiveBufferEntry.Dispose();
            networkReceiveBufferEntry = tmp;
            networkReceiveBuffer = tmp.entry;
            networkReceiveBufferPtr = tmp.entryPtr;
            RefreshNonTlsTransportAlias();
        }

        unsafe void ShiftNetworkReceiveBuffer()
        {
            var bytesLeft = networkBytesRead - networkReadHead;
            if (bytesLeft != networkBytesRead)
            {
                // Shift them to the head of the array so we can reset the buffer to a consistent state                
                if (bytesLeft > 0) Buffer.MemoryCopy(networkReceiveBufferPtr + networkReadHead, networkReceiveBufferPtr, bytesLeft, bytesLeft);
                networkBytesRead = bytesLeft;
                networkReadHead = 0;
            }
        }

        unsafe void DoubleTransportReceiveBuffer()
        {
            if (sslStream != null)
            {
                var tmp = networkPool.Get(transportReceiveBuffer.Length * 2, PoolEntryBufferType.DoubleTransportReceiveBuffer);
                Array.Copy(transportReceiveBuffer, tmp.entry, transportReceiveBuffer.Length);
                transportReceiveBufferEntry.Dispose();
                transportReceiveBufferEntry = tmp;
                transportReceiveBuffer = tmp.entry;
                transportReceiveBufferPtr = tmp.entryPtr;
                transportShrinkCountdown = ShrinkHysteresis;
            }
        }

        /// <summary>
        /// Mirror of <see cref="TryShrinkNetworkReceiveBuffer"/> for the decrypted TLS transport buffer.
        /// Only safe to call from the reader while it owns the buffer and no <c>ReadAsync</c> is outstanding
        /// against it, which is exactly where <see cref="DoubleTransportReceiveBuffer"/> is called from.
        /// </summary>
        /// <param name="demand">
        /// Bytes the buffer held for this pass, sampled before consumed bytes were shifted away. This is the
        /// capacity the traffic needed; the residual left after processing is not.
        /// </param>
        void TryShrinkTransportReceiveBuffer(int demand)
        {
            if (sslStream == null)
                return;

            if (TryPlanReceiveBufferShrink(transportReceiveBuffer.Length, transportBytesRead, demand, ref transportShrinkCountdown, out var target))
                ShrinkTransportReceiveBuffer(target);
        }

        // NoInlining as this should be a rare call if Garnet is properly configured
        [MethodImpl(MethodImplOptions.NoInlining)]
        unsafe void ShrinkTransportReceiveBuffer(int newSize)
        {
            Debug.Assert(transportReadHead == 0, "Shouldn't call if remaining data not already moved to head of transport buffer");
            Debug.Assert(transportBytesRead <= newSize, "Shrink target must hold the bytes already buffered");

            var tmp = networkPool.Get(newSize, PoolEntryBufferType.ShrinkTransportReceiveBuffer);
            if (transportBytesRead > 0)
            {
                Array.Copy(transportReceiveBuffer, tmp.entry, transportBytesRead);
            }

            transportReceiveBufferEntry.Dispose();
            transportReceiveBufferEntry = tmp;
            transportReceiveBuffer = tmp.entry;
            transportReceiveBufferPtr = tmp.entryPtr;
        }

        unsafe void ShiftTransportReceiveBuffer()
        {
            // The bytes left in the current buffer not consumed by previous operations
            var bytesLeft = transportBytesRead - transportReadHead;
            if (bytesLeft != transportBytesRead)
            {
                // Shift them to the head of the array so we can reset the buffer to a consistent state                
                if (bytesLeft > 0) Buffer.MemoryCopy(transportReceiveBufferPtr + transportReadHead, transportReceiveBufferPtr, bytesLeft, bytesLeft);
                transportBytesRead = bytesLeft;
                transportReadHead = 0;
            }
        }

        /// <inheritdoc />
        public override void Enter()
            => networkSender.Enter();

        /// <inheritdoc />
        public override unsafe void EnterAndGetResponseObject(out byte* head, out byte* tail)
        {
            networkSender.Enter();
            head = transportSendBufferPtr;
            tail = transportSendBufferPtr + transportSendBuffer.Length;
        }

        /// <inheritdoc />
        public override void Exit()
            => networkSender.Exit();

        /// <inheritdoc />
        public override void ExitAndReturnResponseObject()
        {
            networkSender.Exit();
        }

        /// <inheritdoc />
        public override void GetResponseObject() { }

        /// <inheritdoc />
        public override void ReturnResponseObject() { }

        /// <inheritdoc />
        public override unsafe bool SendResponse(int offset, int size)
        {
#if MESSAGETRAGE
            logger?.LogInformation("Sending response of size {size} bytes", size);
            logger?.LogTrace("SEND: [{send}]", System.Text.Encoding.UTF8.GetString(
                new Span<byte>(transportSendBuffer).Slice(offset, size)).Replace("\n", "|").Replace("\r", ""));
#endif
            sslStream.Write(transportSendBuffer, offset, size);
            sslStream.Flush();
            return true;
        }

        /// <inheritdoc />
        public override void SendResponse(byte[] buffer, int offset, int count, object context)
        {
#if MESSAGETRAGE
            logger?.LogInformation("Sending response of size {count} bytes", count);
            logger?.LogTrace("SEND: [{send}]", System.Text.Encoding.UTF8.GetString(
                new Span<byte>(buffer).Slice(offset, count)).Replace("\n", "|").Replace("\r", ""));
#endif
            sslStream.Write(buffer, offset, count);
            sslStream.Flush();
            networkSender.SendCallback(context);
        }

        /// <inheritdoc />
        public override void SendCallback(object context) { }

        /// <inheritdoc />
        public override unsafe byte* GetResponseObjectHead()
            => transportSendBufferPtr;

        /// <inheritdoc />
        public override unsafe byte* GetResponseObjectTail()
            => transportSendBufferPtr + transportSendBuffer.Length;

        /// <summary>
        /// Implementation of dispose for network handler.
        /// Expected to be called exactly once, by the same thread that listens to network
        /// and calls the mono-threaded ProcessMessage.
        /// </summary>
        /// <exception cref="Exception"></exception>
        protected void DisposeImpl()
        {
            // We might dispose either via SAEA callback or via user Dispose code path
            // Ensure we perform the dispose logic exactly once
            if (Interlocked.Increment(ref disposeCount) != 1)
            {
                logger?.LogTrace("NetworkHandler.Dispose called multiple times");
                return;
            }

            cancellationTokenSource?.Cancel();

            // A parked session owns the receive state, including the SocketAsyncEventArgs its resume path is
            // waiting to release. Abandon the operation so that path runs and cleans up rather than leaking.
            AbortParkedSession();

            // Before deferring anything: a resume may be draining a pipelined command that still waits in
            // place, and such a wait ends only when the session is told it is going away. That notification
            // is part of the reclamation about to be deferred behind the very wait it would end, so it has
            // to be delivered here instead.
            parkableSession?.CancelInPlaceWaits();

            // If a resume is in flight it is actively using the session and the receive buffer, so it -- not
            // this thread -- must be the one to release them. It takes the lease before reading disposeCount
            // and this reads the lease after publishing disposeCount, so the two cannot both skip the work.
            if ((Interlocked.Or(ref resumeLease, ResumeLeaseReclaimDeferred) & ResumeLeaseOwnerMask) != 0)
                return;

            // No resume was in flight. Claim the reclamation, unless one started in the meantime and took it.
            if (Interlocked.CompareExchange(ref resumeLease, 0, ResumeLeaseReclaimDeferred) == ResumeLeaseReclaimDeferred)
                ReclaimConnectionResources();
        }

        /// <summary>
        /// Releases everything the connection owns. Runs exactly once, on whichever of teardown or a parked
        /// session's resume path is last to let go of the connection.
        /// </summary>
        void ReclaimConnectionResources()
        {
            serverHook.DisposeMessageConsumer(this);
            networkSender.Dispose();
            sslStream?.Dispose();
            // Release the reader so it sees the cancellation
            receivedData?.Release();
            receivedData?.Dispose();
            // Release the expecter so it sees the cancellation
            expectingData?.Release();
            expectingData?.Dispose();
            cancellationTokenSource?.Dispose();
            networkReceiveBufferEntry?.Dispose();
            transportSendBufferEntry?.Dispose();
            transportReceiveBufferEntry?.Dispose();
        }

        /// <inheritdoc />
        public override void DisposeNetworkSender(bool waitForSendCompletion)
            => networkSender.DisposeNetworkSender(waitForSendCompletion);

        /// <inheritdoc />
        public override void Throttle() { }

        static void LogSecurityInfo(SslStream stream, string remoteEndpointName, ILogger logger = null)
        {
            logger?.LogTrace("[{remoteEndpointName}] Cipher Suite: {NegotiatedCipherSuite}", remoteEndpointName, stream.NegotiatedCipherSuite);
            logger?.LogTrace("[{remoteEndpointName}] Protocol: {SslProtocol}", remoteEndpointName, stream.SslProtocol);

            logger?.LogTrace("[{remoteEndpointName}] Is authenticated: {IsAuthenticated} as server? {IsServer}", remoteEndpointName, stream.IsAuthenticated, stream.IsServer);
            logger?.LogTrace("[{remoteEndpointName}] IsSigned: {IsSigned}", remoteEndpointName, stream.IsSigned);
            logger?.LogTrace("[{remoteEndpointName}] Is Encrypted: {IsEncrypted}", remoteEndpointName, stream.IsEncrypted);

            logger?.LogTrace("[{remoteEndpointName}] Can read: {CanRead}, write {CanWrite}", remoteEndpointName, stream.CanRead, stream.CanWrite);
            logger?.LogTrace("[{remoteEndpointName}] Can timeout: {CanTimeout}", remoteEndpointName, stream.CanTimeout);

            logger?.LogTrace("[{remoteEndpointName}] Certificate revocation list checked: {CheckCertRevocationStatus}", remoteEndpointName, stream.CheckCertRevocationStatus);

            X509Certificate localCertificate = stream.LocalCertificate;
            if (stream.LocalCertificate != null)
            {
                logger?.LogTrace("[{remoteEndpointName}] Local cert was issued to {Subject} and is valid from {GetEffectiveDateString} until {GetExpirationDateString}.",
                    remoteEndpointName,
                    localCertificate.Subject,
                    localCertificate.GetEffectiveDateString(),
                    localCertificate.GetExpirationDateString());
            }
            else
            {
                logger?.LogTrace("[{remoteEndpointName}] Local certificate is null.", remoteEndpointName);
            }
            // Display the properties of the client's certificate.
            X509Certificate remoteCertificate = stream.RemoteCertificate;
            if (stream.RemoteCertificate != null)
            {
                logger?.LogTrace("[{remoteEndpointName}] Remote cert was issued to {Subject} and is valid from {GetEffectiveDateString} until {GetExpirationDateString}.",
                    remoteEndpointName,
                    remoteCertificate.Subject,
                    remoteCertificate.GetEffectiveDateString(),
                    remoteCertificate.GetExpirationDateString());
            }
            else
            {
                logger?.LogTrace("[{remoteEndpointName}] Remote certificate is null.", remoteEndpointName);
            }
        }
    }
}