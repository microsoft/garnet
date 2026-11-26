// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Net.Security;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.common.Parsing;
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
        /// Pool entry for <see cref="networkReceiveBuffer"/>; null while the buffer is native
        /// </summary>
        protected PoolEntry networkReceiveBufferEntry;

        /// <summary>
        /// Native memory behind the network receive buffer while it is larger than the pool's largest size class; created the first time it is,
        /// and reused
        /// </summary>
        private protected NativeReceiveBuffer networkReceiveNative;

        /// <summary>
        /// Buffer that receives data directly from network; null while the buffer is native (see <see cref="networkReceiveNative"/>)
        /// This is allocated and populated by derived classes
        /// </summary>
        protected byte[] networkReceiveBuffer;

        /// <summary>
        /// Pointer to buffer that receives data directly from network
        /// This is allocated and populated by derived classes
        /// </summary>
        protected unsafe byte* networkReceiveBufferPtr;

        /// <summary>
        /// Length of the network receive buffer
        /// </summary>
        protected int networkReceiveBufferLength;

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
        NativeReceiveBuffer transportReceiveNative;
        /// <summary>
        /// Transport receive buffer; null while the buffer is native (see <see cref="transportReceiveNative"/>)
        /// </summary>
        protected byte[] transportReceiveBuffer;
        unsafe byte* transportReceiveBufferPtr;
        int transportReceiveBufferLength;

        /// <summary>
        /// Bytes read by application from transport buffer
        /// </summary>
        int transportBytesRead, transportReadHead;

        /* Buffer that application writes data to */
        readonly PoolEntry transportSendBufferEntry;
        readonly byte[] transportSendBuffer;
        readonly unsafe byte* transportSendBufferPtr;

        // Receive cleanup owns one interest. Clients retaining raw plaintext pointers acquire another
        // before Start; the last release returns the buffer, independently of socket shutdown.
        int transportSendBufferOwners = 1;

        /* Wrapper for buffer used to write directly to the network */
        readonly TNetworkSender networkSender;

        IMessageConsumer session;

        /// <inheritdoc />
        public IMessageConsumer Session => session;

        readonly ILogger logger;

        /* TLS related fields */
        readonly SslStream sslStream;
        readonly SemaphoreSlim receivedData, expectingData;
        protected readonly CancellationTokenSource cancellationTokenSource;
        readonly CancellationToken connectionCancellationToken;

        // Stream reader status: Rest = 0, Active = 1, Waiting = 2
        volatile TlsReaderStatus readerStatus;

        // Number of times Dispose has been called
        int disposeCount;

        // Set once a TLS write fails; a failed write can leave a partial record on the connection, so nothing
        // further can be written to it
        bool writeFailed;

        // Longest a server-side TLS handshake may take; zero waits indefinitely
        readonly TimeSpan tlsHandshakeTimeout;

        // Longest delay a cancellation timer supports, about 49.7 days
        static readonly TimeSpan MaxTlsHandshakeTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tlsHandshakeTimeout">Longest a server-side TLS handshake may take before the connection is closed. Zero or negative waits indefinitely; a timeout beyond the longest a timer supports, about 49.7 days, is limited to it.</param>
        public unsafe NetworkHandler(TServerHook serverHook, TNetworkSender networkSender, NetworkBufferSettings networkBufferSettings, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null, TimeSpan tlsHandshakeTimeout = default)
            : base(networkPool.MinAllocationSize)
        {
            this.logger = logger;
            this.tlsHandshakeTimeout = tlsHandshakeTimeout <= TimeSpan.Zero ? TimeSpan.Zero
                : tlsHandshakeTimeout < MaxTlsHandshakeTimeout ? tlsHandshakeTimeout : MaxTlsHandshakeTimeout;
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
                connectionCancellationToken = cancellationTokenSource.Token;

                transportReceiveBufferEntry = this.networkPool.Get(BaseReceiveBufferSize, PoolEntryBufferType.TransportReceiveBuffer);
                transportReceiveBuffer = transportReceiveBufferEntry.entry;
                transportReceiveBufferPtr = transportReceiveBufferEntry.entryPtr;
                transportReceiveBufferLength = transportReceiveBuffer.Length;

                transportSendBufferEntry = this.networkPool.Get(BaseSendBufferSize, PoolEntryBufferType.TransportSendBuffer);
                transportSendBuffer = transportSendBufferEntry.entry;
                transportSendBufferPtr = transportSendBufferEntry.entryPtr;
            }
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

            // The deadline is absolute; handshake bytes that arrive before it do not extend it
            using var handshakeDeadline = tlsHandshakeTimeout > TimeSpan.Zero ? CancellationTokenSource.CreateLinkedTokenSource(token) : null;
            handshakeDeadline?.CancelAfter(tlsHandshakeTimeout);
            try
            {
                await sslStream.AuthenticateAsServerAsync(tlsOptions, handshakeDeadline?.Token ?? token).ConfigureAwait(false);

                if (token.IsCancellationRequested) throw new TaskCanceledException("AuthenticateAsServerAsync was cancelled");

                logger?.LogDebug("Completed server TLS authentication for {remoteEndpoint}", remoteEndpointName);
                // Display the properties and settings for the authenticated stream.
                if (logger != null && logger.IsEnabled(LogLevel.Trace))
                    LogSecurityInfo(sslStream, remoteEndpointName, logger);

                // There may be extra bytes left over after auth, we need to process them (non-blocking) before returning
                var result = sslStream.ReadAsync(TransportReceiveMemory[transportBytesRead..], cancellationTokenSource.Token);
                _ = SslReaderAsync(result.AsTask(), cancellationTokenSource.Token);
            }
            catch (Exception ex)
            {
                var timedOut = handshakeDeadline?.IsCancellationRequested == true && !token.IsCancellationRequested;
                if (timedOut)
                    logger?.LogInformation("TLS handshake with {remoteEndpoint} did not complete within {timeout}", remoteEndpointName, tlsHandshakeTimeout);
                else
                    logger?.LogWarning(ex, "An error has occurred");
                readerStatus = TlsReaderStatus.Rest;
                if (expectingData.CurrentCount == 0) expectingData.Release();
                Dispose();
                if (timedOut)
                    throw new TimeoutException($"TLS handshake did not complete within {tlsHandshakeTimeout}", ex);
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
            // Socket teardown must cancel the handshake's transport read even when the caller supplied a token.
            using var authenticationCancellation = CancellationTokenSource.CreateLinkedTokenSource(token, connectionCancellationToken);
            try
            {
                await sslStream.AuthenticateAsClientAsync(sslClientOptions, authenticationCancellation.Token).ConfigureAwait(false);

                if (token.IsCancellationRequested) throw new TaskCanceledException("AuthenticateAsClientAsync was cancelled");

                logger?.LogDebug("Completed client TLS authentication for {remoteEndpoint}", remoteEndpointName);
                // Display the properties and settings for the authenticated stream.
                if (logger != null && logger.IsEnabled(LogLevel.Trace))
                    LogSecurityInfo(sslStream, remoteEndpointName, logger);

                // There may be extra bytes left over after auth, we need to process them (non-blocking) before returning
                var result = sslStream.ReadAsync(TransportReceiveMemory[transportBytesRead..], cancellationTokenSource.Token);
                _ = SslReaderAsync(result.AsTask(), cancellationTokenSource.Token);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An error has occurred");
                Dispose();
                if (ex is OperationCanceledException && !token.IsCancellationRequested)
                    throw new IOException("The connection closed during TLS authentication.", ex);
                throw;
            }
        }

        public unsafe void OnNetworkReceiveWithoutTLS(int bytesTransferred)
        {
            networkBytesRead += bytesTransferred;
            transportReceiveBuffer = networkReceiveBuffer;
            transportReceiveBufferPtr = networkReceiveBufferPtr;
            transportReceiveBufferLength = networkReceiveBufferLength;
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
            if (networkBytesRead == networkReceiveBufferLength)
            {
                DoubleNetworkReceiveBuffer();
                networkShrinkCountdown = ShrinkHysteresis;
            }
            else if (networkReceiveBufferLength > BaseReceiveBufferSize)
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
        /// <param name="plaintext">The unparsed bytes at the head of the buffer, or null when the buffer holds ciphertext.</param>
        /// <param name="buffered">Bytes still buffered, which the replacement has to be able to hold.</param>
        /// <param name="demand">
        /// Bytes the buffer held for this pass, sampled before consumed bytes were shifted away. This is the
        /// capacity the traffic needed; the residual left after processing is not.
        /// </param>
        /// <param name="countdown">The caller's shrink hysteresis countdown, updated in place.</param>
        /// <param name="target">Size to replace the buffer with, when this returns <see langword="true"/>.</param>
        /// <returns><see langword="true"/> when the caller should replace its buffer with a <paramref name="target"/>-byte one.</returns>
        unsafe bool TryPlanReceiveBufferShrink(int current, byte* plaintext, int buffered, int demand, ref int countdown, out int target)
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

                target = RaiseToPendingMessage(target, current, plaintext, buffered);
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
            target = RaiseToPendingMessage(target, current, plaintext, buffered);
            if (target >= current)
                return false;

            if (underPressure)
                budget.RecordPressureShrink();
            else
                budget.RecordIdleShrink();
            return true;
        }

        /// <summary>
        /// Raise a shrink <paramref name="target"/> to hold the whole message whose start is buffered. A buffer grown toward the size a message
        /// declares stays mostly empty until the message has arrived, and releasing it in the meantime would only grow it again.
        /// </summary>
        /// <param name="target">Planned size of the replacement buffer.</param>
        /// <param name="current">Size of the buffer being considered.</param>
        /// <param name="plaintext">The unparsed bytes at the head of the buffer, or null when the buffer holds ciphertext.</param>
        /// <param name="buffered">Number of bytes at <paramref name="plaintext"/>.</param>
        unsafe int RaiseToPendingMessage(int target, int current, byte* plaintext, int buffered)
        {
            if (plaintext == null || buffered == 0 || session == null)
                return target;

            var required = session.GetRequiredReceiveBufferSize(plaintext, buffered);
            if (required <= target)
                return target;

            return (int)Math.Min(Math.Max(DeclaredLengthGrowth.PendingMessageFloor(required, buffered), target), current);
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
        unsafe void TryShrinkNetworkReceiveBuffer(int demand)
        {
            // Without TLS the network buffer holds the plaintext the session parses; with TLS it holds ciphertext.
            var plaintext = sslStream == null ? networkReceiveBufferPtr : null;
            if (TryPlanReceiveBufferShrink(networkReceiveBufferLength, plaintext, networkBytesRead, demand, ref networkShrinkCountdown, out var target))
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
                if (session != null || serverHook.TryCreateMessageConsumer(new Span<byte>(transportReceiveBufferPtr, transportBytesRead), GetNetworkSender(), out session))
                    TryProcessRequest();
            }
        }

        /// <summary>
        /// Get network sender for this handler
        /// </summary>
        public INetworkSender GetNetworkSender() => sslStream == null ? networkSender : this;

        /// <summary>
        /// Handles end-of-stream for the TLS reader. A zero-byte read from <see cref="sslStream"/> means the peer
        /// completed a graceful TLS shutdown (close_notify) or closed the connection, so no further plaintext can
        /// arrive. Returns the reader to Rest, wakes the receive-loop waiter, and disposes the connection instead of
        /// re-reading zero bytes forever while a partial frame remains buffered.
        /// </summary>
        void HandleReaderEndOfStream()
        {
            readerStatus = TlsReaderStatus.Rest;
            if (expectingData.CurrentCount == 0) expectingData.Release();
            Dispose();
        }

        void Read()
        {
            bool retry = false;
            while (networkBytesRead > networkReadHead || transportBytesRead > 0 || retry)
            {
                retry = false;
                var result = sslStream.ReadAsync(TransportReceiveMemory[transportBytesRead..], cancellationTokenSource.Token);
                if (result.IsCompletedSuccessfully)
                {
                    // blocking is unavoidable here, but safe since we've checked IsCompletedSuccessfully
                    var count = AsyncUtils.BlockingWait(result);
                    if (count == 0)
                    {
                        // Peer completed a graceful TLS shutdown (close_notify) or closed the connection: no further
                        // plaintext can arrive. Tear down instead of re-reading zero bytes forever while a partial
                        // frame remains buffered. No expectingData release here: the synchronous path has no waiter.
                        readerStatus = TlsReaderStatus.Rest;
                        Dispose();
                        return;
                    }
                    transportBytesRead += count;

                    // Occupancy before processing is the capacity this pass needed; the residual after it is not.
                    var transportDemand = transportBytesRead;

                    // Read task has control, process the decrypted transport bytes
                    Process();

                    // Shift bytes in transport buffer
                    if (transportReadHead > 0)
                        ShiftTransportReceiveBuffer();

                    retry = AdjustTransportReceiveBufferAfterProcess(transportDemand);
                }
                else
                {
                    // Rare case: Our read has gone async, we need to invoke the async read processing code.
                    _ = SslReaderAsync(result.AsTask(), cancellationTokenSource.Token);
                    return;
                }
            }
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

                if (count == 0)
                {
                    // Peer completed a graceful TLS shutdown (close_notify) or closed the connection: no further
                    // plaintext can arrive. Tear down instead of re-reading zero bytes forever while a partial frame
                    // remains buffered.
                    HandleReaderEndOfStream();
                    return;
                }

                transportBytesRead += count;

                // Occupancy before processing is the capacity this pass needed; the residual after it is not.
                var transportDemand = transportBytesRead;

                // Read task has control, process the decrypted transport bytes
                Process();

                // Shift bytes in transport buffer, Process would not have shifted
                // as we are in active state
                if (transportReadHead > 0)
                    ShiftTransportReceiveBuffer();

                retry = AdjustTransportReceiveBufferAfterProcess(transportDemand);

                // Continue while a reply remains partially parsed even when all ciphertext has been consumed;
                // SslStream may still hold its remaining plaintext internally. Also propagate retry after a read
                // that filled the transport buffer so SslStream is drained before returning to the network receive loop.
                if (networkBytesRead > networkReadHead || transportBytesRead > 0 || retry)
                {
                    _ = SslReaderLoopAsync(retry, token);
                }
                else
                {
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
                while (networkBytesRead > networkReadHead || transportBytesRead > 0 || retry)
                {
                    retry = false;
#if DEBUG
                    // Test-only rendezvous: while a partial (not-yet-consumable) frame is buffered, pause before
                    // the next read so a test can deliver a graceful close (close_notify/EOF) mid-frame and
                    // exercise the end-of-stream handling below. No-op unless the injection point is enabled;
                    // compiled out entirely in Release.
                    if (transportBytesRead > 0)
                        await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Tls_Pause_With_Partial_Frame).ConfigureAwait(false);
#endif
                    Debug.Assert(readerStatus == TlsReaderStatus.Active);
                    int count = await sslStream.ReadAsync(TransportReceiveMemory[transportBytesRead..], token).ConfigureAwait(false);
                    Debug.Assert(readerStatus == TlsReaderStatus.Active);

                    if (count == 0)
                    {
                        // Peer completed a graceful TLS shutdown (close_notify) or closed the connection: no further
                        // plaintext can arrive. Tear down instead of re-reading zero bytes forever while a partial
                        // frame remains buffered.
                        HandleReaderEndOfStream();
                        return;
                    }

                    transportBytesRead += count;

                    // Occupancy before processing is the capacity this pass needed; the residual after it is not.
                    var transportDemand = transportBytesRead;

                    // Read task has control, process the decrypted transport bytes
                    Process();

                    // Shift bytes in transport buffer, Process would not have shifted
                    // as we are in active state
                    if (transportReadHead > 0)
                        ShiftTransportReceiveBuffer();

                    retry = AdjustTransportReceiveBufferAfterProcess(transportDemand);
                }

                // Normal exit: hand control back to OnNetworkReceiveWithTLSAsync.
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
            // Without TLS the network buffer holds the plaintext the session parses; with TLS it holds ciphertext.
            var newSize = GrowReceiveBufferSize(networkReceiveBufferLength, sslStream == null ? networkReceiveBufferPtr : null, networkBytesRead);
            ReplaceNetworkReceiveBuffer(newSize, PoolEntryBufferType.DoubleNetworkReceiveBuffer);
        }

        /// <summary>
        /// Size to grow a full receive buffer of <paramref name="currentSize"/> bytes to. Doubles, unless the session can tell from the unparsed
        /// bytes that the message being received needs more, in which case the buffer grows toward the power of two that holds it by up to
        /// <see cref="DeclaredLengthGrowth.FastGrowthFactor"/> at once while below <see cref="DeclaredLengthGrowth.FastGrowthLimit"/>, rather
        /// than through every size between, each a fresh allocation and copy.
        /// </summary>
        /// <param name="currentSize">Size of the full buffer.</param>
        /// <param name="plaintext">The unparsed bytes at the head of the buffer, or null when the buffer holds ciphertext.</param>
        /// <param name="buffered">Number of bytes at <paramref name="plaintext"/>.</param>
        unsafe int GrowReceiveBufferSize(int currentSize, byte* plaintext, int buffered)
        {
            RespParsingException.ThrowIfReceiveBufferLimitExceeded(currentSize);
            var newSize = 2L * currentSize;
            if (plaintext != null && session != null && currentSize < DeclaredLengthGrowth.FastGrowthLimit)
            {
                var required = session.GetRequiredReceiveBufferSize(plaintext, buffered);
                if (required > newSize)
                {
                    RespParsingException.ThrowIfMessageExceedsReceiveBufferLimit(required);
                    newSize = Math.Max(newSize, Math.Min((long)BitOperations.RoundUpToPowerOf2((ulong)required), DeclaredLengthGrowth.Limit(currentSize)));
                }
            }
            return (int)Math.Min(newSize, RespLimits.MaxReceiveBufferBytes);
        }

        /// <summary>
        /// Whether receive buffers that grow past the pool's largest size class use native memory. Derived handlers that expose the receive
        /// buffer as an array turn this off.
        /// </summary>
        protected virtual bool UseNativeOversizedReceiveBuffers => true;

        /// <summary>
        /// Replace the network receive buffer with one of <paramref name="newSize"/> bytes holding the buffered bytes. Only called while no
        /// receive is outstanding against the buffer, so the old one is released at once.
        /// </summary>
        unsafe void ReplaceNetworkReceiveBuffer(int newSize, PoolEntryBufferType bufferType)
        {
            Debug.Assert(networkReadHead == 0, "Shouldn't call if remaining data not already moved to head of receive buffer");
            ReplaceReceiveBuffer(ref networkReceiveBufferEntry, ref networkReceiveNative, ref networkReceiveBuffer, ref networkReceiveBufferPtr,
                ref networkReceiveBufferLength, networkBytesRead, newSize, bufferType);
            RefreshNonTlsTransportAlias();
        }

        /// <summary>
        /// Replace a receive buffer with one of <paramref name="newSize"/> bytes holding its first <paramref name="buffered"/> bytes: a pinned
        /// array from the pool within its size classes, and above them the buffer's <see cref="NativeReceiveBuffer"/>, unless
        /// <see cref="UseNativeOversizedReceiveBuffers"/> is false. The caller guarantees no receive is outstanding against the buffer, so the
        /// region it replaces is released at once.
        /// </summary>
        unsafe void ReplaceReceiveBuffer(ref PoolEntry entry, ref NativeReceiveBuffer native, ref byte[] array, ref byte* ptr, ref int length,
            int buffered, int newSize, PoolEntryBufferType bufferType)
        {
            Debug.Assert(buffered <= newSize, "New buffer must hold the bytes already buffered");

            if (newSize > networkPool.MaxAllocationSize && UseNativeOversizedReceiveBuffers)
            {
                native ??= new NativeReceiveBuffer(networkPool);
                var region = native.Map(newSize);
                if (buffered > 0)
                    Buffer.MemoryCopy(ptr, (void*)region.AlignedPtr, newSize, buffered);
                HoldBeforeReceiveBufferSwap();

                // Unmaps the region it replaces, if the buffer was already native
                native.Install(region, newSize);
                array = null;
                ptr = (byte*)region.AlignedPtr;
                length = newSize;
                Interlocked.Exchange(ref entry, null)?.Dispose();
            }
            else
            {
                var pooled = networkPool.Get(newSize, bufferType);
                if (buffered > 0)
                    Buffer.MemoryCopy(ptr, pooled.entryPtr, newSize, buffered);
                HoldBeforeReceiveBufferSwap();

                array = pooled.entry;
                ptr = pooled.entryPtr;
                length = newSize;
                Interlocked.Exchange(ref entry, pooled)?.Dispose();
                native?.Unmap();
            }
            ReleaseReceiveBuffersIfDisposed();
        }

        /// <summary>Test hook: holds a receive buffer replacement after the new buffer is filled and before it is installed.</summary>
        [Conditional("DEBUG")]
        static void HoldBeforeReceiveBufferSwap()
        {
#if DEBUG
            ExceptionInjectionHelper.WaitOnClear(ExceptionInjectionType.ReceiveBuffer_Hold_Before_Swap);
#endif
        }

        /// <summary>
        /// Release the receive buffers if the handler was disposed while one was being replaced. <see cref="DisposeImpl"/> can run on another
        /// thread, such as when the server shuts down, and releases only the buffers installed when it runs.
        /// </summary>
        void ReleaseReceiveBuffersIfDisposed()
        {
            // Pairs with the increment in DisposeImpl: either it sees the buffer just installed, or this sees the handler disposed.
            Interlocked.MemoryBarrier();
            if (Volatile.Read(ref disposeCount) > 0)
                ReleaseReceiveBuffers();
        }

        /// <summary>
        /// Release the receive buffers on disposal. A pooled entry is taken by exchange, so that it is returned exactly once when a buffer
        /// replacement races this; releasing a native buffer is idempotent.
        /// </summary>
        void ReleaseReceiveBuffers()
        {
            Interlocked.Exchange(ref networkReceiveBufferEntry, null)?.Dispose();
            Interlocked.Exchange(ref transportReceiveBufferEntry, null)?.Dispose();

            // A native region stays mapped until the handler is unreachable: a reader or a socket receive on another thread may still be using it.
            networkReceiveNative?.ReleaseDeferred();
            transportReceiveNative?.ReleaseDeferred();
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
            transportReceiveBufferLength = networkReceiveBufferLength;
        }

        // NoInlining as this should be a rare call if Garnet is properly configured
        [MethodImpl(MethodImplOptions.NoInlining)]
        void ShrinkNetworkReceiveBuffer(int newSize)
            => ReplaceNetworkReceiveBuffer(newSize, PoolEntryBufferType.ShrinkNetworkReceiveBuffer);

        /// <summary>
        /// Copy <paramref name="destination"/>.Length bytes from the head of the unread network bytes.
        /// </summary>
        unsafe void CopyFromNetworkReceiveBuffer(Span<byte> destination)
            => new ReadOnlySpan<byte>(networkReceiveBufferPtr + networkReadHead, destination.Length).CopyTo(destination);

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
                var newSize = GrowReceiveBufferSize(transportReceiveBufferLength, transportReceiveBufferPtr, transportBytesRead);
                ReplaceTransportReceiveBuffer(newSize, PoolEntryBufferType.DoubleTransportReceiveBuffer);
                transportShrinkCountdown = ShrinkHysteresis;
            }
        }

        /// <summary>
        /// The transport receive buffer, for reads from <see cref="SslStream"/>.
        /// </summary>
        Memory<byte> TransportReceiveMemory
            => transportReceiveBuffer != null ? transportReceiveBuffer.AsMemory(0, transportReceiveBufferLength) : transportReceiveNative.Memory;

        /// <summary>
        /// Resizes the transport buffer after a processed read and reports whether the reader must read again.
        /// A read that filled the buffer may have stopped short of the end of a TLS record, leaving decrypted
        /// plaintext inside <see cref="SslStream"/> with no further ciphertext to trigger another receive, so
        /// the reader re-reads even when processing consumed every byte. The buffer doubles only when unparsed
        /// bytes still fill it; otherwise the shrink policy applies first, so that the re-read, which may wait
        /// indefinitely, does not hold a buffer the connection no longer needs.
        /// </summary>
        /// <param name="transportDemand">Bytes the buffer held before processing.</param>
        /// <returns>True if the reader must issue another read before resting.</returns>
        bool AdjustTransportReceiveBufferAfterProcess(int transportDemand)
        {
            if (transportBytesRead == transportReceiveBufferLength)
            {
                DoubleTransportReceiveBuffer();
                return true;
            }

            var filled = transportDemand == transportReceiveBufferLength;
            TryShrinkTransportReceiveBuffer(transportDemand);
            return filled;
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
        unsafe void TryShrinkTransportReceiveBuffer(int demand)
        {
            if (sslStream == null)
                return;

            if (TryPlanReceiveBufferShrink(transportReceiveBufferLength, transportReceiveBufferPtr, transportBytesRead, demand, ref transportShrinkCountdown, out var target))
                ShrinkTransportReceiveBuffer(target);
        }

        // NoInlining as this should be a rare call if Garnet is properly configured
        [MethodImpl(MethodImplOptions.NoInlining)]
        void ShrinkTransportReceiveBuffer(int newSize)
            => ReplaceTransportReceiveBuffer(newSize, PoolEntryBufferType.ShrinkTransportReceiveBuffer);

        /// <summary>
        /// Replace the TLS transport receive buffer with one of <paramref name="newSize"/> bytes holding the buffered bytes. Only called by the
        /// reader while no <c>ReadAsync</c> is outstanding against the buffer, so the old one is released at once.
        /// </summary>
        unsafe void ReplaceTransportReceiveBuffer(int newSize, PoolEntryBufferType bufferType)
        {
            Debug.Assert(transportReadHead == 0, "Shouldn't call if remaining data not already moved to head of transport buffer");
            ReplaceReceiveBuffer(ref transportReceiveBufferEntry, ref transportReceiveNative, ref transportReceiveBuffer, ref transportReceiveBufferPtr,
                ref transportReceiveBufferLength, transportBytesRead, newSize, bufferType);
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
            if (writeFailed)
                return false;

            try
            {
                sslStream.Write(transportSendBuffer, offset, size);
                sslStream.Flush();
            }
            catch (Exception ex)
            {
                writeFailed = true;

                // SslStream wraps what the underlying sender throws; surface the sender's own failure, such as a send
                // timeout, as the session would see it without TLS
                if (ex is IOException { InnerException: GarnetException inner })
                    ExceptionDispatchInfo.Throw(inner);
                throw;
            }
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
        /// Retains the TLS plaintext send buffer for an external sender. Call before starting the handler.
        /// </summary>
        protected void RetainTransportSendBuffer()
        {
            Debug.Assert(disposeCount == 0);
            if (transportSendBufferEntry != null)
                Interlocked.Increment(ref transportSendBufferOwners);
        }

        /// <summary>
        /// Disposes one TLS plaintext-buffer ownership interest; the last owner returns the buffer.
        /// </summary>
        protected void DisposeTransportSendBuffer()
        {
            if (transportSendBufferEntry != null && Interlocked.Decrement(ref transportSendBufferOwners) == 0)
                transportSendBufferEntry.Dispose();
        }

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

            try
            {
                cancellationTokenSource?.Cancel();
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
                ReleaseReceiveBuffers();
            }
            finally
            {
                DisposeTransportSendBuffer();
#if DEBUG
                if (transportSendBufferEntry != null)
                    _ = ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.Tls_Signal_After_Handler_Dispose);
                _ = ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.Handler_Signal_After_Dispose);
#endif
            }
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