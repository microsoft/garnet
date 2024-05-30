﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Security;
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
        /// Network buffer pool
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

        /// <inheritdoc />
        public IMessageConsumer Session => session;

        readonly ILogger logger;

        /* TLS related fields */
        readonly SslStream sslStream;
        readonly SemaphoreSlim receivedData;
        readonly CancellationTokenSource cancellationTokenSource;

        // Stream reader status: Rest = 0, Active = 1, Waiting = 2
        volatile TlsReaderStatus readerStatus;

        // Number of times Dispose has been called
        int disposeCount;

        /// <summary>
        /// Constructor
        /// </summary>
        public unsafe NetworkHandler(TServerHook serverHook, TNetworkSender networkSender, LimitedFixedBufferPool networkPool, bool useTLS, IMessageConsumer messageConsumer = null, ILogger logger = null)
            : base(networkPool.MinAllocationSize)
        {
            this.logger = logger;
            this.serverHook = serverHook;
            this.networkSender = networkSender;
            this.session = messageConsumer;
            this.readerStatus = TlsReaderStatus.Rest;
            this.networkPool = networkPool;

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
                cancellationTokenSource = new();

                transportReceiveBufferEntry = networkPool.Get(networkPool.MinAllocationSize);
                transportReceiveBuffer = transportReceiveBufferEntry.entry;
                transportReceiveBufferPtr = transportReceiveBufferEntry.entryPtr;

                transportSendBufferEntry = networkPool.Get(networkPool.MinAllocationSize);
                transportSendBuffer = transportSendBufferEntry.entry;
                transportSendBufferPtr = transportSendBufferEntry.entryPtr;
            }
        }

        /// <summary>
        /// Begin (background) network handler (including auth). Make sure you do not send data
        /// until authentication completes.
        /// </summary>
        public virtual void Start(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslServerAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslServerAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;
            if (token == default) token = cancellationTokenSource.Token;

            _ = AuthenticateAsServerAsync(tlsOptions, remoteEndpointName, token);
        }

        /// <summary>
        /// Begin async network handler (including auth)
        /// </summary>
        public virtual async Task StartAsync(SslServerAuthenticationOptions tlsOptions = null, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslServerAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslServerAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;
            if (token == default) token = cancellationTokenSource.Token;

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
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Begin (background) network handler (including auth). Make sure you do not send data
        /// until authentication completes.
        /// </summary>
        public virtual void Start(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslClientAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslClientAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;
            if (token == default) token = cancellationTokenSource.Token;

            _ = AuthenticateAsClientAsync(tlsOptions, remoteEndpointName, token);
        }

        /// <summary>
        /// Begin async network handler (including auth)
        /// </summary>
        public virtual async Task StartAsync(SslClientAuthenticationOptions tlsOptions, string remoteEndpointName = null, CancellationToken token = default)
        {
            if (tlsOptions != null && sslStream == null)
                throw new Exception("Need to provide SslClientAuthenticationOptions when TLS is enabled");
            if (tlsOptions == null && sslStream != null)
                throw new Exception("Cannot provide SslClientAuthenticationOptions when TLS is disabled");
            if (tlsOptions == null && sslStream == null) return;
            if (token == default) token = cancellationTokenSource.Token;

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
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// On network receive
        /// </summary>
        /// <param name="bytesTransferred">Number of bytes transferred</param>
        public unsafe void OnNetworkReceive(int bytesTransferred)
        {
            // Wait for SslStream sync processing task to complete, if any
            if (sslStream != null)
                while (readerStatus == TlsReaderStatus.Active) Thread.Yield();

            // Increment network bytes read
            networkBytesRead += bytesTransferred;

            // Double network buffer if out of space
            // Okay to do since we have control over network buffer here
            if (networkBytesRead == networkReceiveBuffer.Length)
                DoubleNetworkReceiveBuffer();

            if (readerStatus == TlsReaderStatus.Rest)
            {
                // Synchronously try to process the received data
                BeginTransformNetworkToTransport();

                // If ReadAsync is active, we are done here, the ReadAsync task will continue the processing
                if (readerStatus == TlsReaderStatus.Active) return;
            }
            else
            {
                Debug.Assert(readerStatus == TlsReaderStatus.Waiting);

                // ReadAsync task is waiting on semaphore, safe to shift network buffer
                // We do this here because the ReadAsync task cannot safely modify network buffer
                // as we will be performing a Socket.ReadAsync on it.
                if (networkReadHead > 0)
                    ShiftNetworkReceiveBuffer();

                // We have a ReadAsync task waiting for new data, set it to active status
                readerStatus = TlsReaderStatus.Active;

                // Unblock the asynchronous ReadAsync task
                receivedData.Release();

                // Release this task - ReadAsync task will continue the processing
                return;
            }

            // We do not have an active read task, so we will process on the network thread
            Debug.Assert(readerStatus != TlsReaderStatus.Active);
            Process();

            EndTransformNetworkToTransport();

            // Shift network buffer after processing is done
            if (networkReadHead > 0)
                ShiftNetworkReceiveBuffer();
        }

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

        unsafe void BeginTransformNetworkToTransport()
        {
            if (sslStream == null)
            {
                transportReceiveBuffer = networkReceiveBuffer;
                transportReceiveBufferPtr = networkReceiveBufferPtr;
                transportBytesRead = networkBytesRead;
            }
            else
            {
                readerStatus = TlsReaderStatus.Active;
                Read();
            }
        }

        void Read()
        {
            bool retry = false;
            while (networkBytesRead > networkReadHead || retry)
            {
                retry = false;
                var result = sslStream.ReadAsync(new Memory<byte>(transportReceiveBuffer, transportBytesRead, transportReceiveBuffer.Length - transportBytesRead), cancellationTokenSource.Token);
                if (result.IsCompletedSuccessfully)
                {
                    transportBytesRead += result.Result;

                    // Shift bytes in transport buffer
                    if (transportReadHead > 0)
                        ShiftTransportReceiveBuffer();

                    // Double the transport buffer if needed
                    if (transportBytesRead == transportReceiveBuffer.Length)
                    {
                        DoubleTransportReceiveBuffer();
                        retry = true;
                    }
                }
                else
                {
                    // Rare case: Our read has gone async, we need to invoke the async read processing code
                    _ = SslReaderAsync(result.AsTask(), cancellationTokenSource.Token);
                    return;
                }
            }
            readerStatus = TlsReaderStatus.Rest;
        }

        async Task SslReaderAsync(Task<int> readTask, CancellationToken token = default)
        {
            try
            {
                bool retry = false;
                int count = await readTask.ConfigureAwait(false);

                Debug.Assert(readerStatus == TlsReaderStatus.Active);

                transportBytesRead += count;

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
                // If more work, passthrough to the general SslReaderAsync, else this task is done
                if (networkBytesRead > networkReadHead || retry)
                    _ = SslReaderAsync(token);
                else
                    readerStatus = TlsReaderStatus.Rest;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception has occurred during NetworkHandler.SslReaderAsync(Task)");
                readerStatus = TlsReaderStatus.Rest;
                Dispose();
            }
        }

        async Task SslReaderAsync(CancellationToken token = default)
        {
            Debug.Assert(readerStatus == TlsReaderStatus.Active);

            try
            {
                bool retry = false;
                while (networkBytesRead > networkReadHead || retry)
                {
                    retry = false;
                    Debug.Assert(readerStatus == TlsReaderStatus.Active);
                    int count = await sslStream.ReadAsync(new Memory<byte>(transportReceiveBuffer, transportBytesRead, transportReceiveBuffer.Length - transportBytesRead), token).ConfigureAwait(false);
                    Debug.Assert(readerStatus == TlsReaderStatus.Active);

                    transportBytesRead += count;

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
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception has occurred during SslReaderAsync");
                Dispose();
                return;
            }
            finally
            {
                readerStatus = TlsReaderStatus.Rest;
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
            var tmp = networkPool.Get(networkReceiveBuffer.Length * 2);
            Array.Copy(networkReceiveBuffer, tmp.entry, networkReceiveBuffer.Length);
            networkReceiveBufferEntry.Dispose();
            networkReceiveBufferEntry = tmp;
            networkReceiveBuffer = tmp.entry;
            networkReceiveBufferPtr = tmp.entryPtr;
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
                var tmp = networkPool.Get(transportReceiveBuffer.Length * 2);
                Array.Copy(transportReceiveBuffer, tmp.entry, transportReceiveBuffer.Length);
                transportReceiveBufferEntry.Dispose();
                transportReceiveBufferEntry = tmp;
                transportReceiveBuffer = tmp.entry;
                transportReceiveBufferPtr = tmp.entryPtr;
            }
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
            logger?.LogInformation($"Sending response of size {size} bytes");
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
            logger?.LogInformation($"Sending response of size {count} bytes");
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
                throw new Exception("NetworkHandler.Dispose called multiple times");
            }

            cancellationTokenSource?.Cancel();
            serverHook.DisposeMessageConsumer(this);
            networkSender.Dispose();
            sslStream?.Dispose();
            // Release the reader so it sees the cancellation
            receivedData?.Release();
            receivedData?.Dispose();
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
            logger?.LogTrace("[{remoteEndpointName}] Cipher: {CipherAlgorithm} strength {CipherStrength}", remoteEndpointName, stream.CipherAlgorithm, stream.CipherStrength);
            logger?.LogTrace("[{remoteEndpointName}] Hash: {HashAlgorithm} strength {HashStrength}", remoteEndpointName, stream.HashAlgorithm, stream.HashStrength);
            logger?.LogTrace("[{remoteEndpointName}] Key exchange: {KeyExchangeAlgorithm} strength {KeyExchangeStrength}", remoteEndpointName, stream.KeyExchangeAlgorithm, stream.KeyExchangeStrength);
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