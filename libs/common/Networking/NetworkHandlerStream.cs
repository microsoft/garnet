// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.networking
{
    public abstract partial class NetworkHandler<TServerHook, TNetworkSender>
        where TServerHook : IServerHook
        where TNetworkSender : INetworkSender
    {
        private sealed class NetworkHandlerStream : Stream
        {
            readonly NetworkHandler<TServerHook, TNetworkSender> garnetNetworkHandler;
            readonly ILogger logger;

            public NetworkHandlerStream(NetworkHandler<TServerHook, TNetworkSender> garnetNetworkHandler, ILogger logger = null)
            {
                this.garnetNetworkHandler = garnetNetworkHandler;
                this.logger = logger;
            }

            public override bool CanTimeout => true;

            public override bool CanRead => true;

            public override bool CanSeek => false;

            public override bool CanWrite => true;

            public override long Length
            {
                get
                {
                    throw new NotSupportedException();
                }
            }

            public override long Position
            {
                get
                {
                    throw new NotSupportedException();
                }
                set
                {
                    throw new NotSupportedException();
                }
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
            {
                return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
            }

            public override ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
                => garnetNetworkHandler.validatingClientCertificate
                    ? HandshakeReadAsync(destination, cancellationToken)
                    : ReadAsyncInternalAsync(destination, cancellationToken);

            /// <summary>
            /// A read of a handshake that validates a client certificate: completes on the thread pool if the thread
            /// that completed it must not wait, so <see cref="SslStream"/> continues there.
            /// </summary>
            async ValueTask<int> HandshakeReadAsync(Memory<byte> destination, CancellationToken cancellationToken)
            {
                var read = await ReadAsyncInternalAsync(destination, cancellationToken).ConfigureAwait(false);
                if (ThreadPoolHop.Required)
                    await new ThreadPoolHop();
                return read;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                logger?.LogWarning("Unexpected call to Read in NetworkHandlerStream");
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                Write(new ReadOnlySpan<byte>(buffer, offset, count));
            }

            public override unsafe void Write(ReadOnlySpan<byte> buffer)
            {
                var networkSender = garnetNetworkHandler.networkSender;
                while (true)
                {
                    networkSender.GetResponseObject();
                    var head = networkSender.GetResponseObjectHead();
                    var tail = networkSender.GetResponseObjectTail();

                    int copyBytes = Math.Min(buffer.Length, (int)(tail - head));

                    buffer.Slice(0, copyBytes).CopyTo(new Span<byte>(head, copyBytes));
                    if (garnetNetworkHandler.sendNoWait)
                        _ = networkSender.SendResponseNoWait(0, copyBytes);
                    else
                        _ = networkSender.SendResponse(0, copyBytes);

                    if (copyBytes == buffer.Length) break;
                    buffer = buffer.Slice(copyBytes);
                }
            }

            public override void Flush()
            {
            }

            // Completes in place, as Flush does nothing: the base implementation would run Flush as a thread-pool task,
            // and SslStream, which flushes after each handshake flight, would continue the handshake on the thread pool.
            public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                Write(buffer.Span);
                return ValueTask.CompletedTask;
            }

            public override int Read(Span<byte> buffer)
            {
                logger?.LogWarning("Unexpected call to Read in NetworkHandlerStream");
                throw new NotImplementedException();
            }

            private async ValueTask<int> ReadAsyncInternalAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                int copiedBytes;
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    copiedBytes = Math.Min(garnetNetworkHandler.networkBytesRead - garnetNetworkHandler.networkReadHead, buffer.Length);

                    // SslStream may perform zero-byte reads, so handle that case by
                    // breaking if there is new data, but we were given a zero-byte buffer
                    if (copiedBytes > 0 || garnetNetworkHandler.networkBytesRead - garnetNetworkHandler.networkReadHead > 0)
                        break;

                    // Set readerStatus to indicate we are waiting for the semaphore
                    garnetNetworkHandler.readerStatus = TlsReaderStatus.Waiting;
                    if (garnetNetworkHandler.expectingData.CurrentCount == 0) garnetNetworkHandler.expectingData.Release();
                    await garnetNetworkHandler.receivedData.WaitAsync(cancellationToken).ConfigureAwait(false);
                    // A handshake waits with its caller's token, which disposing the handler does not cancel. The
                    // wait still holds the buffers, so it must end when the handler is disposed.
                    garnetNetworkHandler.cancellationTokenSource.Token.ThrowIfCancellationRequested();
                    // Releaser of semaphore should have updated readerStatus
                    Debug.Assert(garnetNetworkHandler.readerStatus == TlsReaderStatus.Active || garnetNetworkHandler.disposeCount > 0);
                }

                if (copiedBytes > 0)
                {
                    new ReadOnlySpan<byte>(garnetNetworkHandler.networkReceiveBuffer, garnetNetworkHandler.networkReadHead, copiedBytes).CopyTo(buffer.Span);
                    garnetNetworkHandler.networkReadHead += copiedBytes;
                }
                // Debug.WriteLine($"Network read head: {garnetNetworkHandler.networkReadHead}");
                return copiedBytes;
            }
        }
    }
}