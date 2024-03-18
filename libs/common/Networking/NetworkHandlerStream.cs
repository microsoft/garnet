// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
                return ReadAsyncInternal(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
            }

            public override ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                return ReadAsyncInternal(destination, cancellationToken);
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
                while (true)
                {
                    garnetNetworkHandler.networkSender.GetResponseObject();
                    var head = garnetNetworkHandler.networkSender.GetResponseObjectHead();
                    var tail = garnetNetworkHandler.networkSender.GetResponseObjectTail();

                    int copyBytes = Math.Min(buffer.Length, (int)(tail - head));

                    buffer.Slice(0, copyBytes).CopyTo(new Span<byte>(head, copyBytes));
                    garnetNetworkHandler.networkSender.SendResponse(0, copyBytes);

                    if (copyBytes == buffer.Length) break;
                    buffer = buffer.Slice(copyBytes);
                }
            }

            public override void Flush()
            {
            }

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

            private async ValueTask<int> ReadAsyncInternal(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                int copiedBytes;
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    copiedBytes = Math.Min(garnetNetworkHandler.networkBytesRead - garnetNetworkHandler.networkReadHead, buffer.Length);

                    // .NET 8 SslStream does zero-byte reads, so we need to handle that case by
                    // breaking if there is new data, but we were given a zero-byte buffer
                    if (copiedBytes > 0 || garnetNetworkHandler.networkBytesRead - garnetNetworkHandler.networkReadHead > 0)
                        break;

                    // Set readerStatus to indicate we are waiting for the semaphore
                    garnetNetworkHandler.readerStatus = TlsReaderStatus.Waiting;
                    await garnetNetworkHandler.receivedData.WaitAsync(cancellationToken).ConfigureAwait(false);
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