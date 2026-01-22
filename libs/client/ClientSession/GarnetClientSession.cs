// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    /// <summary>
    /// Mono-threaded remote client session for Garnet (a session makes a single network connection, and 
    /// expects mono-threaded client access, i.e., no concurrent invocations of API by client)
    /// </summary>
    public sealed partial class GarnetClientSession : IServerHook, IMessageConsumer
    {
        readonly int bufferSizeDigits;
        INetworkSender networkSender;
        readonly ElasticCircularBuffer<TaskType> tasksTypes = new();
        readonly ElasticCircularBuffer<TaskCompletionSource<string>> tcsQueue = new();
        readonly ElasticCircularBuffer<TaskCompletionSource<string[]>> tcsArrayQueue = new();
        readonly ILogger logger;

        /// <summary>
        /// Max outstanding network sends allowed
        /// </summary>
        readonly int networkSendThrottleMax;

        readonly SslClientAuthenticationOptions sslOptions;
        GarnetClientSessionTcpNetworkHandler networkHandler;

        /// <summary>
        /// Exception to throw to ongoing tasks when disposed
        /// </summary>
        static readonly Exception disposeException = new GarnetClientDisposedException();

        Socket socket;
        int disposed;

        // Send        
        unsafe byte* offset, end;

        // Num outstanding commands
        volatile int numCommands;

        /// <summary>
        /// The host endpoint
        /// </summary>
        public EndPoint EndPoint { get; }

        /// <inheritdoc />
        public bool Disposed => disposed > 0;

        /// <summary>
        /// Whether we are connected to the server
        /// </summary>
        public bool IsConnected => socket != null && socket.Connected && !Disposed;

        /// <summary>
        /// Get raw results from tcs completion
        /// </summary>
        public bool RawResult = false;

        /// <summary>
        /// Username to authenticate the session on the server.
        /// </summary>
        readonly string authUsername = null;

        /// <summary>
        /// Password to authenticate the session on the server.
        /// </summary>
        readonly string authPassword = null;

        /// <summary>
        /// Indicating whether this instance is using its own network pool or one that was provided
        /// </summary>
        readonly bool usingManagedNetworkPool = false;

        /// <summary>
        /// Instance of network buffer settings describing the send and receive buffer sizes
        /// </summary>
        readonly NetworkBufferSettings networkBufferSettings;

        /// <summary>
        /// NetworkPool used to allocate send and receive buffers
        /// </summary>
        readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// Create client instance
        /// </summary>
        /// <param name="endpoint">Endpoint of the server</param>
        /// <param name="tlsOptions">TLS options</param>
        /// <param name="authUsername">Username to authenticate with</param>
        /// <param name="authPassword">Password to authenticate with</param>
        /// <param name="networkBufferSettings">Settings for send and receive network buffers</param>
        /// <param name="networkPool">Buffer pool to use for allocating send and receive buffers</param>
        /// <param name="networkSendThrottleMax">Max outstanding network sends allowed</param>
        /// <param name="logger">Logger</param>
        public GarnetClientSession(
            EndPoint endpoint,
            NetworkBufferSettings networkBufferSettings,
            LimitedFixedBufferPool networkPool = null,
            SslClientAuthenticationOptions tlsOptions = null,
            string authUsername = null,
            string authPassword = null,
            int networkSendThrottleMax = 8,
            bool rawResult = false,
            ILogger logger = null)
        {
            EndPoint = endpoint;

            this.usingManagedNetworkPool = networkPool != null;
            this.networkBufferSettings = networkBufferSettings;
            this.networkPool = networkPool ?? networkBufferSettings.CreateBufferPool();
            this.bufferSizeDigits = NumUtils.CountDigits(this.networkBufferSettings.sendBufferSize);

            this.logger = logger;
            this.sslOptions = tlsOptions;
            this.networkSendThrottleMax = networkSendThrottleMax;
            this.disposed = 0;
            this.authUsername = authUsername;
            this.authPassword = authPassword;
            this.RawResult = rawResult;
        }

        /// <summary>
        /// Connect to server
        /// </summary>
        /// <param name="timeoutMs">Timeout in milliseconds (default 0 for immediate timeout)</param>
        /// <param name="token"></param>
        public unsafe void Connect(int timeoutMs = 0, CancellationToken token = default)
        {
            socket = ConnectSendSocketAsync(timeoutMs, token).ConfigureAwait(false).GetAwaiter().GetResult();
            networkHandler = new GarnetClientSessionTcpNetworkHandler(
                this,
                socket,
                networkBufferSettings,
                networkPool,
                sslOptions != null,
                messageConsumer: this,
                networkSendThrottleMax: networkSendThrottleMax,
                logger: logger);
            networkHandler.StartAsync(sslOptions, EndPoint.ToString(), token).ConfigureAwait(false).GetAwaiter().GetResult();
            networkSender = networkHandler.GetNetworkSender();
            networkSender.GetResponseObject();
            offset = networkSender.GetResponseObjectHead();
            end = networkSender.GetResponseObjectTail();
            numCommands = 0;

            try
            {
                if (authUsername != null)
                {
                    ExecuteAsync("AUTH", authUsername, authPassword == null ? "" : authPassword).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                else if (authPassword != null)
                {
                    ExecuteAsync("AUTH", authPassword).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AUTH returned error");
                throw;
            }
        }

        /// <summary>
        /// Connect client send socket
        /// </summary>
        /// <param name="millisecondsTimeout"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task<Socket> ConnectSendSocketAsync(int millisecondsTimeout = 0, CancellationToken cancellationToken = default)
        {
            if (EndPoint is DnsEndPoint dnsEndpoint)
            {
                var hostEntries = await Dns.GetHostEntryAsync(dnsEndpoint.Host, cancellationToken).ConfigureAwait(false);
                // Try all available DNS entries if a hostName is provided
                foreach (var addressEntry in hostEntries.AddressList)
                {
                    var endpoint = new IPEndPoint(addressEntry, dnsEndpoint.Port);
                    var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                    {
                        NoDelay = true
                    };

                    if (await TryConnectSocketAsync(socket, endpoint, millisecondsTimeout, cancellationToken))
                        return socket;
                }
            }
            else
            {
                var socket = new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
                if (EndPoint is not UnixDomainSocketEndPoint)
                    socket.NoDelay = true;

                if (await TryConnectSocketAsync(socket, EndPoint, millisecondsTimeout, cancellationToken))
                    return socket;
            }

            logger?.LogWarning("Failed to connect at {endpoint}", EndPoint);
            throw new Exception($"Failed to connect at {EndPoint}");
        }

        /// <summary>
        /// Try to establish connection for <paramref name="socket"/> using <paramref name="endpoint"/>
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="endpoint"></param>
        /// <param name="millisecondsTimeout"></param>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <returns></returns>
        private async Task<bool> TryConnectSocketAsync(Socket socket, EndPoint endpoint, int millisecondsTimeout, CancellationToken cancellationToken = default)
        {
            try
            {
                if (millisecondsTimeout > 0)
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                    var connectTask = socket.ConnectAsync(endpoint, timeoutCts.Token).AsTask();
                    if (await Task.WhenAny(connectTask, Task.Delay(millisecondsTimeout, timeoutCts.Token)) == connectTask)
                    {
                        // Task completed within timeout.
                        // Consider that the task may have faulted or been canceled.
                        // We re-await the task so that any exceptions/cancellation is rethrown.
                        await connectTask;
                    }
                    else
                    {
                        timeoutCts.Cancel();
                    }

                    if (!socket.Connected)
                    {
                        socket.Close();
                        throw new Exception($"Failed to connect server {endpoint}.");
                    }
                }
                else
                {
                    await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed at GarnetClient.TryConnectSocketAsync");
                socket.Dispose();
                return false;
            }

            return true;
        }

        /// <summary>
        /// Reconnect to server
        /// </summary>
        public void Reconnect(int timeoutMs = 0, CancellationToken token = default)
        {
            if (Disposed) throw new ObjectDisposedException("GarnetClientSession");
            try
            {
                networkSender?.ReturnResponseObject();
                socket?.Dispose();
                networkHandler?.Dispose();
            }
            catch { }
            Connect(timeoutMs, token);
        }

        /// <summary>
        /// Dispose instance
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Increment(ref disposed) > 1) return;

            networkSender?.ReturnResponseObject();
            socket?.Dispose();
            networkHandler?.Dispose();
            if (!usingManagedNetworkPool) networkPool.Dispose();
        }

        /// <summary>
        /// Execute remote command
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public void Execute(params string[] command)
        {
            tcsQueue.Enqueue(null);
            InternalExecute(command);
            Flush();
        }

        /// <summary>
        /// Execute remote command
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public void ExecuteBatch(params string[] command)
        {
            tcsQueue.Enqueue(null);
            InternalExecute(command);
        }

        /// <summary>
        /// Execute remote command (for array return type)
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public void ExecuteForArray(params string[] command)
        {
            tcsArrayQueue.Enqueue(null);
            InternalExecute(command);
            Flush();
        }

        static ReadOnlySpan<byte> CLUSTER => "$7\r\nCLUSTER\r\n"u8;
        static ReadOnlySpan<byte> appendLog => "APPENDLOG"u8;

        /// <summary>
        /// ClusterAppendLog
        /// </summary>
        /// <seealso cref="T:Garnet.cluster.ClusterSession.NetworkClusterAppendLog"/>
        /// <param name="nodeId"></param>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        /// <param name="payloadPtr"></param>
        /// <param name="payloadLength"></param>
        /// <exception cref="Exception"></exception>
        public unsafe void ExecuteClusterAppendLog(string nodeId, int physicalSublogIdx, long previousAddress, long currentAddress, long nextAddress, long payloadPtr, int payloadLength)
        {
            Debug.Assert(nodeId != null);

            var curr = offset;
            var arraySize = 8;

            while (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 1
            while (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 2
            while (!RespWriteUtils.TryWriteBulkString(appendLog, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 3
            while (!RespWriteUtils.TryWriteAsciiBulkString(nodeId, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 4
            while (!RespWriteUtils.TryWriteArrayItem(physicalSublogIdx, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 5
            while (!RespWriteUtils.TryWriteArrayItem(previousAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 6
            while (!RespWriteUtils.TryWriteArrayItem(currentAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            // 7
            while (!RespWriteUtils.TryWriteArrayItem(nextAddress, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            if (payloadLength > networkBufferSettings.sendBufferSize)
                throw new Exception($"Payload length {payloadLength} is larger than bufferSize {networkBufferSettings.sendBufferSize} bytes");

            // 8
            while (!RespWriteUtils.TryWriteBulkString(new Span<byte>((void*)payloadPtr, payloadLength), ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;
        }

        /// <summary>
        /// Throttle the network sender, potentially blocking
        /// </summary>
        public void Throttle()
            => networkSender.Throttle();

        /// <summary>
        /// Flush current buffer of outgoing messages. Optionally spin-wait for all responses to be received and processed.
        /// </summary>
        public void CompletePending(bool wait = true)
        {
            Flush();
            if (wait) Wait();
        }

        /// <summary>
        /// Wait for responses to arrive
        /// </summary>
        public void Wait()
        {
            while (numCommands > 0)
            {
                Thread.Yield();
            }
        }

        /// <summary>
        /// Issue command for execution
        /// </summary>
        /// <param name="command"></param>
        private unsafe void InternalExecute(params string[] command)
        {
            byte* curr = offset;
            while (!RespWriteUtils.TryWriteArrayLength(command.Length, ref curr, end))
            {
                Flush();
                curr = offset;
            }
            offset = curr;

            foreach (var cmd in command)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(cmd, ref curr, end))
                {
                    Flush();
                    curr = offset;
                }
                offset = curr;
            }

            Interlocked.Increment(ref numCommands);
            return;
        }

        private unsafe int ProcessReplies(byte* recvBufferPtr, int bytesRead)
        {
            // Debug.WriteLine("RECV: [" + Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, bytesRead)).Replace("\n", "|").Replace("\r", "") + "]");

            string result = null;
            string[] resultArray = null;
            bool isArray = false;
            byte* ptr = recvBufferPtr;
            bool error = false;
            bool success = true;
            int readHead = 0;

            while (readHead < bytesRead)
            {
                if (RawResult)
                {
                    if (RespReadUtils.TryReadString(out var tmp, ref ptr, recvBufferPtr + bytesRead))
                    {
                        result += tmp;
                        result += "\r\n";
                    }
                    else
                    {
                        success = false;
                    }
                }
                else
                {
                    switch (*ptr)
                    {
                        case (byte)'+':
                            if (!RespReadResponseUtils.TryReadSimpleString(out result, ref ptr, recvBufferPtr + bytesRead))
                                success = false;
                            break;
                        case (byte)':':
                            if (!RespReadResponseUtils.TryReadIntegerAsString(out result, ref ptr, recvBufferPtr + bytesRead))
                                success = false;
                            break;

                        case (byte)'-':
                            error = true;
                            if (!RespReadResponseUtils.TryReadErrorAsString(out result, ref ptr, recvBufferPtr + bytesRead))
                                success = false;
                            break;

                        case (byte)'$':
                            if (!RespReadResponseUtils.TryReadStringWithLengthHeader(out result, ref ptr, recvBufferPtr + bytesRead))
                                success = false;
                            break;

                        case (byte)'*':
                            isArray = true;
                            if (!RespReadResponseUtils.TryReadStringArrayWithLengthHeader(out resultArray, ref ptr, recvBufferPtr + bytesRead))
                                success = false;
                            break;

                        default:
                            throw new Exception("Unexpected response: " + Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, bytesRead)).Replace("\n", "|").Replace("\r", "") + "]");
                    }
                }

                if (!success) return readHead;
                readHead = (int)(ptr - recvBufferPtr);

                Interlocked.Decrement(ref numCommands);
                if (isArray)
                {
                    var tcs = tcsArrayQueue.Dequeue();
                    tcs?.SetResult(resultArray);
                }
                else if (!RawResult)
                {
                    var tcs = tcsQueue.Dequeue();
                    if (error) tcs?.SetException(new Exception(result));
                    else tcs?.SetResult(result);
                }
            }

            if (RawResult)
            {
                var tcs = tcsQueue.Dequeue();
                if (error) tcs?.SetException(new Exception(result));
                else tcs?.SetResult(result);
            }

            return readHead;
        }

        /// <summary>
        /// Flush current buffer of outgoing messages. Does not wait for responses.
        /// </summary>
        private unsafe void Flush()
        {
            if (offset > networkSender.GetResponseObjectHead())
            {
                int payloadSize = (int)(offset - networkSender.GetResponseObjectHead());

                try
                {
                    networkSender.SendResponse(0, payloadSize);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Exception calling networkSender.SendResponse in GarnetClientSession.Flush");
                    Dispose();
                    throw;
                }
                ResetOffset();
            }
        }

        private unsafe void ResetOffset()
        {
            networkSender.GetResponseObject();
            offset = networkSender.GetResponseObjectHead();
            end = networkSender.GetResponseObjectTail();
        }

        /// <inheritdoc />
        public bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session)
            => throw new NotSupportedException();

        /// <inheritdoc />
        public void DisposeMessageConsumer(INetworkHandler session)
        {
            while (!tcsArrayQueue.IsEmpty())
            {
                var tcs = tcsArrayQueue.Dequeue();
                tcs?.TrySetException(disposeException);
            }
            while (!tcsQueue.IsEmpty())
            {
                var tcs = tcsQueue.Dequeue();
                tcs?.TrySetException(disposeException);
            }
        }

        /// <inheritdoc />
        public unsafe int TryConsumeMessages(byte* reqBuffer, int bytesRead)
            => ProcessReplies(reqBuffer, bytesRead);
    }
}