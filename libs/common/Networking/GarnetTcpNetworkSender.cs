// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.networking;

namespace Garnet.common
{
    /// <summary>
    /// TCP network sender
    /// </summary>
    public class GarnetTcpNetworkSender : NetworkSenderBase
    {
        /// <summary>
        /// Socket
        /// </summary>
        protected readonly Socket socket;

        /// <summary>
        /// Response object
        /// </summary>
        protected GarnetSaeaBuffer responseObject;

        /// <summary>
        /// Reusable SeaaBuffer
        /// </summary>
        readonly LightConcurrentStack<GarnetSaeaBuffer> saeaStack;

        /// <summary>
        /// Throttle
        /// </summary>
        protected readonly SemaphoreSlim throttle = new(0);

        /// <summary>
        /// Count of sends for throttling
        /// </summary>
        protected int throttleCount;

        /// <summary>
        /// Max concurrent sends (per session) for throttling
        /// </summary>
        protected readonly int ThrottleMax = 8;

        readonly string remoteEndpoint;
        readonly string localEndpoint;

        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// NOTE: This variable should not be marked as readonly as it is a mutable struct
        /// </summary>
        SpinLock spinLock;

        private int closeRequested;

        /// <summary>
        /// GarnetTcpNetworkSender Constructor
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="networkBufferSettings"></param>
        /// <param name="throttleMax"></param>
        public GarnetTcpNetworkSender(
            Socket socket,
            NetworkBufferSettings networkBufferSettings,
            LimitedFixedBufferPool networkPool,
            int throttleMax = 8)
            : base(networkBufferSettings.sendBufferSize)
        {
            this.networkBufferSettings = networkBufferSettings;
            this.networkPool = networkPool;
            this.socket = socket;
            this.saeaStack = new(2 * ThrottleMax);
            this.responseObject = null;
            this.ThrottleMax = throttleMax;
            this.spinLock = new();
            this.closeRequested = 0;

            remoteEndpoint = socket.RemoteEndPoint is IPEndPoint remote ? $"{remote.Address}:{remote.Port}" : "";
            localEndpoint = socket.LocalEndPoint is IPEndPoint local ? $"{local.Address}:{local.Port}" : "";
        }


        /// <inheritdoc />
        public override string RemoteEndpointName => remoteEndpoint;

        /// <inheritdoc />
        public override string LocalEndpointName => localEndpoint;

        /// <inheritdoc />
        public override bool IsLocalConnection()
        {
            if (socket.RemoteEndPoint is IPEndPoint ip)
            {
                return IPAddress.IsLoopback(ip.Address);
            }

            if (socket.RemoteEndPoint is UnixDomainSocketEndPoint)
            {
                return true;
            }

            return false;
        }

        /// <inheritdoc />
        public override void Enter()
        {
            var lockTaken = false;
            spinLock.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
        }

        /// <inheritdoc />
        public override unsafe void EnterAndGetResponseObject(out byte* head, out byte* tail)
        {
            var lockTaken = false;
            spinLock.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            Debug.Assert(responseObject == null);
            if (!saeaStack.TryPop(out responseObject, out bool disposed))
            {
                if (disposed)
                    ThrowDisposed();
                responseObject = new GarnetSaeaBuffer(SeaaBuffer_Completed, networkBufferSettings, networkPool);
            }
            head = responseObject.buffer.entryPtr;
            tail = responseObject.buffer.entryPtr + responseObject.buffer.entry.Length;
        }

        /// <inheritdoc />
        public override void Exit()
        {
            spinLock.Exit();
        }

        /// <inheritdoc />
        public override unsafe void ExitAndReturnResponseObject()
        {
            if (responseObject != null)
            {
                ReturnBuffer(responseObject);
                responseObject = null;
            }
            spinLock.Exit();
        }


        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void GetResponseObject()
        {
            if (responseObject == null)
            {
                if (!saeaStack.TryPop(out responseObject, out bool disposed))
                {
                    if (disposed)
                        ThrowDisposed();
                    responseObject = new GarnetSaeaBuffer(SeaaBuffer_Completed, networkBufferSettings, networkPool);
                }
            }
        }

        static void ThrowDisposed()
            => throw new ObjectDisposedException("GarnetTcpNetworkSender");

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ReturnResponseObject()
        {
            if (responseObject != null)
            {
                ReturnBuffer(responseObject);
                responseObject = null;
            }
        }

        void ReturnBuffer(GarnetSaeaBuffer buffer)
        {
            Debug.Assert(buffer != null);
            if (!saeaStack.TryPush(buffer))
                buffer.Dispose();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override unsafe byte* GetResponseObjectHead()
        {
            if (responseObject != null)
                return responseObject.buffer.entryPtr;
            return base.GetResponseObjectHead();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override unsafe byte* GetResponseObjectTail()
        {
            if (responseObject != null)
                return responseObject.buffer.entryPtr + responseObject.buffer.entry.Length;
            return base.GetResponseObjectTail();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool SendResponse(int offset, int size)
        {
            var _r = responseObject;
            if (_r == null) return false;
            responseObject = null;
            try
            {
                // If this does not throw, _r is ReturnBuffer()ed when it completes.
                Send(socket, _r, offset, size);
            }
            catch
            {
                ReturnBuffer(_r);
                if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                    throttle.Release();
                // Rethrow exception as session is not usable
                throw;
            }
            return true;
        }

        /// <inheritdoc />
        public override void SendResponse(byte[] buffer, int offset, int count, object context)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc />
        public override void SendCallback(object context)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc />
        public override void Dispose() => DisposeNetworkSender(false);

        /// <inheritdoc />
        public override void DisposeNetworkSender(bool waitForSendCompletion)
        {
            if (!waitForSendCompletion)
                socket.Dispose();

            // Wait for ongoing sends to complete
            while (throttleCount >= 0 && Interlocked.CompareExchange(ref throttleCount, int.MinValue, 0) != 0) Thread.Yield();

            // Empty and dispose the stack
            saeaStack.Dispose();

            throttle.Dispose();
            if (waitForSendCompletion)
                socket.Dispose();
        }

        /// <inheritdoc />
        public override void Throttle()
        {
            // Short circuit for common case of no network overload
            if (throttleCount < ThrottleMax) return;

            // We are throttling, so wait for throttle to be released by some ongoing sender
            var cnt = Interlocked.Increment(ref throttleCount);
            if (cnt < 0)
            {
                Interlocked.Decrement(ref cnt);
                return;
            }
            if (cnt > ThrottleMax)
                throttle.Wait();

            // Release throttle, since we used up one slot
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void Send(Socket socket, GarnetSaeaBuffer sendObject, int offset, int size)
        {
            var cnt = Interlocked.Increment(ref throttleCount);
            if (cnt < 0)
            {
                sendObject.socketEventAsyncArgs.UserToken = sendObject;
                SeaaBuffer_Completed(null, sendObject.socketEventAsyncArgs);
                return;
            }
            if (cnt > ThrottleMax)
                throttle.Wait();

            // Debug.WriteLine($"SEND: [{size}][{System.Text.Encoding.UTF8.GetString(new Span<byte>(sendObject.socketEventAsyncArgs.Buffer, offset, size)).Replace("\n", "|").Replace("\r", "")}]");

            // Reset send buffer
            sendObject.socketEventAsyncArgs.SetBuffer(offset, size);
            // Set user context to reusable object handle for disposal when send is done
            sendObject.socketEventAsyncArgs.UserToken = sendObject;
            if (!socket.SendAsync(sendObject.socketEventAsyncArgs))
                SeaaBuffer_Completed(null, sendObject.socketEventAsyncArgs);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
            ReturnBuffer((GarnetSaeaBuffer)e.UserToken);
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();
        }
    }
}