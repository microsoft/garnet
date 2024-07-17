// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.common.Parsing;
using Garnet.networking;
using Garnet.server;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;

    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        readonly ClusterProvider clusterProvider;
        readonly TransactionManager txnManager;
        readonly GarnetSessionMetrics sessionMetrics;
        BasicGarnetApi basicGarnetApi;
        readonly INetworkSender networkSender;
        readonly ILogger logger;

        // Authenticator used to validate permissions for cluster commands
        readonly IGarnetAuthenticator authenticator;

        // User currently authenticated in this session
        User user;

        byte* dcurr, dend;
        byte* recvBufferPtr;
        int readHead, bytesRead;
        long _localCurrentEpoch = 0;

        public long LocalCurrentEpoch => _localCurrentEpoch;

        /// <summary>
        /// Indicates if this is a session that allows for reads and writes
        /// </summary>
        bool readWriteSession = false;

        public bool ReadWriteSession => clusterProvider.clusterManager.CurrentConfig.IsPrimary || readWriteSession;

        public void SetReadOnlySession() => readWriteSession = false;
        public void SetReadWriteSession() => readWriteSession = true;

        public ClusterSession(ClusterProvider clusterProvider, TransactionManager txnManager, IGarnetAuthenticator authenticator, User user, GarnetSessionMetrics sessionMetrics, BasicGarnetApi basicGarnetApi, INetworkSender networkSender, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.authenticator = authenticator;
            this.user = user;
            this.txnManager = txnManager;
            this.sessionMetrics = sessionMetrics;
            this.basicGarnetApi = basicGarnetApi;
            this.networkSender = networkSender;
            this.logger = logger;
        }

        public bool ProcessClusterCommands(RespCommand command, int count, byte* recvBufferPtr, int bytesRead, ref int readHead, ref byte* dcurr, ref byte* dend, out bool result)
        {
            this.recvBufferPtr = recvBufferPtr;
            this.bytesRead = bytesRead;
            this.dcurr = dcurr;
            this.dend = dend;
            this.readHead = readHead;
            bool ret;
            try
            {
                if (command.IsClusterSubCommand())
                {
                    result = ProcessClusterCommands(command, count);
                    ret = true;
                }
                else
                {
                    (ret, result) = command switch
                    {
                        RespCommand.MIGRATE => (true, TryMIGRATE(count, recvBufferPtr + readHead)),
                        RespCommand.FAILOVER => (true, TryFAILOVER(count, recvBufferPtr + readHead)),
                        RespCommand.SECONDARYOF or RespCommand.REPLICAOF => (true, TryREPLICAOF(count, recvBufferPtr + readHead)),
                        _ => (false, false)
                    };
                }

                return ret;
            }
            finally
            {
                dcurr = this.dcurr;
                dend = this.dend;
                readHead = this.readHead;
            }
        }

        void SendAndReset()
        {
            byte* d = networkSender.GetResponseObjectHead();
            if ((int)(dcurr - d) > 0)
            {
                Send(d);
                networkSender.GetResponseObject();
                dcurr = networkSender.GetResponseObjectHead();
                dend = networkSender.GetResponseObjectTail();
            }
        }

        void SendAndReset(ref byte* dcurr, ref byte* dend)
        {
            byte* d = networkSender.GetResponseObjectHead();
            if ((int)(dcurr - d) > 0)
            {
                Send(d);
                networkSender.GetResponseObject();
                dcurr = networkSender.GetResponseObjectHead();
                dend = networkSender.GetResponseObjectTail();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Send(byte* d)
        {
            // #if DEBUG
            // logger?.LogTrace("SEND: [{send}]", Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", ""));
            // #endif

            if ((int)(dcurr - d) > 0)
            {
                // Debug.WriteLine("SEND: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "!") + "]");
                if (clusterProvider.storeWrapper.appendOnlyFile != null && clusterProvider.storeWrapper.serverOptions.WaitForCommit)
                {
                    var task = clusterProvider.storeWrapper.appendOnlyFile.WaitForCommitAsync();
                    if (!task.IsCompleted) task.AsTask().GetAwaiter().GetResult();
                }
                int sendBytes = (int)(dcurr - d);
                networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), sendBytes);
                sessionMetrics?.incr_total_net_output_bytes((ulong)sendBytes);
            }
        }

        /// <summary>
        /// Updates the user currently authenticated in the session.
        /// </summary>
        /// <param name="user">User to set as authenticated user.</param>
        public void SetUser(User user)
        {
            this.user = user;
        }

        bool DrainCommands(int count)
        {
            for (var i = 0; i < count; i++)
            {
                if (!SkipCommand()) return false;
            }
            return true;
        }

        bool SkipCommand()
        {
            var ptr = recvBufferPtr + readHead;
            var end = recvBufferPtr + bytesRead;

            // Try to read the command length
            if (!RespReadUtils.ReadUnsignedLengthHeader(out int length, ref ptr, end))
            {
                return false;
            }

            readHead = (int)(ptr - recvBufferPtr);

            // Try to read the command value
            ptr += length;
            if (ptr + 2 > end)
            {
                return false;
            }

            if (*(ushort*)ptr != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            readHead += length + 2;

            return true;
        }

        public void AcquireCurrentEpoch() => _localCurrentEpoch = clusterProvider.GarnetCurrentEpoch;
        public void ReleaseCurrentEpoch() => _localCurrentEpoch = 0;

        /// <summary>
        /// Release epoch, wait for config transition and re-acquire the epoch
        /// </summary>
        public void UnsafeBumpAndWaitForEpochTransition()
        {
            ReleaseCurrentEpoch();
            _ = clusterProvider.BumpAndWaitForEpochTransition();
            AcquireCurrentEpoch();
        }
    }
}