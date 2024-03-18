// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

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

        public void AcquireCurrentEpoch() => _localCurrentEpoch = clusterProvider.GarnetCurrentEpoch;
        public void ReleaseCurrentEpoch() => _localCurrentEpoch = 0;

        public bool ProcessClusterCommands(ReadOnlySpan<byte> command, ReadOnlySpan<byte> bufSpan, int count, byte* recvBufferPtr, int bytesRead, ref int readHead, ref byte* dcurr, ref byte* dend, out bool result)
        {
            this.recvBufferPtr = recvBufferPtr;
            this.bytesRead = bytesRead;
            this.dcurr = dcurr;
            this.dend = dend;
            this.readHead = readHead;
            result = false;

            try
            {
                if (command.SequenceEqual(CmdStrings.CLUSTER))
                {
                    result = ProcessClusterCommands(bufSpan, count);
                }
                else if (command.SequenceEqual(CmdStrings.MIGRATE))
                {
                    result = TryMIGRATE(count, recvBufferPtr + readHead);
                }
                else if (command.SequenceEqual(CmdStrings.FAILOVER))
                {
                    if (!CheckACLAdminPermissions(bufSpan, count - 1, out bool success))
                    {
                        return success;
                    }

                    result = TryFAILOVER(count, recvBufferPtr + readHead);
                }
                else if (command.SequenceEqual(CmdStrings.REPLICAOF) || command.SequenceEqual(CmdStrings.SECONDARYOF))
                {
                    if (!CheckACLAdminPermissions(bufSpan, count - 1, out bool success))
                    {
                        return success;
                    }

                    result = TryREPLICAOF(count, recvBufferPtr + readHead);
                }
                else
                {
                    return false;
                }
                return true;
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

        bool DrainCommands(ReadOnlySpan<byte> bufSpan, int count)
        {
            for (int i = 0; i < count; i++)
            {
                GetCommand(bufSpan, out bool success1);
                if (!success1) return false;
            }
            return true;
        }

        /// <summary>
        /// Updates the user currently authenticated in the session.
        /// </summary>
        /// <param name="user">User to set as authenticated user.</param>
        public void SetUser(User user)
        {
            this.user = user;
        }

        /// <summary>
        /// Performs @admin command group permission checks for the current user and the given command.
        /// (NOTE: This function is temporary until per-command permissions are implemented)
        /// </summary>
        /// <param name="bufSpan">Buffer containing the current command in RESP3 style.</param>
        /// <param name="count">Number of parameters left in the command specification.</param>
        /// <param name="processingCompleted">Indicates whether the command was completely processed, regardless of whether execution was successful or not.</param>
        /// <returns>True if the command execution is allowed to continue, otherwise false.</returns>
        bool CheckACLAdminPermissions(ReadOnlySpan<byte> bufSpan, int count, out bool processingCompleted)
        {
            Debug.Assert(!authenticator.IsAuthenticated || (user != null));

            if (!authenticator.IsAuthenticated || (!user.CanAccessCategory(CommandCategory.Flag.Admin)))
            {
                if (!DrainCommands(bufSpan, count))
                {
                    processingCompleted = false;
                }
                else
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_NOAUTH, ref dcurr, dend))
                        SendAndReset();
                    processingCompleted = true;
                }
                return false;
            }

            processingCompleted = true;

            return true;
        }

        /// <summary>
        /// Performs @admin command group permission checks for the current user and the given command.
        /// (NOTE: This function is temporary until per-command permissions are implemented)
        /// Does not write to response buffer. Caller responsible for handling error.
        /// </summary>
        /// <returns>True if the command execution is allowed to continue, otherwise false.</returns>
        bool CheckACLAdminPermissions()
        {
            Debug.Assert(!authenticator.IsAuthenticated || (user != null));

            if (!authenticator.IsAuthenticated || (!user.CanAccessCategory(CommandCategory.Flag.Admin)))
                return false;
            return true;
        }

        ReadOnlySpan<byte> GetCommand(ReadOnlySpan<byte> bufSpan, out bool success)
        {
            if (bytesRead - readHead < 6)
            {
                success = false;
                return default;
            }

            Debug.Assert(*(recvBufferPtr + readHead) == '$');
            int psize = *(recvBufferPtr + readHead + 1) - '0';
            readHead += 2;
            while (*(recvBufferPtr + readHead) != '\r')
            {
                psize = psize * 10 + *(recvBufferPtr + readHead) - '0';
                if (bytesRead - readHead < 1)
                {
                    success = false;
                    return default;
                }
                readHead++;
            }
            if (bytesRead - readHead < 2 + psize + 2)
            {
                success = false;
                return default;
            }
            Debug.Assert(*(recvBufferPtr + readHead + 1) == '\n');

            var result = bufSpan.Slice(readHead + 2, psize);
            Debug.Assert(*(recvBufferPtr + readHead + 2 + psize) == '\r');
            Debug.Assert(*(recvBufferPtr + readHead + 2 + psize + 1) == '\n');

            readHead += 2 + psize + 2;
            success = true;
            return result;
        }
    }
}