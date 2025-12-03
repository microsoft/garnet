// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
    using BasicGarnetApi = GarnetApi<BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;

    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        readonly ClusterProvider clusterProvider;
        readonly TransactionManager txnManager;
        readonly GarnetSessionMetrics sessionMetrics;
        BasicGarnetApi basicGarnetApi;
        readonly INetworkSender networkSender;
        readonly ILogger logger;
        ClusterSlotVerificationInput csvi;

        // Authenticator used to validate permissions for cluster commands
        readonly IGarnetAuthenticator authenticator;

        // User currently authenticated in this session
        UserHandle userHandle;

        SessionParseState parseState;
        byte* dcurr, dend;
        long _localCurrentEpoch = 0;

        public long LocalCurrentEpoch => _localCurrentEpoch;

        /// <summary>
        /// Indicates if this is a session that allows for reads and writes
        /// </summary>
        bool readWriteSession = false;

        public bool ReadWriteSession => clusterProvider.clusterManager.CurrentConfig.IsPrimary || readWriteSession;

        public void SetReadOnlySession() => readWriteSession = false;
        public void SetReadWriteSession() => readWriteSession = true;

        /// <inheritdoc/>
        public bool IsReplicating { get; private set; }

        /// <inheritdoc/>
        public IGarnetServer Server { get; set; }

        public ClusterSession(ClusterProvider clusterProvider, TransactionManager txnManager, IGarnetAuthenticator authenticator, UserHandle userHandle, GarnetSessionMetrics sessionMetrics, BasicGarnetApi basicGarnetApi, INetworkSender networkSender, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.authenticator = authenticator;
            this.userHandle = userHandle;
            this.txnManager = txnManager;
            this.sessionMetrics = sessionMetrics;
            this.basicGarnetApi = basicGarnetApi;
            this.networkSender = networkSender;
            this.logger = logger;
        }

        public void ProcessClusterCommands(RespCommand command, ref SessionParseState parseState, ref byte* dcurr, ref byte* dend)
        {
            this.dcurr = dcurr;
            this.dend = dend;
            this.parseState = parseState;
            var invalidParameters = false;

            try
            {
                RespCommandsInfo commandInfo = null;
                if (command.IsClusterSubCommand())
                {
                    if (RespCommandsInfo.TryGetRespCommandInfo(command, out commandInfo) && commandInfo.KeySpecifications != null)
                    {
                        csvi.keyNumOffset = -1;
                        clusterProvider.ExtractKeySpecs(commandInfo, command, ref parseState, ref csvi);
                        if (NetworkMultiKeySlotVerifyNoResponse(ref parseState, ref csvi, ref this.dcurr, ref this.dend))
                            return;
                    }

                    ProcessClusterCommands(command, out invalidParameters);
                }
                else
                {
                    _ = command switch
                    {
                        RespCommand.MIGRATE => TryMIGRATE(out invalidParameters),
                        RespCommand.FAILOVER => TryFAILOVER(),
                        RespCommand.SECONDARYOF or RespCommand.REPLICAOF => TryREPLICAOF(out invalidParameters),
                        _ => false
                    };
                }

                if (invalidParameters)
                {
                    var cmdName = commandInfo?.Name ?? RespCommandsInfo.GetRespCommandName(command);
                    var errorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName.ToLowerInvariant());
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref this.dcurr, this.dend))
                        SendAndReset();
                }
            }
            finally
            {
                dcurr = this.dcurr;
                dend = this.dend;
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
                    var task = clusterProvider.storeWrapper.appendOnlyFile.Log.WaitForCommitAsync();
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
        /// <param name="userHandle"><see cref="UserHandle"/> to set as authenticated user.</param>
        public void SetUserHandle(UserHandle userHandle)
        {
            this.userHandle = userHandle;
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

        /// <summary>
        /// NOTE: Unsafe! DO NOT USE, other than benchmarking
        /// </summary>
        /// <param name="replicaOf"></param>
        public void UnsafeSetConfig(string replicaOf = null)
        {
            var config = clusterProvider.clusterManager.CurrentConfig;
            config = config.MakeReplicaOf(replicaOf);
            clusterProvider.clusterManager.UnsafeSetConfig(config);
        }

        public void Dispose()
        {
            // Call dispose on ref of this session if this session is a replication task
            if (IsReplicating)
                replicaReplayTaskGroup?.Dispose();
        }
    }
}