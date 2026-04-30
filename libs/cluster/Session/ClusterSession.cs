// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicContext = BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
        /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
        SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>;

    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>,
        BasicContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions,
            /* VectorStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>>;

    using VectorContext = BasicContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>;

    internal sealed partial class ClusterSession : IClusterSession
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
        unsafe byte* dcurr, dend;
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

        private VectorContext vectorContext;
        private BasicContext basicContext;

        public ClusterSession(
            ClusterProvider clusterProvider,
            TransactionManager txnManager,
            IGarnetAuthenticator authenticator,
            UserHandle userHandle,
            GarnetSessionMetrics sessionMetrics,
            BasicGarnetApi basicGarnetApi,
            BasicContext basicContext,
            VectorContext vectorContext,
            INetworkSender networkSender,
            ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.authenticator = authenticator;
            this.userHandle = userHandle;
            this.txnManager = txnManager;
            this.sessionMetrics = sessionMetrics;
            this.basicGarnetApi = basicGarnetApi;
            this.basicContext = basicContext;
            this.vectorContext = vectorContext;
            this.networkSender = networkSender;
            this.logger = logger;
        }

        public unsafe void ProcessClusterCommands(RespCommand command, VectorManager vectorManager, ref SessionParseState parseState, ref byte* dcurr, ref byte* dend)
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

                    ProcessClusterCommands(command, vectorManager, out invalidParameters);
                }
                else
                {
                    _ = command switch
                    {
                        RespCommand.MIGRATE => NetworkTryMIGRATE(out invalidParameters),
                        RespCommand.FAILOVER => TryFAILOVER(),
                        RespCommand.SECONDARYOF or RespCommand.REPLICAOF => NetworkTryREPLICAOF(out invalidParameters),
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

        unsafe void SendAndReset()
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

        unsafe void SendAndReset(ref byte* dcurr, ref byte* dend)
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
        unsafe void Send(byte* d)
        {
            // #if DEBUG
            // logger?.LogTrace("SEND: [{send}]", Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", ""));
            // #endif

            if ((int)(dcurr - d) > 0)
            {
                // Debug.WriteLine("SEND: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "!") + "]");
                if (clusterProvider.storeWrapper.appendOnlyFile != null && clusterProvider.storeWrapper.serverOptions.WaitForCommit)
                {
                    clusterProvider.storeWrapper.appendOnlyFile.WaitForCommit();
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
        public async Task UnsafeBumpAndWaitForEpochTransitionAsync()
        {
            ReleaseCurrentEpoch();
            _ = await clusterProvider.BumpAndWaitForEpochTransitionAsync().ConfigureAwait(false);
            AcquireCurrentEpoch();
        }
    }
}