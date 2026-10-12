// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ClusterSession : IClusterSession
    {
        readonly ClusterProvider clusterProvider;
        readonly TransactionManager txnManager;
        readonly GarnetSessionMetrics sessionMetrics;
        BasicGarnetApi basicGarnetApi;
        readonly INetworkSender networkSender;

        /// <summary>
        /// Flushes replies through the RESP session that owns this one, so both write into a single ordered
        /// stream and a reply held back for an AOF commit holds the cluster session's replies back with it.
        /// </summary>
        readonly ResponseFlusher responseFlusher;

        /// <summary>
        /// Shape of the reply a suspended command writes once its wait completes.
        /// </summary>
        enum PendingReply : byte
        {
            /// <summary>No reply recorded; reaching <see cref="CompletePendingCommand"/> with it is a bug.</summary>
            Unset,
            /// <summary>Simple OK.</summary>
            Ok,
            /// <summary>The error in <see cref="pendingError"/>.</summary>
            Error,
            /// <summary>The replication offset as it stands when the wait completes.</summary>
            ReplicationOffset,
        }

        ValueTask pendingCommand;
        bool hasPendingCommand;
        PendingReply pendingReply;
        ReadOnlyMemory<byte> pendingError;

        readonly ILogger logger;
        ClusterSlotVerificationInput csvi;

        // Authenticator used to validate permissions for cluster commands
        readonly IGarnetAuthenticator authenticator;

        // User currently authenticated in this session
        UserHandle userHandle;

        SessionParseState parseState;
        unsafe byte* dcurr, dend;
        long localCurrentEpoch = 0;

        public long LocalCurrentEpoch => Volatile.Read(ref localCurrentEpoch);

        bool readOnlySession;
        bool internalWriteSession;

        public bool ReadOnlySession => readOnlySession;
        public bool IsInternalWriteSession => internalWriteSession;

        public void SetReadOnlySession() => readOnlySession = true;
        public void SetReadWriteSession() => readOnlySession = false;
        public void SetInternalWriteSession() => internalWriteSession = true;

        /// <inheritdoc/>
        public bool IsReplicating { get; private set; }

        /// <inheritdoc/>
        public IGarnetServer Server { get; set; }

        private StringBasicContext stringBasicContext;
        private VectorBasicContext vectorBasicContext;

        /// <summary>
        /// Streaming state for receiving inbound RangeIndex migration data.
        /// Unlike regular keys or VectorSet keys (which are each sent as a single record),
        /// a RangeIndex BfTree snapshot can be large and is transmitted as a sequence of
        /// chunked <c>SerializedRangeIndexStream</c> records
        /// spread across multiple <c>CLUSTER MIGRATE</c> commands on this connection.
        /// The sender serializes all chunks for one key sequentially before moving to the next,
        /// so per-connection state is sufficient to reassemble them.
        /// </summary>
        private readonly RangeIndexMigrationReceiveState rangeIndexMigrationState;

        public ClusterSession(ClusterProvider clusterProvider, TransactionManager txnManager, IGarnetAuthenticator authenticator, UserHandle userHandle, GarnetSessionMetrics sessionMetrics, BasicGarnetApi basicGarnetApi, StringBasicContext stringBasicContext, VectorBasicContext vectorBasicContext, INetworkSender networkSender, ResponseFlusher responseFlusher, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.authenticator = authenticator;
            this.userHandle = userHandle;
            this.txnManager = txnManager;
            this.sessionMetrics = sessionMetrics;
            this.basicGarnetApi = basicGarnetApi;
            this.stringBasicContext = stringBasicContext;
            this.vectorBasicContext = vectorBasicContext;
            this.networkSender = networkSender;
            this.responseFlusher = responseFlusher;
            this.logger = logger;

            if (clusterProvider.serverOptions.EnableRangeIndexPreview)
                rangeIndexMigrationState = new RangeIndexMigrationReceiveState(clusterProvider.storeWrapper.DefaultDatabase.RangeIndexManager, clusterProvider.storeWrapper.appendOnlyFile, logger);
        }

        public unsafe bool ProcessClusterCommands(RespCommand command, VectorManager vectorManager, ref SessionParseState parseState, ref byte* dcurr, ref byte* dend, out ValueTask pending)
        {
            this.dcurr = dcurr;
            this.dend = dend;
            this.parseState = parseState;

            // Cleared per command rather than on completion, so a body that faults -- whose reply the session
            // writes as an error instead of calling CompletePendingCommand -- cannot leave state behind.
            pendingCommand = default;
            hasPendingCommand = false;
            pendingReply = default;
            pendingError = default;

            var invalidParameters = false;

            try
            {
                if (command.IsClusterSubCommand())
                {
                    if (RespCommandsInfo.TryGetSimpleRespCommandInfo(command, out var cmdInfo) && cmdInfo.KeySpecs?.Length > 0)
                    {
                        csvi.keySpecs = cmdInfo.KeySpecs;
                        csvi.isSubCommand = cmdInfo.IsSubCommand;
                        csvi.readOnly = command.IsReadOnly();
                        if (NetworkMultiKeySlotVerifyNoResponse(ref parseState, ref csvi, ref this.dcurr, ref this.dend))
                        {
                            pending = default;
                            return true;
                        }
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
                    var cmdName = RespCommandsInfo.GetRespCommandName(command);
                    var errorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName.ToLowerInvariant());
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref this.dcurr, this.dend))
                        SendAndReset();
                }
            }
            finally
            {
                dcurr = this.dcurr;
                dend = this.dend;

                // this.parseState is a copy of the caller's struct, so it holds its own reference to the
                // session's pinned root buffer. Left populated it survives the owning session's shrink and
                // keeps whatever the widest cluster command ever saw alive for the life of the connection.
                this.parseState = default;
            }

            pending = pendingCommand;
            return !hasPendingCommand;
        }

        /// <summary>
        /// Hands a wait to the RESP session that owns this one, which suspends on it instead of blocking the
        /// network thread and calls <see cref="CompletePendingCommand"/> once it completes.
        /// </summary>
        /// <param name="wait">The work the command must wait for.</param>
        /// <param name="reply">The reply to write when the wait completes.</param>
        bool SuspendOn(ValueTask wait, PendingReply reply)
        {
            pendingCommand = wait;
            hasPendingCommand = true;
            pendingReply = reply;
            return true;
        }

        /// <summary>
        /// Hands over a wait whose body records its own reply through <see cref="SetPendingOutcome"/>.
        /// </summary>
        /// <remarks>
        /// The body of an <c>async</c> method runs up to its first incomplete await before the call
        /// returns, so one that finishes without suspending has already recorded its outcome by the time
        /// this is reached. Naming a reply here would overwrite it, and a failed command would answer with
        /// whatever its success reply was.
        /// </remarks>
        /// <param name="wait">The work the command must wait for.</param>
        bool SuspendOn(ValueTask wait)
        {
            pendingCommand = wait;
            hasPendingCommand = true;
            return true;
        }

        public unsafe void CompletePendingCommand(ref byte* dcurr, ref byte* dend)
        {
            this.dcurr = dcurr;
            this.dend = dend;
            try
            {
                switch (pendingReply)
                {
                    case PendingReply.Ok:
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref this.dcurr, this.dend))
                            SendAndReset();
                        break;

                    case PendingReply.Error:
                        while (!RespWriteUtils.TryWriteError(pendingError.Span, ref this.dcurr, this.dend))
                            SendAndReset();
                        break;

                    case PendingReply.ReplicationOffset:
                        while (!RespWriteUtils.TryWriteAsciiBulkString(clusterProvider.replicationManager.ReplicationOffset.ToString(), ref this.dcurr, this.dend))
                            SendAndReset();
                        break;

                    default:
                        throw new InvalidOperationException($"Unexpected {nameof(PendingReply)}: {pendingReply}");
                }
            }
            finally
            {
                dcurr = this.dcurr;
                dend = this.dend;
            }
        }

        /// <summary>
        /// Records the outcome of a wait that produces either a success or an error message.
        /// </summary>
        void SetPendingOutcome(bool success, ReadOnlyMemory<byte> errorMessage)
        {
            pendingReply = success ? PendingReply.Ok : PendingReply.Error;
            pendingError = success ? default : errorMessage;
        }

        unsafe void SendAndReset() => responseFlusher(ref dcurr, ref dend);

        unsafe void SendAndReset(ref byte* dcurr, ref byte* dend) => responseFlusher(ref dcurr, ref dend);

        /// <summary>
        /// Updates the user currently authenticated in the session.
        /// </summary>
        /// <param name="userHandle"><see cref="UserHandle"/> to set as authenticated user.</param>
        public void SetUserHandle(UserHandle userHandle)
        {
            this.userHandle = userHandle;
        }
        /// <summary>
        /// Announces this session as active at a stable current Garnet epoch.
        /// </summary>
        public void AcquireCurrentEpoch()
        {
            while (true)
            {
                var epoch = clusterProvider.garnetEpoch.GetCurrentEpoch();
                Interlocked.Exchange(ref localCurrentEpoch, epoch);
                if (epoch == clusterProvider.garnetEpoch.GetCurrentEpoch())
                    return;
            }
        }

        /// <summary>
        /// Announces this session as idle after all epoch-protected work is complete.
        /// </summary>
        public void ReleaseCurrentEpoch() => Volatile.Write(ref localCurrentEpoch, 0);

        /// <summary>
        /// Release epoch, wait for config transition and re-acquire the epoch
        /// </summary>
        public async ValueTask<bool> UnsafeBumpAndWaitForEpochTransitionAsync()
        {
            ReleaseCurrentEpoch();
            var result = await clusterProvider.BumpAndWaitForEpochTransitionAsync().ConfigureAwait(false);
            AcquireCurrentEpoch();
            return result;
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

            if (replicaOf != null)
                clusterProvider.replicationManager.ResetReplicaReplayDriverStore();
        }

        public void Dispose()
        {
            rangeIndexMigrationState?.Dispose();

            // A record whose chunks were still arriving when the connection went away holds pooled buffers that no Reset
            // will reach.
            chunkedRecordReassembler?.Dispose();

            // Call dispose on ref of this session if this session is a replication task
            if (IsReplicating)
                replicaReplayDriverStore?.Dispose();
        }
    }
}