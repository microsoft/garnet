// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.common.Parsing;
using Garnet.networking;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<TransientSessionLocker, GarnetSafeEpochGuard>;
    using LockableGarnetApi = GarnetApi<TransactionalSessionLocker, GarnetSafeEpochGuard>;

    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        readonly GarnetSessionMetrics sessionMetrics;
        readonly GarnetLatencyMetricsSession LatencyMetrics;

        public GarnetLatencyMetricsSession latencyMetrics => LatencyMetrics;

        /// <summary>
        /// Get a copy of sessionMetrics
        /// </summary>
        public GarnetSessionMetrics GetSessionMetrics => sessionMetrics;

        /// <summary>
        /// Get a copy of latencyMetrics
        /// </summary>
        public GarnetLatencyMetricsSession GetLatencyMetrics() => LatencyMetrics;

        /// <summary>
        /// Reset latencyMetrics for eventType
        /// </summary>
        public void ResetLatencyMetrics(LatencyMetricsType latencyEvent) => latencyMetrics?.Reset(latencyEvent);

        /// <summary>
        /// Reset all latencyMetrics
        /// </summary>
        public void ResetAllLatencyMetrics() => latencyMetrics?.ResetAll();

        internal readonly StoreWrapper storeWrapper;
        internal readonly TransactionManager txnManager;
        internal readonly ScratchBufferManager scratchBufferManager;

        internal SessionParseState parseState;
        internal ArgSlice[] parseStateBuffer;
        ClusterSlotVerificationInput csvi;
        GCHandle recvHandle;

        /// <summary>
        /// Pointer to the (fixed) receive buffer
        /// </summary>
        byte* recvBufferPtr;

        /// <summary>
        /// Current readHead. On successful parsing, this is left at the start of 
        /// the command payload for use by legacy operators.
        /// </summary>
        int readHead;

        /// <summary>
        /// End of the current command, after successful parsing.
        /// </summary>
        int endReadHead;

        byte* dcurr, dend;
        bool toDispose;

        internal StorageSession storageSession;
        internal BasicGarnetApi basicGarnetApi;
        internal LockableGarnetApi lockableGarnetApi;

        int opCount;

        readonly IGarnetAuthenticator _authenticator;

        /// <summary>
        /// The user currently authenticated in this session
        /// </summary>
        User _user = null;

        readonly ILogger logger = null;

        /// <summary>
        /// Clients must enable asking to make node respond to requests on slots that are being imported.
        /// </summary>
        public byte SessionAsking { get; set; }

        /// <summary>
        /// If set, commands can use this to enumerate details about the server or other sessions.
        /// 
        /// It is not guaranteed to be set.
        /// </summary>
        public IGarnetServer Server { get; set; }

        // Track whether the incoming network batch contains slow commands that should not be counter in NET_RS histogram
        bool containsSlowCommand;

        readonly CustomCommandManagerSession customCommandManagerSession;

        /// <summary>
        /// Cluster session
        /// </summary>
        public readonly IClusterSession clusterSession;

        /// <summary>
        /// Current custom transaction to be executed in the session.
        /// </summary>
        CustomTransaction currentCustomTransaction = null;

        /// <summary>
        /// Current custom command to be executed in the session.
        /// </summary>
        CustomRawStringCommand currentCustomRawStringCommand = null;

        /// <summary>
        /// Current custom object command to be executed in the session.
        /// </summary>
        CustomObjectCommand currentCustomObjectCommand = null;

        /// <summary>
        /// Current custom command to be executed in the session.
        /// </summary>
        CustomProcedureWrapper currentCustomProcedure = null;

        /// <summary>
        /// RESP protocol version (RESP2 is the default)
        /// </summary>
        byte respProtocolVersion = 2;

        /// <summary>
        /// Client name for the session
        /// </summary>
        string clientName = null;

        /// <summary>
        /// Flag indicating whether any of the commands in one message
        /// requires us to block on AOF before sending response over the network
        /// </summary>
        bool waitForAofBlocking = false;

        /// <summary>
        /// A per-session cache for storing lua scripts
        /// </summary>
        internal readonly SessionScriptCache sessionScriptCache;

        /// <summary>
        /// Identifier for session - used for CLIENT and related commands.
        /// </summary>
        public long Id { get; }

        /// <summary>
        /// <see cref="Environment.TickCount64"/> when this <see cref="RespServerSession"/> was created.
        /// </summary>
        public long CreationTicks { get; }

        internal TsavoriteKernel TsavoriteKernel => storeWrapper.TsavoriteKernel;

        public RespServerSession(
            long id,
            INetworkSender networkSender,
            StoreWrapper storeWrapper,
            SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscribeBroker,
            IGarnetAuthenticator authenticator,
            bool enableScripts)
            : base(networkSender)
        {
            this.customCommandManagerSession = new CustomCommandManagerSession(storeWrapper.customCommandManager);
            this.sessionMetrics = storeWrapper.serverOptions.MetricsSamplingFrequency > 0 ? new GarnetSessionMetrics() : null;
            this.LatencyMetrics = storeWrapper.serverOptions.LatencyMonitor ? new GarnetLatencyMetricsSession(storeWrapper.monitor) : null;
            logger = storeWrapper.sessionLogger != null ? new SessionLogger(storeWrapper.sessionLogger, $"[{networkSender?.RemoteEndpointName}] [{GetHashCode():X8}] ") : null;

            this.Id = id;
            this.CreationTicks = Environment.TickCount64;

            logger?.LogDebug("Starting RespServerSession Id={0}", this.Id);

            // Initialize session-local scratch buffer of size 64 bytes, used for constructing arguments in GarnetApi
            this.scratchBufferManager = new ScratchBufferManager();

            // Create storage session and API
            this.storageSession = new StorageSession(storeWrapper, scratchBufferManager, sessionMetrics, LatencyMetrics, logger);
            this.basicGarnetApi = new BasicGarnetApi(storageSession);
            this.lockableGarnetApi = new LockableGarnetApi(storageSession);

            this.storeWrapper = storeWrapper;
            this.subscribeBroker = subscribeBroker;
            this._authenticator = authenticator ?? storeWrapper.serverOptions.AuthSettings?.CreateAuthenticator(this.storeWrapper) ?? new GarnetNoAuthAuthenticator();

            if (storeWrapper.serverOptions.EnableLua && enableScripts)
                sessionScriptCache = new(storeWrapper, _authenticator, logger);

            // Associate new session with default user and automatically authenticate, if possible
            this.AuthenticateUser(Encoding.ASCII.GetBytes(this.storeWrapper.accessControlList.GetDefaultUser().Name));

            txnManager = new TransactionManager(storeWrapper.TsavoriteKernel, this, storageSession, scratchBufferManager, storeWrapper.serverOptions.EnableCluster, logger);
            storageSession.txnManager = txnManager;

            clusterSession = storeWrapper.clusterProvider?.CreateClusterSession(txnManager, this._authenticator, this._user, sessionMetrics, basicGarnetApi, networkSender, logger);
            clusterSession?.SetUser(this._user);
            sessionScriptCache?.SetUser(this._user);

            parseState.Initialize(ref parseStateBuffer);
            readHead = 0;
            toDispose = false;
            SessionAsking = 0;

            // Reserve minimum 4 bytes to send pending sequence number as output
            if (this.networkSender != null)
            {
                if (this.networkSender.GetMaxSizeSettings?.MaxOutputSize < sizeof(int))
                    this.networkSender.GetMaxSizeSettings.MaxOutputSize = sizeof(int);
            }
        }

        internal void SetUser(User user)
        {
            this._user = user;
            clusterSession?.SetUser(user);
        }

        public override void Dispose()
        {
            logger?.LogDebug("Disposing RespServerSession Id={0}", this.Id);

            if (recvBufferPtr != null)
            {
                try { if (recvHandle.IsAllocated) recvHandle.Free(); } catch { }
            }

            if (storeWrapper.serverOptions.MetricsSamplingFrequency > 0 || storeWrapper.serverOptions.LatencyMonitor)
                storeWrapper.monitor.AddMetricsHistorySessionDispose(sessionMetrics, latencyMetrics);

            subscribeBroker?.RemoveSubscription(this);
            storeWrapper.itemBroker?.HandleSessionDisposed(this);
            sessionScriptCache?.Dispose();

            // Cancel the async processor, if any
            asyncWaiterCancel?.Cancel();
            asyncWaiter?.Signal();

            storageSession.Dispose();
        }

        public int StoreSessionID => storageSession.SessionID;

        /// <summary>
        /// Tries to authenticate the given username/password and updates the user associated with this server session.
        /// </summary>
        /// <param name="username">Name of the user to authenticate.</param>
        /// <param name="password">Password to authenticate with.</param>
        /// <returns>True if the session has been authenticated successfully, false if the user could not be authenticated.</returns>
        bool AuthenticateUser(ReadOnlySpan<byte> username, ReadOnlySpan<byte> password = default)
        {
            // Authenticate user or change to default user if no authentication is supported
            var success = !_authenticator.CanAuthenticate || _authenticator.Authenticate(password, username);

            if (success)
            {
                // Set authenticated user or fall back to default user, if separate users are not supported
                // NOTE: Currently only GarnetACLAuthenticator supports multiple users
                _user = _authenticator is GarnetACLAuthenticator aclAuthenticator
                    ? aclAuthenticator.GetUser()
                    : storeWrapper.accessControlList.GetDefaultUser();

                // Propagate authentication to cluster session
                clusterSession?.SetUser(_user);
                sessionScriptCache?.SetUser(_user);
            }

            return _authenticator.CanAuthenticate && success;
        }

        public override int TryConsumeMessages(byte* reqBuffer, int bytesReceived)
        {
            bytesRead = bytesReceived;
            if (!txnManager.IsSkippingOperations())
                readHead = 0;
            try
            {
                latencyMetrics?.Start(LatencyMetricsType.NET_RS_LAT);
                clusterSession?.AcquireCurrentEpoch();
                recvBufferPtr = reqBuffer;
                networkSender.EnterAndGetResponseObject(out dcurr, out dend);
                ProcessMessages();
                recvBufferPtr = null;
            }
            catch (RespParsingException ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger?.Log(ex.LogLevel, ex, "Aborting open session due to RESP parsing error");

                // Forward parsing error as RESP error
                while (!RespWriteUtils.WriteError($"ERR Protocol Error: {ex.Message}", ref dcurr, dend))
                    SendAndReset();

                // Send message and dispose the network sender to end the session
                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());

                // The session is no longer usable, dispose it
                networkSender.DisposeNetworkSender(true);
            }
            catch (GarnetException ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger?.Log(ex.LogLevel, ex, "ProcessMessages threw a GarnetException:");

                // Forward Garnet error as RESP error
                if (ex.ClientResponse)
                {
                    while (!RespWriteUtils.WriteError($"ERR Garnet Exception: {ex.Message}", ref dcurr, dend))
                        SendAndReset();
                }

                // Send message and dispose the network sender to end the session
                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());

                // The session is no longer usable, dispose it
                networkSender.DisposeNetworkSender(true);
            }
            catch (Exception ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger?.LogCritical(ex, "ProcessMessages threw an exception:");
                // The session is no longer usable, dispose it
                networkSender.Dispose();
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
                clusterSession?.ReleaseCurrentEpoch();
                scratchBufferManager.Reset();
            }

            if (txnManager.IsSkippingOperations())
                return 0; // so that network does not try to shift the byte array

            // If server processed input data successfully, update tracked metrics
            if (readHead > 0)
            {
                if (latencyMetrics != null)
                {
                    if (containsSlowCommand)
                    {
                        latencyMetrics.StopAndSwitch(LatencyMetricsType.NET_RS_LAT, LatencyMetricsType.NET_RS_LAT_ADMIN);
                        containsSlowCommand = false;
                    }
                    else
                        latencyMetrics.Stop(LatencyMetricsType.NET_RS_LAT);
                    latencyMetrics.RecordValue(LatencyMetricsType.NET_RS_BYTES, readHead);
                    latencyMetrics.RecordValue(LatencyMetricsType.NET_RS_OPS, opCount);
                    opCount = 0;
                }
                sessionMetrics?.incr_total_net_input_bytes((ulong)readHead);
            }
            return readHead;
        }

        internal void SetTransactionMode(bool enable)
            => txnManager.state = enable ? TxnState.Running : TxnState.None;

        private void ProcessMessages()
        {
            // #if DEBUG
            // logger?.LogTrace("RECV: [{recv}]", Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, bytesRead)).Replace("\n", "|").Replace("\r", ""));
            // Debug.WriteLine($"RECV: [{Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, bytesRead)).Replace("\n", "|").Replace("\r", "")}]");
            // #endif

            var _origReadHead = readHead;

            while (bytesRead - readHead >= 4)
            {
                // First, parse the command, making sure we have the entire command available
                // We use endReadHead to track the end of the current command
                // On success, readHead is left at the start of the command payload for legacy operators
                var cmd = ParseCommand(out var commandReceived);

                // If the command was not fully received, reset addresses and break out
                if (!commandReceived)
                {
                    endReadHead = readHead = _origReadHead;
                    break;
                }

                // Check ACL permissions for the command
                if (cmd != RespCommand.INVALID)
                {
                    if (CheckACLPermissions(cmd))
                    {
                        if (txnManager.state != TxnState.None)
                        {
                            if (txnManager.state == TxnState.Running)
                            {
                                _ = ProcessBasicCommands<TransactionalSessionLocker, GarnetSafeEpochGuard, LockableGarnetApi>(cmd, ref lockableGarnetApi);
                            }
                            else _ = cmd switch
                            {
                                RespCommand.EXEC => NetworkEXEC(),
                                RespCommand.MULTI => NetworkMULTI(),
                                RespCommand.DISCARD => NetworkDISCARD(),
                                RespCommand.QUIT => NetworkQUIT(),
                                _ => NetworkSKIP(cmd),
                            };
                        }
                        else
                        {
                            if (clusterSession == null || CanServeSlot(cmd))
                                _ = ProcessBasicCommands<TransientSessionLocker, GarnetSafeEpochGuard, BasicGarnetApi>(cmd, ref basicGarnetApi);
                        }
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOAUTH, ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    containsSlowCommand = true;
                }

                // Advance read head variables to process the next command
                _origReadHead = readHead = endReadHead;

                // Handle metrics and special cases
                if (latencyMetrics != null) opCount++;
                if (sessionMetrics != null)
                {
                    sessionMetrics.total_commands_processed++;

                    sessionMetrics.total_write_commands_processed += cmd.OneIfWrite();
                    sessionMetrics.total_read_commands_processed += cmd.OneIfRead();
                }
                if (SessionAsking != 0)
                    SessionAsking = (byte)(SessionAsking - 1);
            }

            if (dcurr > networkSender.GetResponseObjectHead())
            {
                Send(networkSender.GetResponseObjectHead());
                if (toDispose)
                {
                    networkSender.DisposeNetworkSender(true);
                }
            }
        }

        // Make first command in string as uppercase
        private bool MakeUpperCase(byte* ptr)
        {
            var tmp = ptr;

            while (tmp < ptr + bytesRead - readHead)
            {
                if (*tmp > 64) // found string
                {
                    var ret = false;
                    while (*tmp > 64 && *tmp < 123 && tmp < ptr + bytesRead - readHead)
                    {
                        if (*tmp > 96)
                        {
                            ret = true;
                            *tmp -= 32;
                        }
                        tmp++;
                    }
                    return ret;
                }
                tmp++;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ProcessBasicCommands<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand cmd, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            /*
             * WARNING: Do not add any command here classified as @slow!
             * Only @fast commands otherwise latency tracking will break for NET_RS (check how containsSlowCommand is used).
             */
            _ = cmd switch
            {
                RespCommand.GET => NetworkGET<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SET => NetworkSET<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SETEX => NetworkSETEX<TKeyLocker, TEpochGuard, TGarnetApi>(false, ref garnetApi),
                RespCommand.PSETEX => NetworkSETEX<TKeyLocker, TEpochGuard, TGarnetApi>(true, ref garnetApi),
                RespCommand.SETEXNX => NetworkSETEXNX<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.DEL => NetworkDEL<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.RENAME => NetworkRENAME<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.RENAMENX => NetworkRENAMENX<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.EXISTS => NetworkEXISTS<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.EXPIRE => NetworkEXPIRE<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.EXPIRE, ref garnetApi),
                RespCommand.PEXPIRE => NetworkEXPIRE<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.PEXPIRE, ref garnetApi),
                RespCommand.TTL => NetworkTTL<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.TTL, ref garnetApi),
                RespCommand.PTTL => NetworkTTL<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.PTTL, ref garnetApi),
                RespCommand.PERSIST => NetworkPERSIST<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.GETRANGE => NetworkGetRange<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SETRANGE => NetworkSetRange<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.GETDEL => NetworkGETDEL<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.APPEND => NetworkAppend<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.STRLEN => NetworkSTRLEN<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.INCR => NetworkIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.INCR, ref garnetApi),
                RespCommand.INCRBY => NetworkIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.INCRBY, ref garnetApi),
                RespCommand.DECR => NetworkIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.DECR, ref garnetApi),
                RespCommand.DECRBY => NetworkIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand.DECRBY, ref garnetApi),
                RespCommand.SETBIT => NetworkStringSetBit<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.GETBIT => NetworkStringGetBit<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.BITCOUNT => NetworkStringBitCount<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.BITPOS => NetworkStringBitPosition<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.PUBLISH => NetworkPUBLISH(),
                RespCommand.PING => parseState.Count == 0 ? NetworkPING() : NetworkArrayPING(),
                RespCommand.ASKING => NetworkASKING(),
                RespCommand.MULTI => NetworkMULTI(),
                RespCommand.EXEC => NetworkEXEC(),
                RespCommand.UNWATCH => NetworkUNWATCH(),
                RespCommand.DISCARD => NetworkDISCARD(),
                RespCommand.QUIT => NetworkQUIT(),
                RespCommand.RUNTXP => NetworkRUNTXP(),
                RespCommand.READONLY => NetworkREADONLY(),
                RespCommand.READWRITE => NetworkREADWRITE(),
                _ => ProcessArrayCommands<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi)
            };

            return true;
        }

        private bool ProcessArrayCommands<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand cmd, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            /*
             * WARNING: Do not add any command here classified as @slow!
             * Only @fast commands otherwise latency tracking will break for NET_RS (check how containsSlowCommand is used).
             */
            var success = cmd switch
            {
                RespCommand.MGET => NetworkMGET<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.MSET => NetworkMSET<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.MSETNX => NetworkMSETNX<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.UNLINK => NetworkDEL<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SELECT => NetworkSELECT(),
                RespCommand.WATCH => NetworkWATCH<TKeyLocker, TEpochGuard>(),
                RespCommand.WATCH_MS => NetworkWATCH_MS<TKeyLocker, TEpochGuard>(),
                RespCommand.WATCH_OS => NetworkWATCH_OS<TKeyLocker, TEpochGuard>(),
                RespCommand.MEMORY_USAGE => NetworkMemoryUsage<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.TYPE => NetworkTYPE<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                // Pub/sub commands
                RespCommand.SUBSCRIBE => NetworkSUBSCRIBE(),
                RespCommand.PSUBSCRIBE => NetworkPSUBSCRIBE(),
                RespCommand.UNSUBSCRIBE => NetworkUNSUBSCRIBE(),
                RespCommand.PUNSUBSCRIBE => NetworkPUNSUBSCRIBE(),
                // Custom Object Commands
                RespCommand.COSCAN => ObjectScan<TKeyLocker, TEpochGuard, TGarnetApi>(GarnetObjectType.All, ref garnetApi),
                // Sorted Set commands
                RespCommand.ZADD => SortedSetAdd<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZREM => SortedSetRemove<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZCARD => SortedSetLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZPOPMAX => SortedSetPop<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZSCORE => SortedSetScore<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZMSCORE => SortedSetScores<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZCOUNT => SortedSetCount<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZINCRBY => SortedSetIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZRANK => SortedSetRank<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZRANGE => SortedSetRange<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZRANGEBYSCORE => SortedSetRange<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZREVRANK => SortedSetRank<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZREMRANGEBYLEX => SortedSetLengthByValue<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZREMRANGEBYRANK => SortedSetRemoveRange<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZREMRANGEBYSCORE => SortedSetRemoveRange<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZLEXCOUNT => SortedSetLengthByValue<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZPOPMIN => SortedSetPop<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZRANDMEMBER => SortedSetRandomMember<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZDIFF => SortedSetDifference<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.ZREVRANGE => SortedSetRange<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZREVRANGEBYSCORE => SortedSetRange<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.ZSCAN => ObjectScan<TKeyLocker, TEpochGuard, TGarnetApi>(GarnetObjectType.SortedSet, ref garnetApi),
                //SortedSet for Geo Commands
                RespCommand.GEOADD => GeoAdd<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.GEOHASH => GeoCommands<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.GEODIST => GeoCommands<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.GEOPOS => GeoCommands<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.GEOSEARCH => GeoCommands<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                //HLL Commands
                RespCommand.PFADD => HyperLogLogAdd<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.PFMERGE => HyperLogLogMerge<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.PFCOUNT => HyperLogLogLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                //Bitmap Commands
                RespCommand.BITOP_AND => NetworkStringBitOperation<TKeyLocker, TEpochGuard, TGarnetApi>(BitmapOperation.AND, ref garnetApi),
                RespCommand.BITOP_OR => NetworkStringBitOperation<TKeyLocker, TEpochGuard, TGarnetApi>(BitmapOperation.OR, ref garnetApi),
                RespCommand.BITOP_XOR => NetworkStringBitOperation<TKeyLocker, TEpochGuard, TGarnetApi>(BitmapOperation.XOR, ref garnetApi),
                RespCommand.BITOP_NOT => NetworkStringBitOperation<TKeyLocker, TEpochGuard, TGarnetApi>(BitmapOperation.NOT, ref garnetApi),
                RespCommand.BITFIELD => StringBitField<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.BITFIELD_RO => StringBitFieldReadOnly<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                // List Commands
                RespCommand.LPUSH => ListPush<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.LPUSHX => ListPush<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.LPOP => ListPop<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.RPUSH => ListPush<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.RPUSHX => ListPush<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.RPOP => ListPop<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.LLEN => ListLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LTRIM => ListTrim<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LRANGE => ListRange<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LINDEX => ListIndex<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LINSERT => ListInsert<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LREM => ListRemove<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.RPOPLPUSH => ListRightPopLeftPush<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LMOVE => ListMove<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LMPOP => ListPopMultiple<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.LSET => ListSet<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.BLPOP => ListBlockingPop(cmd),
                RespCommand.BRPOP => ListBlockingPop(cmd),
                RespCommand.BLMOVE => ListBlockingMove(cmd),
                // Hash Commands
                RespCommand.HSET => HashSet<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HMSET => HashSet<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HGET => HashGet<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HMGET => HashGetMultiple<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HGETALL => HashGetAll<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HDEL => HashDelete<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.HLEN => HashLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.HSTRLEN => HashStrLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.HEXISTS => HashExists<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.HKEYS => HashKeys<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HVALS => HashKeys<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HINCRBY => HashIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HINCRBYFLOAT => HashIncrement<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HSETNX => HashSet<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HRANDFIELD => HashRandomField<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi),
                RespCommand.HSCAN => ObjectScan<TKeyLocker, TEpochGuard, TGarnetApi>(GarnetObjectType.Hash, ref garnetApi),
                // Set Commands
                RespCommand.SADD => SetAdd<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SMEMBERS => SetMembers<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SISMEMBER => SetIsMember<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SREM => SetRemove<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SCARD => SetLength<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SPOP => SetPop<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SRANDMEMBER => SetRandomMember<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SSCAN => ObjectScan<TKeyLocker, TEpochGuard, TGarnetApi>(GarnetObjectType.Set, ref garnetApi),
                RespCommand.SMOVE => SetMove<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SINTER => SetIntersect<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SINTERSTORE => SetIntersectStore<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SUNION => SetUnion<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SUNIONSTORE => SetUnionStore<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SDIFF => SetDiff<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SDIFFSTORE => SetDiffStore<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                _ => ProcessOtherCommands<TKeyLocker, TEpochGuard, TGarnetApi>(cmd, ref garnetApi)
            };
            return success;
        }

        private bool ProcessOtherCommands<TKeyLocker, TEpochGuard, TGarnetApi>(RespCommand cmd, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            /*
             * WARNING: Here is safe to add @slow commands (check how containsSlowCommand is used).
             */
            containsSlowCommand = true;
            var success = cmd switch
            {
                RespCommand.AUTH => NetworkAUTH(),
                RespCommand.CLIENT_ID => NetworkCLIENTID(),
                RespCommand.CLIENT_INFO => NetworkCLIENTINFO(),
                RespCommand.CLIENT_LIST => NetworkCLIENTLIST(),
                RespCommand.CLIENT_KILL => NetworkCLIENTKILL(),
                RespCommand.COMMAND => NetworkCOMMAND(),
                RespCommand.COMMAND_COUNT => NetworkCOMMAND_COUNT(),
                RespCommand.COMMAND_INFO => NetworkCOMMAND_INFO(),
                RespCommand.ECHO => NetworkECHO(),
                RespCommand.HELLO => NetworkHELLO(),
                RespCommand.TIME => NetworkTIME(),
                RespCommand.FLUSHALL => NetworkFLUSHALL(),
                RespCommand.FLUSHDB => NetworkFLUSHDB(),
                RespCommand.ACL_CAT => NetworkAclCat(),
                RespCommand.ACL_WHOAMI => NetworkAclWhoAmI(),
                RespCommand.ASYNC => NetworkASYNC(),
                RespCommand.RUNTXP => NetworkRUNTXP(),
                RespCommand.INFO => NetworkINFO(),
                RespCommand.CustomTxn => NetworkCustomTxn(),
                RespCommand.CustomRawStringCmd => NetworkCustomRawStringCmd<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.CustomObjCmd => NetworkCustomObjCmd<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.CustomProcedure => NetworkCustomProcedure(),
                //General key commands
                RespCommand.DBSIZE => NetworkDBSIZE<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.KEYS => NetworkKEYS<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                RespCommand.SCAN => NetworkSCAN<TKeyLocker, TEpochGuard, TGarnetApi>(ref garnetApi),
                // Script Commands
                RespCommand.SCRIPT => TrySCRIPT(),
                RespCommand.EVAL => TryEVAL(),
                RespCommand.EVALSHA => TryEVALSHA(),
                _ => Process(cmd)
            };

            bool NetworkCLIENTID()
            {
                if (parseState.Count != 0)
                    return AbortWithWrongNumberOfArguments("client|id");

                while (!RespWriteUtils.WriteInteger(Id, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            bool NetworkCustomTxn()
            {
                if (!IsCommandArityValid(currentCustomTransaction.NameStr, parseState.Count))
                {
                    currentCustomTransaction = null;
                    return true;
                }

                // Perform the operation
                _ = TryTransactionProc(currentCustomTransaction.id, recvBufferPtr + readHead, recvBufferPtr + endReadHead, customCommandManagerSession.GetCustomTransactionProcedure(currentCustomTransaction.id, txnManager, scratchBufferManager).Item1);
                currentCustomTransaction = null;
                return true;
            }

            bool NetworkCustomProcedure()
            {
                if (!IsCommandArityValid(currentCustomProcedure.NameStr, parseState.Count))
                {
                    currentCustomProcedure = null;
                    return true;
                }

                TryCustomProcedure(currentCustomProcedure.Id, recvBufferPtr + readHead, recvBufferPtr + endReadHead,
                    currentCustomProcedure.CustomProcedureImpl);

                currentCustomProcedure = null;
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            bool Process(RespCommand command)
            {
                ProcessAdminCommands(command);
                return true;
            }

            return success;
        }

        private bool NetworkCustomRawStringCmd<TKeyLocker, TEpochGuard, TGarnetApi>(ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (!IsCommandArityValid(currentCustomRawStringCommand.NameStr, parseState.Count))
            {
                currentCustomRawStringCommand = null;
                return true;
            }

            // Perform the operation
            _ = TryCustomRawStringCommand<TKeyLocker, TEpochGuard, TGarnetApi>(recvBufferPtr + readHead, recvBufferPtr + endReadHead, currentCustomRawStringCommand.GetRespCommand(), 
                    currentCustomRawStringCommand.expirationTicks, currentCustomRawStringCommand.type, ref garnetApi);
            currentCustomRawStringCommand = null;
            return true;
        }

        bool NetworkCustomObjCmd<TKeyLocker, TEpochGuard, TGarnetApi>(ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetApi<TKeyLocker, TEpochGuard>
        {
            if (!IsCommandArityValid(currentCustomObjectCommand.NameStr, parseState.Count))
            {
                currentCustomObjectCommand = null;
                return true;
            }

            // Perform the operation
            _ = TryCustomObjectCommand<TKeyLocker, TEpochGuard, TGarnetApi>(recvBufferPtr + readHead, recvBufferPtr + endReadHead, currentCustomObjectCommand.GetRespCommand(),
                    currentCustomObjectCommand.subid, currentCustomObjectCommand.type, ref garnetApi);
            currentCustomObjectCommand = null;
            return true;
        }

        private bool IsCommandArityValid(string cmdName, int count)
        {
            if (storeWrapper.customCommandManager.CustomCommandsInfo.TryGetValue(cmdName, out var cmdInfo))
            {
                Debug.Assert(cmdInfo != null, "Custom command info should not be null");
                if ((cmdInfo.Arity > 0 && count != cmdInfo.Arity - 1) ||
                    (cmdInfo.Arity < 0 && count < -cmdInfo.Arity - 1))
                {
                    while (!RespWriteUtils.WriteError(string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName), ref dcurr, dend))
                        SendAndReset();

                    return false;
                }
            }

            return true;
        }

        Span<byte> GetCommand(out bool success)
        {
            var ptr = recvBufferPtr + readHead;
            var end = recvBufferPtr + bytesRead;

            // Try the command length
            if (!RespReadUtils.ReadUnsignedLengthHeader(out var length, ref ptr, end))
            {
                success = false;
                return default;
            }

            readHead = (int)(ptr - recvBufferPtr);

            // Try to read the command value
            ptr += length;
            if (ptr + 2 > end)
            {
                success = false;
                return default;
            }

            if (*(ushort*)ptr != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*ptr);
            }

            var result = new Span<byte>(recvBufferPtr + readHead, length);
            readHead += length + 2;
            success = true;

            return result;
        }

        public ArgSlice GetCommandAsArgSlice(out bool success)
        {
            if (bytesRead - readHead < 6)
            {
                success = false;
                return default;
            }

            Debug.Assert(*(recvBufferPtr + readHead) == '$');
            var psize = *(recvBufferPtr + readHead + 1) - '0';
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

            var result = new ArgSlice(recvBufferPtr + readHead + 2, psize);
            Debug.Assert(*(recvBufferPtr + readHead + 2 + psize) == '\r');
            Debug.Assert(*(recvBufferPtr + readHead + 2 + psize + 1) == '\n');

            readHead += 2 + psize + 2;
            success = true;
            return result;
        }

        /// <summary>
        /// Attempt to kill this session.
        /// 
        /// Returns true if this call actually kills the underlying network connection.
        /// 
        /// Subsequent calls will return false.
        /// </summary>
        public bool TryKill()
        => networkSender.TryClose();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool Write(ref Status s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = s.Value;
            return true;
        }

        private static unsafe bool Write(ref SpanByteAndMemory k, ref byte* dst, int length)
        {
            if (k.Length > length) return false;

            var dest = new SpanByte(length, (IntPtr)dst);
            if (k.IsSpanByte)
                k.SpanByte.CopyTo(ref dest);
            else
                k.AsMemoryReadOnlySpan().CopyTo(dest.AsSpan());
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool Write(int seqNo, ref byte* dst, int length)
        {
            if (length < sizeof(int)) return false;
            *(int*)dst = seqNo;
            dst += sizeof(int);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendAndReset()
        {
            var d = networkSender.GetResponseObjectHead();
            if ((int)(dcurr - d) > 0)
            {
                Send(d);
                networkSender.GetResponseObject();
                dcurr = networkSender.GetResponseObjectHead();
                dend = networkSender.GetResponseObjectTail();
            }
            else
            {
                // Reaching here means that we retried SendAndReset without the RespWriteUtils.Write*
                // method making any progress. This should only happen when the message being written is
                // too large to fit in the response buffer.
                GarnetException.Throw("Failed to write to response buffer", LogLevel.Critical);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendAndReset(IMemoryOwner<byte> memory, int length)
        {
            // Copy allocated memory to main buffer and send
            fixed (byte* _src = memory.Memory.Span)
            {
                var src = _src;
                var bytesLeft = length;

                // Repeat while we have bytes left to write from input Memory to output buffer
                while (bytesLeft > 0)
                {
                    // Compute space left on output buffer
                    var destSpace = (int)(dend - dcurr);

                    // Adjust number of bytes to copy, to MIN(space left on output buffer, bytes left to copy)
                    var toCopy = bytesLeft;
                    if (toCopy > destSpace)
                        toCopy = destSpace;

                    // Copy bytes to output buffer
                    Buffer.MemoryCopy(src, dcurr, destSpace, toCopy);

                    // Move cursor on output buffer and input memory, update bytes left
                    dcurr += toCopy;
                    src += toCopy;
                    bytesLeft -= toCopy;

                    // If output buffer is full, send and reset output buffer. It is okay to leave the
                    // buffer partially full, as ProcessMessage will do a final Send before returning.
                    if (toCopy == destSpace)
                    {
                        Send(networkSender.GetResponseObjectHead());
                        networkSender.GetResponseObject();
                        dcurr = networkSender.GetResponseObjectHead();
                        dend = networkSender.GetResponseObjectTail();
                    }
                }
            }
            memory.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteDirectLarge(ReadOnlySpan<byte> src)
        {
            // Repeat while we have bytes left to write
            while (src.Length > 0)
            {
                // Compute space left on output buffer
                var destSpace = (int)(dend - dcurr);

                // Fast path if there is enough space 
                if (src.Length <= destSpace)
                {
                    src.CopyTo(new Span<byte>(dcurr, src.Length));
                    dcurr += src.Length;
                    break;
                }

                // Adjust number of bytes to copy, to space left on output buffer, then copy
                src.Slice(0, destSpace).CopyTo(new Span<byte>(dcurr, destSpace));
                src = src.Slice(destSpace);

                // Send and reset output buffer
                Send(networkSender.GetResponseObjectHead());
                networkSender.GetResponseObject();
                dcurr = networkSender.GetResponseObjectHead();
                dend = networkSender.GetResponseObjectTail();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Send(byte* d)
        {
            // Note: This SEND method may be called for responding to multiple commands in a single message (pipelining),
            // or multiple times in a single command for sending data larger than fitting in buffer at once.

            // #if DEBUG
            // logger?.LogTrace("SEND: [{send}]", Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", ""));
            // Debug.WriteLine($"SEND: [{Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "")}]");
            // #endif

            if ((int)(dcurr - d) > 0)
            {
                // Debug.WriteLine("SEND: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "!") + "]");
                if (waitForAofBlocking)
                {
                    var task = storeWrapper.appendOnlyFile.WaitForCommitAsync();
                    if (!task.IsCompleted) task.AsTask().GetAwaiter().GetResult();
                }
                var sendBytes = (int)(dcurr - d);
                _ = networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), sendBytes);
                sessionMetrics?.incr_total_net_output_bytes((ulong)sendBytes);
            }
        }

        /// <summary>
        /// Debug version - send one byte at a time
        /// </summary>
        private void DebugSend(byte* d)
        {
            // Debug.WriteLine("SEND: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr-d))).Replace("\n", "|").Replace("\r", "") + "]");

            if ((int)(dcurr - d) > 0)
            {
                if (storeWrapper.appendOnlyFile != null && storeWrapper.serverOptions.WaitForCommit)
                {
                    var task = storeWrapper.appendOnlyFile.WaitForCommitAsync();
                    if (!task.IsCompleted) task.AsTask().GetAwaiter().GetResult();
                }
                var sendBytes = (int)(dcurr - d);
                var buffer = new byte[sendBytes];
                fixed (byte* dest = buffer)
                    Buffer.MemoryCopy(d, dest, sendBytes, sendBytes);


                for (var i = 0; i < sendBytes; i++)
                {
                    *d = buffer[i];
                    _ = networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), 1);
                    networkSender.GetResponseObject();
                    d = dcurr = networkSender.GetResponseObjectHead();
                    dend = networkSender.GetResponseObjectTail();
                }

                sessionMetrics?.incr_total_net_output_bytes((ulong)sendBytes);
            }
        }

        /// <summary>
        /// Gets the output object from the SpanByteAndMemory object
        /// </summary>
        /// <param name="output"></param>
        /// <returns></returns>
        private unsafe ObjectOutputHeader ProcessOutputWithHeader(SpanByteAndMemory output)
        {
            ReadOnlySpan<byte> outputSpan;
            ObjectOutputHeader header;

            if (output.IsSpanByte)
            {
                header = *(ObjectOutputHeader*)(output.SpanByte.ToPointer() + output.Length - sizeof(ObjectOutputHeader));

                // Only increment dcurr if the operation was completed
                dcurr += output.Length - sizeof(ObjectOutputHeader);
            }
            else
            {
                outputSpan = output.Memory.Memory.Span;
                fixed (byte* p = outputSpan)
                {
                    header = *(ObjectOutputHeader*)(p + output.Length - sizeof(ObjectOutputHeader));
                }
                SendAndReset(output.Memory, output.Length - sizeof(ObjectOutputHeader));
            }

            return header;
        }
    }
}