// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.common.Parsing;
using Garnet.networking;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Garnet.server.Auth.Settings;
using HdrHistogram;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        readonly GarnetSessionMetrics sessionMetrics;

        public GarnetLatencyMetricsSession LatencyMetrics { get; }

        public StoreWrapper StoreWrapper => this.storeWrapper;

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
        public void ResetLatencyMetrics(LatencyMetricsType latencyEvent) => LatencyMetrics?.Reset(latencyEvent);

        /// <summary>
        /// Reset all latencyMetrics
        /// </summary>
        public void ResetAllLatencyMetrics() => LatencyMetrics?.ResetAll();

        readonly StoreWrapper storeWrapper;
        internal readonly ScratchBufferBuilder scratchBufferBuilder;
        internal readonly ScratchBufferAllocator scratchBufferAllocator;

        internal SessionParseState parseState;
        internal SessionParseState customCommandParseState;

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

        internal byte* dcurr, dend;
        bool toDispose;

        int opCount;

        /// <summary>
        /// Current database session items
        /// </summary>
        public StorageSession storageSession;
        internal BasicGarnetApi basicGarnetApi;
        internal TransactionalGarnetApi transactionalGarnetApi;
        internal TransactionManager txnManager;
        internal ConsistentReadGarnetApi consistentReadGarnetApi;
        internal TransactionalConsistentReadGarnetApi txnConsistentReadApi;

        readonly IGarnetAuthenticator _authenticator;

        // Current active database ID
        internal int activeDbId;

        // True if multiple logical databases are enabled on this session
        readonly bool allowMultiDb;

        // Track whether consistent read session is active
        internal bool IsConsistentReadSessionActive = false;

        // Map of all active database sessions (default of size 1, containing DB 0 session)
        private ExpandableMap<GarnetDatabaseSession> databaseSessions;

        // Consistent database read session
        private GarnetDatabaseSession consistentReadDBSession;

        /// <summary>
        /// The user currently authenticated in this session
        /// </summary>
        UserHandle _userHandle = null;

        readonly ILogger logger = null;

        IGarnetServer server;

        /// <summary>
        /// Clients must enable asking to make node respond to requests on slots that are being imported.
        /// </summary>
        public byte SessionAsking { get; set; }

        /// <summary>
        /// If set, commands can use this to enumerate details about the server or other sessions.
        ///
        /// It is not guaranteed to be set.
        /// </summary>
        public IGarnetServer Server
        {
            get => server;
            set
            {
                server = value;
                if (clusterSession is not null)
                {
                    clusterSession.Server = value;
                }
            }
        }

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
        public byte respProtocolVersion { get; private set; } = ServerOptions.DEFAULT_RESP_VERSION;

        /// <summary>
        /// Client name for the session
        /// </summary>
        string clientName = null;

        /// <summary>
        /// Name of the client library.
        /// </summary>
        string clientLibName = null;

        /// <summary>
        /// Version of the client library.
        /// </summary>
        string clientLibVersion = null;

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

        // Track start time (in ticks) of last command for slow log purposes
        long slowLogStartTime;
        // Threshold for slow log in ticks (0 means disabled)
        readonly long slowLogThreshold;

        /// <summary>
        /// Create a new RESP server session
        /// </summary>
        /// <param name="id"></param>
        /// <param name="networkSender"></param>
        /// <param name="storeWrapper"></param>
        /// <param name="subscribeBroker"></param>
        /// <param name="authenticator"></param>
        /// <param name="enableScripts"></param>
        /// <param name="clusterProvider"></param>
        /// <exception cref="GarnetException"></exception>
        public RespServerSession(
            long id,
            INetworkSender networkSender,
            StoreWrapper storeWrapper,
            SubscribeBroker subscribeBroker,
            IGarnetAuthenticator authenticator,
            bool enableScripts,
            IClusterProvider clusterProvider = null)
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
            this.scratchBufferBuilder = new ScratchBufferBuilder();

            // Initialize session-local scratch allocation of size 64 bytes, used for constructing arguments in GarnetApi
            this.scratchBufferAllocator = new ScratchBufferAllocator();

            this.storeWrapper = storeWrapper;
            this.subscribeBroker = subscribeBroker;
            this._authenticator = authenticator ?? storeWrapper.serverOptions.AuthSettings?.CreateAuthenticator(this.storeWrapper) ?? new GarnetNoAuthAuthenticator();

            if (storeWrapper.serverOptions.EnableLua && enableScripts)
                sessionScriptCache = new(storeWrapper, _authenticator, storeWrapper.luaTimeoutManager, logger);

            allowMultiDb = storeWrapper.serverOptions.AllowMultiDb;

            // Create the default DB session (for DB 0) & add it to the session map
            activeDbId = 0;
            var dbSession = CreateDatabaseSession(0);
            var maxDbs = storeWrapper.serverOptions.EnableCluster ? 2 : storeWrapper.serverOptions.MaxDatabases;

            databaseSessions = new ExpandableMap<GarnetDatabaseSession>(1, 0, maxDbs - 1);
            if (!databaseSessions.TrySetValue(0, dbSession))
                throw new GarnetException("Failed to set initialize database session in database sessions map!");

            // Create consistent read APIs and storageSession
            if (storeWrapper.serverOptions.EnableCluster && storeWrapper.serverOptions.EnableAOF && storeWrapper.serverOptions.AofPhysicalSublogCount > 1)
                consistentReadDBSession = CreateConsistentReadApi();

            // Set the current active session to the default session
            SwitchActiveDatabaseSession(dbSession);

            // Associate new session with default user and automatically authenticate, if possible
            this.AuthenticateUser(Encoding.ASCII.GetBytes(this.storeWrapper.accessControlList.GetDefaultUserHandle().User.Name));

            var cp = clusterProvider ?? storeWrapper.clusterProvider;
            clusterSession = cp?.CreateClusterSession(txnManager, this._authenticator, this._userHandle, sessionMetrics, basicGarnetApi, networkSender, logger);
            clusterSession?.SetUserHandle(this._userHandle);
            sessionScriptCache?.SetUserHandle(this._userHandle);

            parseState.Initialize();
            readHead = 0;
            toDispose = false;
            SessionAsking = 0;
            if (storeWrapper.serverOptions.SlowLogThreshold > 0)
                slowLogThreshold = (long)(storeWrapper.serverOptions.SlowLogThreshold * OutputScalingFactor.TimeStampToMicroseconds);

            // Reserve minimum 4 bytes to send pending sequence number as output
            if (this.networkSender != null)
            {
                if (this.networkSender.GetMaxSizeSettings?.MaxOutputSize < sizeof(int))
                    this.networkSender.GetMaxSizeSettings.MaxOutputSize = sizeof(int);
            }
        }

        /// <summary>
        /// Just for fuzzing and testing purposes, do not use otherwise.
        /// </summary>
        internal RespServerSession() : base(null)
        {
            var cmdManager = new CustomCommandManager();
            customCommandManagerSession = new(cmdManager);
            storeWrapper = new(
                "",
                "",
                [],
                cmdManager,
                new(),
                subscribeBroker: null,
                createDatabaseDelegate: delegate { return new(); }
            );
        }

        /// <summary>
        /// Get all active database sessions
        /// </summary>
        /// <returns>Array of active database sessions</returns>
        public GarnetDatabaseSession[] GetDatabaseSessionsSnapshot()
        {
            var databaseSessionsMapSize = databaseSessions.ActualSize;
            var databaseSessionsMapSnapshot = databaseSessions.Map;

            // If there is only 1 active session, it is the default session
            if (databaseSessionsMapSize == 1)
                return [databaseSessionsMapSnapshot[0]];

            var databaseSessionsSnapshot = new List<GarnetDatabaseSession>();

            for (var i = 0; i < databaseSessionsMapSize; i++)
            {
                if (databaseSessionsMapSnapshot[i] != null)
                    databaseSessionsSnapshot.Add(databaseSessionsMapSnapshot[i]);
            }

            return databaseSessionsSnapshot.ToArray();
        }

        internal void SetUserHandle(UserHandle userHandle)
        {
            this._userHandle = userHandle;
            clusterSession?.SetUserHandle(userHandle);
        }

        /// <summary>
        /// Update RESP protocol version used by session
        /// </summary>
        /// <param name="_respProtocolVersion"></param>
        public void UpdateRespProtocolVersion(byte _respProtocolVersion)
        {
            this.respProtocolVersion = _respProtocolVersion;
            this.storageSession.UpdateRespProtocolVersion(respProtocolVersion);
        }

        public override void Dispose()
        {
            logger?.LogDebug("Disposing RespServerSession Id={id}", this.Id);

            readSessionWaiter?.Dispose();

            if (recvBufferPtr != null)
            {
                try { if (recvHandle.IsAllocated) recvHandle.Free(); } catch { }
            }

            // Dispose special consistent read database session
            consistentReadDBSession?.Dispose();

            // Dispose all database sessions
            foreach (var dbSession in databaseSessions.Map)
                dbSession?.Dispose();

            clusterSession?.Dispose();

            if (storeWrapper.serverOptions.MetricsSamplingFrequency > 0 || storeWrapper.serverOptions.LatencyMonitor)
                storeWrapper.monitor.AddMetricsHistorySessionDispose(sessionMetrics, LatencyMetrics);

            subscribeBroker?.RemoveSubscription(this);
            storeWrapper.itemBroker?.HandleSessionDisposed(this);
            sessionScriptCache?.Dispose();

            // Cancel the async processor, if any
            asyncWaiterCancel?.Cancel();
            asyncWaiter?.Signal();
        }

        public int StoreSessionID => storageSession.SessionID;
        public int ObjectStoreSessionID => storageSession.ObjectStoreSessionID;

        /// <summary>
        /// Tries to authenticate the given username/password and updates the user associated with this server session.
        /// </summary>
        /// <param name="username">Name of the user to authenticate.</param>
        /// <param name="password">Password to authenticate with.</param>
        /// <returns>True if the session has been authenticated successfully, false if the user could not be authenticated.</returns>
        bool AuthenticateUser(ReadOnlySpan<byte> username, ReadOnlySpan<byte> password = default(ReadOnlySpan<byte>))
        {
            // Authenticate user or change to default user if no authentication is supported
            bool success = _authenticator.CanAuthenticate ? _authenticator.Authenticate(password, username) : true;

            if (success)
            {
                // Set authenticated user or fall back to default user, if separate users are not supported
                // NOTE: Currently only GarnetACLAuthenticator supports multiple users
                if (_authenticator is GarnetACLAuthenticator aclAuthenticator)
                {
                    this._userHandle = aclAuthenticator.GetUserHandle();
                }
                else
                {
                    this._userHandle = this.storeWrapper.accessControlList.GetDefaultUserHandle();
                }

                // Propagate authentication to cluster session
                clusterSession?.SetUserHandle(this._userHandle);
                sessionScriptCache?.SetUserHandle(this._userHandle);
            }

            return _authenticator.CanAuthenticate ? success : false;
        }

        internal bool CanRunDebug()
        {
            var enableDebugCommand = storeWrapper.serverOptions.EnableDebugCommand;

            return
                (enableDebugCommand == ConnectionProtectionOption.Yes) ||
                ((enableDebugCommand == ConnectionProtectionOption.Local) &&
                    networkSender.IsLocalConnection());
        }

        internal bool CanRunModule()
        {
            var enableModuleCommand = storeWrapper.serverOptions.EnableModuleCommand;

            return
                (enableModuleCommand == ConnectionProtectionOption.Yes) ||
                ((enableModuleCommand == ConnectionProtectionOption.Local) &&
                    networkSender.IsLocalConnection());
        }

        bool txnSkip = false;

        public override int TryConsumeMessages(byte* reqBuffer, int bytesReceived)
        {
            bytesRead = bytesReceived;
            if (!txnSkip)
                readHead = 0;
            try
            {
                LatencyMetrics?.Start(LatencyMetricsType.NET_RS_LAT);
                if (slowLogThreshold > 0)
                {
                    slowLogStartTime = LatencyMetrics != null ? LatencyMetrics.Get(LatencyMetricsType.NET_RS_LAT) : Stopwatch.GetTimestamp();
                }
                clusterSession?.AcquireCurrentEpoch();
                recvBufferPtr = reqBuffer;
                networkSender.EnterAndGetResponseObject(out dcurr, out dend);

                if (storeWrapper.EnforceConsistentRead())
                {
                    try
                    {
                        // We actively switch session because we aim to avoid performing any additional checks or switches on the normal processing path
                        // This requires us to cache txnSkip result since the txnManager instance will change when the following finally executes
                        // Switching is required because we cannot guaranttee the role of the node outside the epoch protection
                        txnSkip = false;
                        Debug.Assert(consistentReadDBSession != null);
                        SwitchActiveDatabaseSession(consistentReadDBSession);
                        ProcessMessages(ref consistentReadGarnetApi, ref txnConsistentReadApi);
                        txnSkip = txnManager.IsSkippingOperations();
                    }
                    finally
                    {
                        // Switch back to normal session in the event a failover results in this node to become a primary
                        SwitchActiveDatabaseSession(databaseSessions.Map[0]);
                    }
                }
                else
                {
                    txnSkip = false;
                    ProcessMessages(ref basicGarnetApi, ref transactionalGarnetApi);
                    txnSkip = txnManager.IsSkippingOperations();
                }
                recvBufferPtr = null;
            }
            catch (RespParsingException ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger?.Log(ex.LogLevel, ex, "Aborting open session due to RESP parsing error");

                // Forward parsing error as RESP error
                while (!RespWriteUtils.TryWriteError($"ERR Protocol Error: {ex.Message}", ref dcurr, dend))
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
                    while (!RespWriteUtils.TryWriteError($"ERR Garnet Exception: {ex.Message}", ref dcurr, dend))
                        SendAndReset();
                }

                // Send message and dispose the network sender to end the session
                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());

                if (ex.Panic)
                {
                    // Does not return, finally block below will NOT be ran.
                    Environment.Exit(-1);
                }

                if (ex.DisposeSession)
                {
                    // The session is no longer usable, dispose it
                    networkSender.DisposeNetworkSender(true);
                }
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
                scratchBufferBuilder.Reset();
                scratchBufferAllocator.Reset();
            }

            if (txnSkip)
                return 0; // so that network does not try to shift the byte array

            // If server processed input data successfully, update tracked metrics
            if (readHead > 0)
            {
                if (LatencyMetrics != null)
                {
                    if (containsSlowCommand)
                    {
                        LatencyMetrics.StopAndSwitch(LatencyMetricsType.NET_RS_LAT, LatencyMetricsType.NET_RS_LAT_ADMIN);
                        containsSlowCommand = false;
                    }
                    else
                        LatencyMetrics.Stop(LatencyMetricsType.NET_RS_LAT);
                    LatencyMetrics.RecordValue(LatencyMetricsType.NET_RS_BYTES, readHead);
                    LatencyMetrics.RecordValue(LatencyMetricsType.NET_RS_OPS, opCount);
                    opCount = 0;
                }
                sessionMetrics?.incr_total_net_input_bytes((ulong)readHead);
            }
            return readHead;
        }

        /// <summary>
        /// For testing purposes, call <see cref="INetworkSender.EnterAndGetResponseObject"/> and update state accordingly.
        /// </summary>
        internal void EnterAndGetResponseObject()
        => networkSender.EnterAndGetResponseObject(out dcurr, out dend);

        /// <summary>
        /// For testing purposes, call <see cref="INetworkSender.ExitAndReturnResponseObject"/> and update state accordingly.
        /// </summary>
        internal void ExitAndReturnResponseObject()
        {
            networkSender.ExitAndReturnResponseObject();
            dcurr = dend = (byte*)0;
        }

        internal void SetTransactionMode(bool enable)
            => txnManager.state = enable ? TxnState.Running : TxnState.None;

        private void ProcessMessages<TBasicApi, TTxnApi>(ref TBasicApi basicApi, ref TTxnApi txnApi)
            where TBasicApi : IGarnetApi
            where TTxnApi : IGarnetApi
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
                var cmd = ParseCommand(writeErrorOnFailure: true, out var commandReceived);

                // If the command was not fully received, reset addresses and break out
                if (!commandReceived)
                {
                    endReadHead = readHead = _origReadHead;
                    break;
                }

                // Check ACL permissions for the command
                if (cmd != RespCommand.INVALID)
                {
                    var noScriptPassed = true;

                    if (CheckACLPermissions(cmd) && (noScriptPassed = CheckScriptPermissions(cmd)))
                    {
                        if (txnManager.state != TxnState.None)
                        {
                            if (txnManager.state == TxnState.Running)
                            {
                                _ = ProcessBasicCommands(cmd, ref txnApi);
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
                                _ = ProcessBasicCommands(cmd, ref basicApi);
                        }
                    }
                    else
                    {
                        if (noScriptPassed)
                        {
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOAUTH, ref dcurr, dend))
                                SendAndReset();
                        }
                        else
                        {
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOSCRIPT, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
                }
                else
                {
                    containsSlowCommand = true;
                }

                // Advance read head variables to process the next command
                _origReadHead = readHead = endReadHead;

                // Handle metrics and special cases
                if (LatencyMetrics != null) opCount++;
                if (slowLogThreshold > 0) HandleSlowLog(cmd);
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
        private bool MakeUpperCase(byte* ptr, int len)
        {
            // Assume most commands are already upper case.
            // Assume most commands are 2-8 bytes long.
            // If that's the case, we would see the following bit patterns:
            //  *.\r\n$2\r\n..\r\n        = 12 bytes
            //  *.\r\n$3\r\n...\r\n       = 13 bytes
            //  ...
            //  *.\r\n$8\r\n........\r\n  = 18 bytes
            //
            // Where . is <= 95
            //
            // Note that _all_ of these bytes are <= 95 in the common case
            // and there's no need to scan the whole string in those cases.

            if (len >= 12)
            {
                var cmdLen = (uint)(*(ptr + 5) - '2');
                if (cmdLen <= 6 && (ptr + 4 + cmdLen + sizeof(ulong)) <= (ptr + len))
                {
                    var firstUlong = *(ulong*)(ptr + 4);
                    var secondUlong = *(ulong*)(ptr + 4 + cmdLen);

                    // Ye olde bit twiddling to check if any sub-byte is > 95
                    // See: https://graphics.stanford.edu/~seander/bithacks.html#HasMoreInWord
                    var firstAllUpper = (((firstUlong + (~0UL / 255 * (127 - 95))) | (firstUlong)) & (~0UL / 255 * 128)) == 0;
                    var secondAllUpper = (((secondUlong + (~0UL / 255 * (127 - 95))) | (secondUlong)) & (~0UL / 255 * 128)) == 0;

                    var allLower = firstAllUpper && secondAllUpper;
                    if (allLower)
                    {
                        // Nothing in the "command" part of the string would be upper cased, so return early
                        return false;
                    }
                }
            }

            // If we're in a weird case, or there are lower case bytes, do the full scan

            var tmp = ptr;

            while (tmp < (ptr + len))
            {
                if (*tmp > 64) // found string
                {
                    var ret = false;
                    while (*tmp > 32 && *tmp < 123 && tmp < (ptr + len))
                    {
                        if (*tmp > 96) { ret = true; *tmp -= 32; }
                        tmp++;
                    }
                    return ret;
                }
                tmp++;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ProcessBasicCommands<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            /*
             * WARNING: Do not add any command here classified as @slow!
             * Only @fast commands otherwise latency tracking will break for NET_RS (check how containsSlowCommand is used).
             */
            _ = cmd switch
            {
                RespCommand.GET => NetworkGET(ref storageApi),
                RespCommand.GETEX => NetworkGETEX(ref storageApi),
                RespCommand.SET => NetworkSET(ref storageApi),
                RespCommand.SETEX => NetworkSETEX(false, ref storageApi),
                RespCommand.SETNX => NetworkSETNX(false, ref storageApi),
                RespCommand.PSETEX => NetworkSETEX(true, ref storageApi),
                RespCommand.SETEXNX => NetworkSETEXNX(ref storageApi),
                RespCommand.DEL => NetworkDEL(ref storageApi),
                RespCommand.RENAME => NetworkRENAME(ref storageApi),
                RespCommand.RENAMENX => NetworkRENAMENX(ref storageApi),
                RespCommand.EXISTS => NetworkEXISTS(ref storageApi),
                RespCommand.EXPIRE => NetworkEXPIRE(RespCommand.EXPIRE, ref storageApi),
                RespCommand.PEXPIRE => NetworkEXPIRE(RespCommand.PEXPIRE, ref storageApi),
                RespCommand.EXPIRETIME => NetworkEXPIRETIME(RespCommand.EXPIRETIME, ref storageApi),
                RespCommand.PEXPIRETIME => NetworkEXPIRETIME(RespCommand.PEXPIRETIME, ref storageApi),
                RespCommand.PERSIST => NetworkPERSIST(ref storageApi),
                RespCommand.GETRANGE => NetworkGetRange(ref storageApi),
                RespCommand.SUBSTR => NetworkGetRange(ref storageApi),
                RespCommand.TTL => NetworkTTL(RespCommand.TTL, ref storageApi),
                RespCommand.PTTL => NetworkTTL(RespCommand.PTTL, ref storageApi),
                RespCommand.SETRANGE => NetworkSetRange(ref storageApi),
                RespCommand.GETDEL => NetworkGETDEL(ref storageApi),
                RespCommand.GETSET => NetworkGETSET(ref storageApi),
                RespCommand.APPEND => NetworkAppend(ref storageApi),
                RespCommand.STRLEN => NetworkSTRLEN(ref storageApi),
                RespCommand.INCR => NetworkIncrement(RespCommand.INCR, ref storageApi),
                RespCommand.INCRBY => NetworkIncrement(RespCommand.INCRBY, ref storageApi),
                RespCommand.INCRBYFLOAT => NetworkIncrementByFloat(ref storageApi),
                RespCommand.DECR => NetworkIncrement(RespCommand.DECR, ref storageApi),
                RespCommand.DECRBY => NetworkIncrement(RespCommand.DECRBY, ref storageApi),
                RespCommand.SETBIT => NetworkStringSetBit(ref storageApi),
                RespCommand.GETBIT => NetworkStringGetBit(ref storageApi),
                RespCommand.BITCOUNT => NetworkStringBitCount(ref storageApi),
                RespCommand.BITPOS => NetworkStringBitPosition(ref storageApi),
                RespCommand.PUBLISH => NetworkPUBLISH(RespCommand.PUBLISH),
                RespCommand.SPUBLISH => NetworkPUBLISH(RespCommand.SPUBLISH),
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
                RespCommand.EXPIREAT => NetworkEXPIRE(RespCommand.EXPIREAT, ref storageApi),
                RespCommand.PEXPIREAT => NetworkEXPIRE(RespCommand.PEXPIREAT, ref storageApi),
                RespCommand.DUMP => NetworkDUMP(ref storageApi),
                RespCommand.RESTORE => NetworkRESTORE(ref storageApi),

                _ => ProcessArrayCommands(cmd, ref storageApi)
            };

            return true;
        }

        private bool ProcessArrayCommands<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
           where TGarnetApi : IGarnetApi
        {
            /*
             * WARNING: Do not add any command here classified as @slow!
             * Only @fast commands otherwise latency tracking will break for NET_RS (check how containsSlowCommand is used).
             */
            var success = cmd switch
            {
                RespCommand.MGET => NetworkMGET(ref storageApi),
                RespCommand.MSET => NetworkMSET(ref storageApi),
                RespCommand.MSETNX => NetworkMSETNX(ref storageApi),
                RespCommand.UNLINK => NetworkDEL(ref storageApi),
                RespCommand.SELECT => NetworkSELECT(),
                RespCommand.SWAPDB => NetworkSWAPDB(),
                RespCommand.WATCH => NetworkWATCH(),
                RespCommand.WATCHMS => NetworkWATCH_MS(),
                RespCommand.WATCHOS => NetworkWATCH_OS(),
                // Pub/sub commands
                RespCommand.SUBSCRIBE => NetworkSUBSCRIBE(cmd),
                RespCommand.SSUBSCRIBE => NetworkSUBSCRIBE(cmd),
                RespCommand.PSUBSCRIBE => NetworkPSUBSCRIBE(),
                RespCommand.UNSUBSCRIBE => NetworkUNSUBSCRIBE(),
                RespCommand.PUNSUBSCRIBE => NetworkPUNSUBSCRIBE(),
                RespCommand.PUBSUB_CHANNELS => NetworkPUBSUB_CHANNELS(),
                RespCommand.PUBSUB_NUMSUB => NetworkPUBSUB_NUMSUB(),
                RespCommand.PUBSUB_NUMPAT => NetworkPUBSUB_NUMPAT(),
                // Custom Object Commands
                RespCommand.COSCAN => ObjectScan(GarnetObjectType.All, ref storageApi),
                // Sorted Set commands
                RespCommand.ZADD => SortedSetAdd(ref storageApi),
                RespCommand.ZREM => SortedSetRemove(ref storageApi),
                RespCommand.ZCARD => SortedSetLength(ref storageApi),
                RespCommand.ZPOPMAX => SortedSetPop(cmd, ref storageApi),
                RespCommand.ZSCORE => SortedSetScore(ref storageApi),
                RespCommand.ZMSCORE => SortedSetScores(ref storageApi),
                RespCommand.ZCOUNT => SortedSetCount(ref storageApi),
                RespCommand.ZINCRBY => SortedSetIncrement(ref storageApi),
                RespCommand.ZRANK => SortedSetRank(cmd, ref storageApi),
                RespCommand.ZRANGE => SortedSetRange(cmd, ref storageApi),
                RespCommand.ZRANGEBYLEX => SortedSetRange(cmd, ref storageApi),
                RespCommand.ZRANGESTORE => SortedSetRangeStore(ref storageApi),
                RespCommand.ZRANGEBYSCORE => SortedSetRange(cmd, ref storageApi),
                RespCommand.ZREVRANK => SortedSetRank(cmd, ref storageApi),
                RespCommand.ZREMRANGEBYLEX => SortedSetLengthByValue(cmd, ref storageApi),
                RespCommand.ZREMRANGEBYRANK => SortedSetRemoveRange(cmd, ref storageApi),
                RespCommand.ZREMRANGEBYSCORE => SortedSetRemoveRange(cmd, ref storageApi),
                RespCommand.ZLEXCOUNT => SortedSetLengthByValue(cmd, ref storageApi),
                RespCommand.ZPOPMIN => SortedSetPop(cmd, ref storageApi),
                RespCommand.ZMPOP => SortedSetMPop(ref storageApi),
                RespCommand.ZRANDMEMBER => SortedSetRandomMember(ref storageApi),
                RespCommand.ZDIFF => SortedSetDifference(ref storageApi),
                RespCommand.ZDIFFSTORE => SortedSetDifferenceStore(ref storageApi),
                RespCommand.BZMPOP => SortedSetBlockingMPop(),
                RespCommand.BZPOPMAX => SortedSetBlockingPop(cmd),
                RespCommand.BZPOPMIN => SortedSetBlockingPop(cmd),
                RespCommand.ZREVRANGE => SortedSetRange(cmd, ref storageApi),
                RespCommand.ZREVRANGEBYLEX => SortedSetRange(cmd, ref storageApi),
                RespCommand.ZREVRANGEBYSCORE => SortedSetRange(cmd, ref storageApi),
                RespCommand.ZSCAN => ObjectScan(GarnetObjectType.SortedSet, ref storageApi),
                RespCommand.ZINTER => SortedSetIntersect(ref storageApi),
                RespCommand.ZINTERCARD => SortedSetIntersectLength(ref storageApi),
                RespCommand.ZINTERSTORE => SortedSetIntersectStore(ref storageApi),
                RespCommand.ZUNION => SortedSetUnion(ref storageApi),
                RespCommand.ZUNIONSTORE => SortedSetUnionStore(ref storageApi),
                RespCommand.ZEXPIRE => SortedSetExpire(cmd, ref storageApi),
                RespCommand.ZPEXPIRE => SortedSetExpire(cmd, ref storageApi),
                RespCommand.ZEXPIREAT => SortedSetExpire(cmd, ref storageApi),
                RespCommand.ZPEXPIREAT => SortedSetExpire(cmd, ref storageApi),
                RespCommand.ZTTL => SortedSetTimeToLive(cmd, ref storageApi),
                RespCommand.ZPTTL => SortedSetTimeToLive(cmd, ref storageApi),
                RespCommand.ZEXPIRETIME => SortedSetTimeToLive(cmd, ref storageApi),
                RespCommand.ZPEXPIRETIME => SortedSetTimeToLive(cmd, ref storageApi),
                RespCommand.ZPERSIST => SortedSetPersist(ref storageApi),
                //SortedSet for Geo Commands
                RespCommand.GEOADD => GeoAdd(ref storageApi),
                RespCommand.GEOHASH => GeoCommands(cmd, ref storageApi),
                RespCommand.GEODIST => GeoCommands(cmd, ref storageApi),
                RespCommand.GEOPOS => GeoCommands(cmd, ref storageApi),
                RespCommand.GEORADIUS => GeoSearchCommands(cmd, ref storageApi),
                RespCommand.GEORADIUS_RO => GeoSearchCommands(cmd, ref storageApi),
                RespCommand.GEORADIUSBYMEMBER => GeoSearchCommands(cmd, ref storageApi),
                RespCommand.GEORADIUSBYMEMBER_RO => GeoSearchCommands(cmd, ref storageApi),
                RespCommand.GEOSEARCH => GeoSearchCommands(cmd, ref storageApi),
                RespCommand.GEOSEARCHSTORE => GeoSearchCommands(cmd, ref storageApi),
                //HLL Commands
                RespCommand.PFADD => HyperLogLogAdd(ref storageApi),
                RespCommand.PFMERGE => HyperLogLogMerge(ref storageApi),
                RespCommand.PFCOUNT => HyperLogLogLength(ref storageApi),
                //Bitmap Commands
                RespCommand.BITOP_AND => NetworkStringBitOperation(BitmapOperation.AND, ref storageApi),
                RespCommand.BITOP_OR => NetworkStringBitOperation(BitmapOperation.OR, ref storageApi),
                RespCommand.BITOP_XOR => NetworkStringBitOperation(BitmapOperation.XOR, ref storageApi),
                RespCommand.BITOP_NOT => NetworkStringBitOperation(BitmapOperation.NOT, ref storageApi),
                RespCommand.BITOP_DIFF => NetworkStringBitOperation(BitmapOperation.DIFF, ref storageApi),
                RespCommand.BITFIELD => StringBitField(ref storageApi),
                RespCommand.BITFIELD_RO => StringBitFieldReadOnly(ref storageApi),
                // List Commands
                RespCommand.LPUSH => ListPush(cmd, ref storageApi),
                RespCommand.LPUSHX => ListPush(cmd, ref storageApi),
                RespCommand.LPOP => ListPop(cmd, ref storageApi),
                RespCommand.LPOS => ListPosition(ref storageApi),
                RespCommand.RPUSH => ListPush(cmd, ref storageApi),
                RespCommand.RPUSHX => ListPush(cmd, ref storageApi),
                RespCommand.RPOP => ListPop(cmd, ref storageApi),
                RespCommand.LLEN => ListLength(ref storageApi),
                RespCommand.LTRIM => ListTrim(ref storageApi),
                RespCommand.LRANGE => ListRange(ref storageApi),
                RespCommand.LINDEX => ListIndex(ref storageApi),
                RespCommand.LINSERT => ListInsert(ref storageApi),
                RespCommand.LREM => ListRemove(ref storageApi),
                RespCommand.RPOPLPUSH => ListRightPopLeftPush(ref storageApi),
                RespCommand.LMOVE => ListMove(ref storageApi),
                RespCommand.LMPOP => ListPopMultiple(ref storageApi),
                RespCommand.LSET => ListSet(ref storageApi),
                RespCommand.BLPOP => ListBlockingPop(cmd),
                RespCommand.BRPOP => ListBlockingPop(cmd),
                RespCommand.BLMOVE => ListBlockingMove(),
                RespCommand.BRPOPLPUSH => ListBlockingPopPush(),
                RespCommand.BLMPOP => ListBlockingPopMultiple(),
                // Hash Commands
                RespCommand.HSET => HashSet(cmd, ref storageApi),
                RespCommand.HMSET => HashSet(cmd, ref storageApi),
                RespCommand.HGET => HashGet(cmd, ref storageApi),
                RespCommand.HMGET => HashGetMultiple(cmd, ref storageApi),
                RespCommand.HGETALL => HashGetAll(cmd, ref storageApi),
                RespCommand.HDEL => HashDelete(ref storageApi),
                RespCommand.HLEN => HashLength(ref storageApi),
                RespCommand.HSTRLEN => HashStrLength(ref storageApi),
                RespCommand.HEXISTS => HashExists(ref storageApi),
                RespCommand.HKEYS => HashKeys(cmd, ref storageApi),
                RespCommand.HVALS => HashKeys(cmd, ref storageApi),
                RespCommand.HINCRBY => HashIncrement(cmd, ref storageApi),
                RespCommand.HINCRBYFLOAT => HashIncrement(cmd, ref storageApi),
                RespCommand.HEXPIRE => HashExpire(cmd, ref storageApi),
                RespCommand.HPEXPIRE => HashExpire(cmd, ref storageApi),
                RespCommand.HEXPIREAT => HashExpire(cmd, ref storageApi),
                RespCommand.HPEXPIREAT => HashExpire(cmd, ref storageApi),
                RespCommand.HTTL => HashTimeToLive(cmd, ref storageApi),
                RespCommand.HPTTL => HashTimeToLive(cmd, ref storageApi),
                RespCommand.HEXPIRETIME => HashTimeToLive(cmd, ref storageApi),
                RespCommand.HPEXPIRETIME => HashTimeToLive(cmd, ref storageApi),
                RespCommand.HPERSIST => HashPersist(ref storageApi),
                RespCommand.HSETNX => HashSet(cmd, ref storageApi),
                RespCommand.HRANDFIELD => HashRandomField(cmd, ref storageApi),
                RespCommand.HSCAN => ObjectScan(GarnetObjectType.Hash, ref storageApi),
                // Set Commands
                RespCommand.SADD => SetAdd(ref storageApi),
                RespCommand.SMEMBERS => SetMembers(ref storageApi),
                RespCommand.SISMEMBER => SetIsMember(cmd, ref storageApi),
                RespCommand.SMISMEMBER => SetIsMember(cmd, ref storageApi),
                RespCommand.SREM => SetRemove(ref storageApi),
                RespCommand.SCARD => SetLength(ref storageApi),
                RespCommand.SPOP => SetPop(ref storageApi),
                RespCommand.SRANDMEMBER => SetRandomMember(ref storageApi),
                RespCommand.SSCAN => ObjectScan(GarnetObjectType.Set, ref storageApi),
                RespCommand.SMOVE => SetMove(ref storageApi),
                RespCommand.SINTER => SetIntersect(ref storageApi),
                RespCommand.SINTERCARD => SetIntersectLength(ref storageApi),
                RespCommand.SINTERSTORE => SetIntersectStore(ref storageApi),
                RespCommand.SUNION => SetUnion(ref storageApi),
                RespCommand.SUNIONSTORE => SetUnionStore(ref storageApi),
                RespCommand.SDIFF => SetDiff(ref storageApi),
                RespCommand.SDIFFSTORE => SetDiffStore(ref storageApi),
                _ => ProcessOtherCommands(cmd, ref storageApi)
            };
            return success;
        }

        private bool ProcessOtherCommands<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            /*
             * WARNING: Here is safe to add @slow commands (check how containsSlowCommand is used).
             */
            containsSlowCommand = true;
            var success = command switch
            {
                RespCommand.AUTH => NetworkAUTH(),
                RespCommand.MEMORY_USAGE => NetworkMemoryUsage(ref storageApi),
                RespCommand.CLIENT_ID => NetworkCLIENTID(),
                RespCommand.CLIENT_INFO => NetworkCLIENTINFO(),
                RespCommand.CLIENT_LIST => NetworkCLIENTLIST(),
                RespCommand.CLIENT_KILL => NetworkCLIENTKILL(),
                RespCommand.CLIENT_GETNAME => NetworkCLIENTGETNAME(),
                RespCommand.CLIENT_SETNAME => NetworkCLIENTSETNAME(),
                RespCommand.CLIENT_SETINFO => NetworkCLIENTSETINFO(),
                RespCommand.CLIENT_UNBLOCK => NetworkCLIENTUNBLOCK(),
                RespCommand.COMMAND => NetworkCOMMAND(),
                RespCommand.COMMAND_COUNT => NetworkCOMMAND_COUNT(),
                RespCommand.COMMAND_DOCS => NetworkCOMMAND_DOCS(),
                RespCommand.COMMAND_INFO => NetworkCOMMAND_INFO(),
                RespCommand.COMMAND_GETKEYS => NetworkCOMMAND_GETKEYS(),
                RespCommand.COMMAND_GETKEYSANDFLAGS => NetworkCOMMAND_GETKEYSANDFLAGS(),
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
                RespCommand.CustomRawStringCmd => NetworkCustomRawStringCmd(ref storageApi),
                RespCommand.CustomObjCmd => NetworkCustomObjCmd(ref storageApi),
                RespCommand.CustomProcedure => NetworkCustomProcedure(),
                //General key commands
                RespCommand.DBSIZE => NetworkDBSIZE(ref storageApi),
                RespCommand.KEYS => NetworkKEYS(ref storageApi),
                RespCommand.SCAN => NetworkSCAN(ref storageApi),
                RespCommand.TYPE => NetworkTYPE(ref storageApi),
                // Script Commands
                RespCommand.SCRIPT_EXISTS => NetworkScriptExists(),
                RespCommand.SCRIPT_FLUSH => NetworkScriptFlush(),
                RespCommand.SCRIPT_LOAD => NetworkScriptLoad(),

                RespCommand.EVAL => TryEVAL(),
                RespCommand.EVALSHA => TryEVALSHA(),
                // Slow commands
                RespCommand.LCS => NetworkLCS(ref storageApi),

                // Etag related commands
                RespCommand.GETWITHETAG => NetworkGETWITHETAG(ref storageApi),
                RespCommand.GETIFNOTMATCH => NetworkGETIFNOTMATCH(ref storageApi),
                RespCommand.SETIFMATCH => NetworkSETIFMATCH(ref storageApi),
                RespCommand.SETIFGREATER => NetworkSETIFGREATER(ref storageApi),
                RespCommand.DELIFGREATER => NetworkDELIFGREATER(ref storageApi),

                _ => Process(command, ref storageApi)
            };

            bool NetworkCLIENTID()
            {
                if (parseState.Count != 0)
                {
                    return AbortWithWrongNumberOfArguments("client|id");
                }

                while (!RespWriteUtils.TryWriteInt64(Id, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            bool NetworkCustomTxn()
            {
                if (!IsCommandArityValid(currentCustomTransaction.NameStr, currentCustomTransaction.arity, parseState.Count))
                {
                    currentCustomTransaction = null;
                    return true;
                }

                // Perform the operation
                TryTransactionProc(currentCustomTransaction.id,
                    customCommandManagerSession
                        .GetCustomTransactionProcedure(currentCustomTransaction.id, this, txnManager,
                            scratchBufferAllocator, out _));
                currentCustomTransaction = null;
                return true;
            }

            bool NetworkCustomProcedure()
            {
                if (!IsCommandArityValid(currentCustomProcedure.NameStr, currentCustomProcedure.Arity, parseState.Count))
                {
                    currentCustomProcedure = null;
                    return true;
                }

                TryCustomProcedure(customCommandManagerSession.GetCustomProcedure(currentCustomProcedure.Id, this));

                currentCustomProcedure = null;
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            bool Process(RespCommand command, ref TGarnetApi storageApi)
            {
                ProcessAdminCommands(command, ref storageApi);
                return true;
            }

            return success;
        }

        private bool NetworkCustomRawStringCmd<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!IsCommandArityValid(currentCustomRawStringCommand.NameStr, currentCustomRawStringCommand.arity, parseState.Count))
            {
                currentCustomRawStringCommand = null;
                return true;
            }

            // Perform the operation
            var cmd = customCommandManagerSession.GetCustomRespCommand(currentCustomRawStringCommand.id);
            TryCustomRawStringCommand(cmd, currentCustomRawStringCommand.expirationTicks, currentCustomRawStringCommand.type, ref storageApi);
            currentCustomRawStringCommand = null;
            return true;
        }

        bool NetworkCustomObjCmd<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!IsCommandArityValid(currentCustomObjectCommand.NameStr, currentCustomObjectCommand.arity, parseState.Count))
            {
                currentCustomObjectCommand = null;
                return true;
            }

            // Perform the operation
            var type = customCommandManagerSession.GetCustomGarnetObjectType(currentCustomObjectCommand.id);
            TryCustomObjectCommand(type, currentCustomObjectCommand.subid,
                currentCustomObjectCommand.type, ref storageApi);
            currentCustomObjectCommand = null;
            return true;
        }

        private bool IsCommandArityValid(string cmdName, int arity, int count)
        {
            // Arity is not set for this command
            if (arity == 0) return true;

            if ((arity > 0 && count != arity - 1) ||
                (arity < 0 && count < -arity - 1))
            {
                while (!RespWriteUtils.TryWriteError(string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName), ref dcurr, dend))
                    SendAndReset();

                return false;
            }

            return true;
        }

        ReadOnlySpan<byte> GetCommand(out bool success)
        {
            var ptr = recvBufferPtr + readHead;
            var end = recvBufferPtr + bytesRead;

            // Try the command length
            if (!RespReadUtils.TryReadUnsignedLengthHeader(out int length, ref ptr, end))
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

            var result = new ReadOnlySpan<byte>(recvBufferPtr + readHead, length);
            readHead += length + 2;
            success = true;

            return result;
        }

        ReadOnlySpan<byte> GetUpperCaseCommand(out bool success)
        {
            var ptr = recvBufferPtr + readHead;
            var end = recvBufferPtr + bytesRead;

            // Try the command length
            if (!RespReadUtils.TryReadUnsignedLengthHeader(out int length, ref ptr, end))
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

            AsciiUtils.ToUpperInPlace(result);
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
            if (k.Length > length)
                return false;
            k.ReadOnlySpan.CopyTo(new Span<byte>(dst, length));
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
        internal void SendAndReset()
        {
            byte* d = networkSender.GetResponseObjectHead();
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
                byte* src = _src;
                int bytesLeft = length;

                // Repeat while we have bytes left to write from input Memory to output buffer
                while (bytesLeft > 0)
                {
                    // Compute space left on output buffer
                    int destSpace = (int)(dend - dcurr);

                    // Adjust number of bytes to copy, to MIN(space left on output buffer, bytes left to copy)
                    int toCopy = bytesLeft;
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
                int destSpace = (int)(dend - dcurr);

                // Fast path if there is enough space
                if (src.Length <= destSpace)
                {
                    src.CopyTo(new Span<byte>(dcurr, src.Length));
                    dcurr += src.Length;
                    break;
                }

                // Adjust number of bytes to copy, to space left on output buffer, then copy
                src.Slice(0, destSpace).CopyTo(new Span<byte>(dcurr, destSpace));
                dcurr += destSpace;
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
                    var task = storeWrapper.WaitForCommitAsync();
                    if (!task.IsCompleted) task.AsTask().GetAwaiter().GetResult();
                }
                int sendBytes = (int)(dcurr - d);
                networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), sendBytes);
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
                if (storeWrapper.serverOptions.EnableAOF && storeWrapper.serverOptions.WaitForCommit)
                {
                    var task = storeWrapper.WaitForCommitAsync();
                    if (!task.IsCompleted) task.AsTask().GetAwaiter().GetResult();
                }
                int sendBytes = (int)(dcurr - d);
                byte[] buffer = new byte[sendBytes];
                fixed (byte* dest = buffer)
                    Buffer.MemoryCopy(d, dest, sendBytes, sendBytes);


                for (int i = 0; i < sendBytes; i++)
                {
                    *d = buffer[i];
                    networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), 1);
                    networkSender.GetResponseObject();
                    d = dcurr = networkSender.GetResponseObjectHead();
                    dend = networkSender.GetResponseObjectTail();
                }

                sessionMetrics?.incr_total_net_output_bytes((ulong)sendBytes);
            }
        }

        /// <summary>
        /// Set the current database session
        /// </summary>
        /// <param name="dbId">Database ID of the current session</param>
        /// <returns>True if successful</returns>
        internal bool TrySwitchActiveDatabaseSession(int dbId)
        {
            if (!allowMultiDb)
                return false;

            // Try to get or set the database session by ID
            var dbSession = TryGetOrSetDatabaseSession(dbId, out var success);
            if (!success)
                return false;

            // Set the active database session
            SwitchActiveDatabaseSession(dbSession);
            return true;
        }

        /// <summary>
        /// Swap between two database sessions
        /// </summary>
        /// <param name="dbId1">Database ID of first session</param>
        /// <param name="dbId2">Database ID of second session</param>
        /// <returns></returns>
        internal bool TrySwapDatabaseSessions(int dbId1, int dbId2)
        {
            if (!allowMultiDb)
                return false;
            if (dbId1 == dbId2)
                return true;

            // Try to get or set the database sessions
            // Note that the dbIdForSessionCreation is set to the other DB ID -
            // That is because the databases have been swapped prior to the session swap
            var dbSession1 = TryGetOrSetDatabaseSession(dbId1, out var success, dbId2);
            if (!success)
                return false;
            var dbSession2 = TryGetOrSetDatabaseSession(dbId2, out success, dbId1);
            if (!success)
                return false;

            // Swap the sessions in the session map
            databaseSessions.Map[dbId1] = new GarnetDatabaseSession(dbId1, dbSession2);
            databaseSessions.Map[dbId2] = new GarnetDatabaseSession(dbId2, dbSession1);

            // If Lua is enabled, switch the database sessions in the script cache
            if (storeWrapper.serverOptions.EnableLua)
                sessionScriptCache?.TrySwapDatabaseSessions(dbId1, dbId2);

            // If any of the IDs are the current active database ID -
            // Set the new active database session to the swapped session
            if (activeDbId == dbId1)
                SwitchActiveDatabaseSession(databaseSessions.Map[dbId1]);
            else if (activeDbId == dbId2)
                SwitchActiveDatabaseSession(databaseSessions.Map[dbId2]);

            return true;
        }

        /// <summary>
        /// Try to retrieve or create a new database session by DB ID
        /// </summary>
        /// <param name="dbId">Database ID of the session</param>
        /// <param name="success">True if successful</param>
        /// <param name="dbIdForSessionCreation">Database ID session creation (defaults to dbId, this option is used for SWAPDB only)</param>
        /// <returns>Reference to the retrieved or created database session</returns>
        private GarnetDatabaseSession TryGetOrSetDatabaseSession(int dbId, out bool success, int dbIdForSessionCreation = -1)
        {
            success = false;
            if (dbIdForSessionCreation == -1)
                dbIdForSessionCreation = dbId;

            var databaseSessionsMapSize = databaseSessions.ActualSize;
            var databaseSessionsMapSnapshot = databaseSessions.Map;

            // If database already exists, return
            if (dbId >= 0 && dbId < databaseSessionsMapSize && databaseSessionsMapSnapshot[dbId] != null)
            {
                success = true;
                return databaseSessionsMapSnapshot[dbId];
            }

            // Take a lock on the database sessions map (this is required since this method can be called from a thread-unsafe context)
            databaseSessions.mapLock.WriteLock();

            try
            {
                // If database already exists, return
                if (dbId >= 0 && dbId < databaseSessionsMapSize && databaseSessionsMapSnapshot[dbId] != null)
                {
                    success = true;
                    return databaseSessionsMapSnapshot[dbId];
                }

                // Create a new database session and set it in the sessions map
                var dbSession = CreateDatabaseSession(dbIdForSessionCreation);
                if (!databaseSessions.TrySetValueUnsafe(dbId, ref dbSession, false))
                    return default;

                // Update the session map snapshot and return a reference to the inserted session
                success = true;
                databaseSessionsMapSnapshot = databaseSessions.Map;
                return databaseSessionsMapSnapshot[dbId];
            }
            finally
            {
                databaseSessions.mapLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Create a new database session
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <returns>New database session</returns>
        private GarnetDatabaseSession CreateDatabaseSession(int dbId)
        {
            var dbStorageSession = new StorageSession(
                storeWrapper,
                scratchBufferBuilder,
                sessionMetrics,
                LatencyMetrics,
                dbId,
                consistentReadContextCallbacks: null,
                logger,
                respProtocolVersion);
            var dbGarnetApi = new BasicGarnetApi(dbStorageSession, dbStorageSession.basicContext,
                dbStorageSession.objectStoreBasicContext, dbStorageSession.unifiedStoreBasicContext);
            var dbLockableGarnetApi = new TransactionalGarnetApi(dbStorageSession,
                dbStorageSession.transactionalContext, dbStorageSession.objectStoreTransactionalContext,
                dbStorageSession.unifiedStoreTransactionalContext);

            var transactionManager = new TransactionManager(storeWrapper, this, dbGarnetApi, dbLockableGarnetApi,
                dbStorageSession, scratchBufferAllocator, storeWrapper.serverOptions.EnableCluster, logger: logger, dbId: dbId);
            dbStorageSession.txnManager = transactionManager;

            return new GarnetDatabaseSession(dbId, dbStorageSession, dbGarnetApi, dbLockableGarnetApi, transactionManager);
        }

        /// <summary>
        /// Create consistent read API
        /// </summary>
        private GarnetDatabaseSession CreateConsistentReadApi()
        {
            // NOTE:
            // Consistent read session should point to dbId = 0 (because dbId is used to identify working database),
            // though its session id = 1 to differentiate between normal session.
            // Session id is set at the caller.
            var dbId = 0;

            // NOTE: We need to create storage session to tie it to the consistent read API
            var dbStorageSession = new StorageSession(
                storeWrapper,
                scratchBufferBuilder,
                sessionMetrics,
                LatencyMetrics,
                dbId: dbId, // NOTE: only for cluster need to retrieve default database
                consistentReadContextCallbacks: new(ValidateKeySequenceNumber, UpdateKeySequenceNumber),
                logger,
                respProtocolVersion);

            var dbGarnetApi = new BasicGarnetApi(dbStorageSession, dbStorageSession.basicContext,
                dbStorageSession.objectStoreBasicContext, dbStorageSession.unifiedStoreBasicContext);
            var dbLockableGarnetApi = new TransactionalGarnetApi(dbStorageSession,
                dbStorageSession.transactionalContext, dbStorageSession.objectStoreTransactionalContext,
                dbStorageSession.unifiedStoreTransactionalContext);

            var consistentReadGarnetApi = new ConsistentReadGarnetApi(dbStorageSession, dbStorageSession.consistentReadContext,
                dbStorageSession.objectStoreConsistentReadContext, dbStorageSession.unifiedStoreConsistentReadContext);
            var txnConsistentReadApi = new TransactionalConsistentReadGarnetApi(dbStorageSession,
                dbStorageSession.transactionalConsistentReadContext, dbStorageSession.objectStoreTransactionalConsistentReadContext,
                dbStorageSession.unifiedStoreTransactionalConsistentReadContext);

            var consistentReadTransactionManager = new TransactionManager(
                storeWrapper,
                this,
                dbGarnetApi,
                dbLockableGarnetApi,
                dbStorageSession,
                scratchBufferAllocator,
                storeWrapper.serverOptions.EnableCluster,
                enableConsistentRead: true,
                garnetConsistentApi: consistentReadGarnetApi,
                transactionalConsistentGarnetApi: txnConsistentReadApi,
                logger: logger,
                dbId: dbId);

            return new GarnetDatabaseSession(id: dbId, // NOTE: sessionID 1 to differentiate from default session
                dbStorageSession,
                dbGarnetApi,
                dbLockableGarnetApi,
                consistentReadTransactionManager,
                consistentReadGarnetApi,
                txnConsistentReadApi);
        }

        /// <summary>
        /// Switch current active database session
        /// </summary>
        /// <param name="dbSession">Database Session</param>
        private void SwitchActiveDatabaseSession(GarnetDatabaseSession dbSession)
        {
            this.activeDbId = dbSession.Id;
            this.txnManager = dbSession.TransactionManager;
            this.storageSession = dbSession.StorageSession;
            this.basicGarnetApi = dbSession.GarnetApi;
            this.transactionalGarnetApi = dbSession.TransactionalGarnetApi;
            this.consistentReadGarnetApi = dbSession.ConsistentGarnetApi;
            this.txnConsistentReadApi = dbSession.TransactionalConsistentGarnetApi;
            this.storageSession.UpdateRespProtocolVersion(this.respProtocolVersion);
        }
    }
}