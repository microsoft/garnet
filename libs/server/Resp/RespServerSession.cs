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
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;
    using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

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

        readonly StoreWrapper storeWrapper;
        internal readonly TransactionManager txnManager;
        readonly ScratchBufferManager scratchBufferManager;

        SessionParseState parseState;
        GCHandle recvHandle;
        byte* recvBufferPtr;
        int readHead, endReadHead;
        byte* dcurr, dend;
        bool toDispose;

        int opCount;
        public readonly StorageSession storageSession;
        internal BasicGarnetApi basicGarnetApi;
        internal LockableGarnetApi lockableGarnetApi;

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

        // Track whether the incoming network batch had some admin command
        bool hasAdminCommand;

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
        CustomCommand currentCustomCommand = null;

        /// <summary>
        /// Current custom object command to be executed in the session.
        /// </summary>
        CustomObjectCommand currentCustomObjectCommand = null;

        /// <summary>
        /// RESP protocol version (RESP2 is the default)
        /// </summary>
        byte respProtocolVersion = 2;

        /// <summary>
        /// Client name for the session
        /// </summary>
        string clientName = null;

        public RespServerSession(
            INetworkSender networkSender,
            StoreWrapper storeWrapper,
            SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscribeBroker)
            : base(networkSender)
        {
            this.customCommandManagerSession = new CustomCommandManagerSession(storeWrapper.customCommandManager);
            this.sessionMetrics = storeWrapper.serverOptions.MetricsSamplingFrequency > 0 ? new GarnetSessionMetrics() : null;
            this.LatencyMetrics = storeWrapper.serverOptions.LatencyMonitor ? new GarnetLatencyMetricsSession(storeWrapper.monitor) : null;
            logger = storeWrapper.sessionLogger != null ? new SessionLogger(storeWrapper.sessionLogger, $"[{storeWrapper.localEndpoint}] [{networkSender?.RemoteEndpointName}] [{GetHashCode():X8}] ") : null;

            logger?.LogDebug("Starting RespServerSession");

            // Initialize session-local scratch buffer of size 64 bytes, used for constructing arguments in GarnetApi
            this.scratchBufferManager = new ScratchBufferManager();

            // Create storage session and API
            this.storageSession = new StorageSession(storeWrapper, scratchBufferManager, sessionMetrics, LatencyMetrics, logger);

            this.basicGarnetApi = new BasicGarnetApi(storageSession, storageSession.basicContext, storageSession.objectStoreBasicContext);
            this.lockableGarnetApi = new LockableGarnetApi(storageSession, storageSession.lockableContext, storageSession.objectStoreLockableContext);

            this.storeWrapper = storeWrapper;
            this.subscribeBroker = subscribeBroker;
            this._authenticator = storeWrapper.serverOptions.AuthSettings?.CreateAuthenticator(this.storeWrapper) ?? new GarnetNoAuthAuthenticator();

            // Associate new session with default user and automatically authenticate, if possible
            this.AuthenticateUser(Encoding.ASCII.GetBytes(this.storeWrapper.accessControlList.GetDefaultUser().Name));

            txnManager = new TransactionManager(this, storageSession, scratchBufferManager, storeWrapper.serverOptions.EnableCluster, logger);
            storageSession.txnManager = txnManager;

            clusterSession = storeWrapper.clusterProvider?.CreateClusterSession(txnManager, this._authenticator, this._user, sessionMetrics, basicGarnetApi, networkSender, logger);
            clusterSession?.SetUser(this._user);

            parseState.Initialize();
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

        public override void Dispose()
        {
            logger?.LogDebug("Disposing RespServerSession");

            if (recvBufferPtr != null)
            {
                try { if (recvHandle.IsAllocated) recvHandle.Free(); } catch { }
            }

            if (storeWrapper.serverOptions.MetricsSamplingFrequency > 0 || storeWrapper.serverOptions.LatencyMonitor)
                storeWrapper.monitor.AddMetricsHistory(sessionMetrics, latencyMetrics);

            subscribeBroker?.RemoveSubscription(this);

            // Cancel the async processor, if any
            asyncWaiterCancel?.Cancel();
            asyncWaiter?.Signal();

            storageSession.Dispose();
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
                    this._user = aclAuthenticator.GetUser();
                }
                else
                {
                    this._user = this.storeWrapper.accessControlList.GetDefaultUser();
                }

                // Propagate authentication to cluster session
                clusterSession?.SetUser(this._user);
            }

            return _authenticator.CanAuthenticate ? success : false;
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
                logger.Log(ex.LogLevel, ex, "Aborting open session due to RESP parsing error");

                // Forward parsing error as RESP error
                while (!RespWriteUtils.WriteError($"ERR Protocol Error: {ex.Message}", ref dcurr, dend))
                    SendAndReset();

                // Send message and dispose the network sender to end the session
                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());
                networkSender.Dispose();
            }
            catch (GarnetException ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger.Log(ex.LogLevel, ex, "ProcessMessages threw a GarnetException:");
                // The session is no longer usable, dispose it
                networkSender.Dispose();
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
            }

            if (txnManager.IsSkippingOperations())
                return 0; // so that network does not try to shift the byte array

            // If server processed input data successfully, update tracked metrics
            if (readHead > 0)
            {
                if (latencyMetrics != null)
                {
                    if (hasAdminCommand)
                    {
                        latencyMetrics.StopAndSwitch(LatencyMetricsType.NET_RS_LAT, LatencyMetricsType.NET_RS_LAT_ADMIN);
                        hasAdminCommand = false;
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
                var cmd = ParseCommand(out bool commandReceived);

                // If the command was not fully received, reset addresses and break out
                if (!commandReceived)
                {
                    endReadHead = readHead = _origReadHead;
                    break;
                }

                // Check ACL permissions for the command
                if (cmd != RespCommand.INVALID && CheckACLPermissions(cmd))
                {
                    if (txnManager.state != TxnState.None)
                    {
                        if (txnManager.state == TxnState.Running)
                        {
                            _ = ProcessBasicCommands(cmd, ref lockableGarnetApi);
                        }
                        else _ = cmd switch
                        {
                            RespCommand.EXEC => NetworkEXEC(),
                            RespCommand.MULTI => NetworkMULTI(),
                            RespCommand.DISCARD => NetworkDISCARD(),
                            _ => NetworkSKIP(cmd),
                        };
                    }
                    else
                    {
                        _ = ProcessBasicCommands(cmd, ref basicGarnetApi);
                    }
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
                if (SessionAsking != 0) SessionAsking = (byte)(SessionAsking - 1);
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
            byte* tmp = ptr;

            while (tmp < ptr + bytesRead - readHead)
            {
                if (*tmp > 64) // found string
                {
                    bool ret = false;
                    while (*tmp > 64 && *tmp < 123 && tmp < ptr + bytesRead - readHead)
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
            var ptr = recvBufferPtr + readHead;
            _ = cmd switch
            {
                RespCommand.GET => NetworkGET(ref storageApi),
                RespCommand.SET => NetworkSET(ref storageApi),
                RespCommand.SETEX => NetworkSETEX(ptr, false, ref storageApi),
                RespCommand.PSETEX => NetworkSETEX(ptr, true, ref storageApi),
                RespCommand.SETEXNX => NetworkSETEXNX(parseState.count, ptr, ref storageApi),
                RespCommand.DEL => NetworkDEL(parseState.count, ptr, ref storageApi),
                RespCommand.RENAME => NetworkRENAME(ptr, ref storageApi),
                RespCommand.EXISTS => NetworkEXISTS(parseState.count, ptr, ref storageApi),
                RespCommand.EXPIRE => NetworkEXPIRE(parseState.count, ptr, RespCommand.EXPIRE, ref storageApi),
                RespCommand.PEXPIRE => NetworkEXPIRE(parseState.count, ptr, RespCommand.PEXPIRE, ref storageApi),
                RespCommand.PERSIST => NetworkPERSIST(ptr, ref storageApi),
                RespCommand.GETRANGE => NetworkGetRange(ptr, ref storageApi),
                RespCommand.TTL => NetworkTTL(ptr, RespCommand.TTL, ref storageApi),
                RespCommand.PTTL => NetworkTTL(ptr, RespCommand.PTTL, ref storageApi),
                RespCommand.SETRANGE => NetworkSetRange(ptr, ref storageApi),
                RespCommand.GETDEL => NetworkGETDEL(ptr, ref storageApi),
                RespCommand.APPEND => NetworkAppend(ptr, ref storageApi),
                RespCommand.INCR => NetworkIncrement(ptr, RespCommand.INCR, ref storageApi),
                RespCommand.INCRBY => NetworkIncrement(ptr, RespCommand.INCRBY, ref storageApi),
                RespCommand.DECR => NetworkIncrement(ptr, RespCommand.DECR, ref storageApi),
                RespCommand.DECRBY => NetworkIncrement(ptr, RespCommand.DECRBY, ref storageApi),
                RespCommand.SETBIT => NetworkStringSetBit(ptr, ref storageApi),
                RespCommand.GETBIT => NetworkStringGetBit(ptr, ref storageApi),
                RespCommand.BITCOUNT => NetworkStringBitCount(ptr, parseState.count, ref storageApi),
                RespCommand.BITPOS => NetworkStringBitPosition(ptr, parseState.count, ref storageApi),
                RespCommand.PUBLISH => NetworkPUBLISH(ptr),
                RespCommand.PING => parseState.count == 0 ? NetworkPING() : ProcessArrayCommands(cmd, ref storageApi),
                RespCommand.ASKING => NetworkASKING(),
                RespCommand.MULTI => NetworkMULTI(),
                RespCommand.EXEC => NetworkEXEC(),
                RespCommand.UNWATCH => NetworkUNWATCH(),
                RespCommand.DISCARD => NetworkDISCARD(),
                RespCommand.QUIT => NetworkQUIT(),
                RespCommand.RUNTXP => NetworkRUNTXP(parseState.count, ptr),
                RespCommand.READONLY => NetworkREADONLY(),
                RespCommand.READWRITE => NetworkREADWRITE(),
                RespCommand.COMMAND => NetworkCOMMAND(ptr, parseState.count),
                RespCommand.COMMAND_COUNT => NetworkCOMMAND_COUNT(ptr, parseState.count),
                RespCommand.COMMAND_DOCS => NetworkCOMMAND_DOCS(ptr, parseState.count),
                RespCommand.COMMAND_INFO => NetworkCOMMAND_INFO(ptr, parseState.count),

                _ => ProcessArrayCommands(cmd, ref storageApi)
            };

            return true;
        }

        private bool ProcessArrayCommands<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
           where TGarnetApi : IGarnetApi
        {
            int count = parseState.count;

            // Continue reading from the current read head.
            byte* ptr = recvBufferPtr + readHead;

            var success = cmd switch
            {
                RespCommand.MGET => NetworkMGET(count, ptr, ref storageApi),
                RespCommand.MSET => NetworkMSET(count, ptr, ref storageApi),
                RespCommand.MSETNX => NetworkMSETNX(count, ptr, ref storageApi),
                RespCommand.UNLINK => NetworkDEL(count, ptr, ref storageApi),
                RespCommand.SELECT => NetworkSELECT(ptr),
                RespCommand.WATCH => NetworkWATCH(count),
                RespCommand.WATCH_MS => NetworkWATCH_MS(count),
                RespCommand.WATCH_OS => NetworkWATCH_OS(count),
                RespCommand.STRLEN => NetworkSTRLEN(ptr, ref storageApi),
                RespCommand.MODULE => NetworkMODULE(count, ptr, ref storageApi),
                //General key commands
                RespCommand.DBSIZE => NetworkDBSIZE(ptr, ref storageApi),
                RespCommand.KEYS => NetworkKEYS(ptr, ref storageApi),
                RespCommand.SCAN => NetworkSCAN(count, ptr, ref storageApi),
                RespCommand.TYPE => NetworkTYPE(count, ptr, ref storageApi),
                // Pub/sub commands
                RespCommand.SUBSCRIBE => NetworkSUBSCRIBE(count, ptr, dend),
                RespCommand.PSUBSCRIBE => NetworkPSUBSCRIBE(count, ptr, dend),
                RespCommand.UNSUBSCRIBE => NetworkUNSUBSCRIBE(count, ptr, dend),
                RespCommand.PUNSUBSCRIBE => NetworkPUNSUBSCRIBE(count, ptr, dend),
                // Custom Object Commands
                RespCommand.COSCAN => ObjectScan(count, ptr, GarnetObjectType.All, ref storageApi),
                // Sorted Set commands
                RespCommand.ZADD => SortedSetAdd(count, ptr, ref storageApi),
                RespCommand.ZREM => SortedSetRemove(count, ptr, ref storageApi),
                RespCommand.ZCARD => SortedSetLength(count, ptr, ref storageApi),
                RespCommand.ZPOPMAX => SortedSetPop(cmd, count, ptr, ref storageApi),
                RespCommand.ZSCORE => SortedSetScore(count, ptr, ref storageApi),
                RespCommand.ZMSCORE => SortedSetScores(count, ptr, ref storageApi),
                RespCommand.ZCOUNT => SortedSetCount(count, ptr, ref storageApi),
                RespCommand.ZINCRBY => SortedSetIncrement(count, ptr, ref storageApi),
                RespCommand.ZRANK => SortedSetRank(cmd, count, ptr, ref storageApi),
                RespCommand.ZRANGE => SortedSetRange(cmd, count, ptr, ref storageApi),
                RespCommand.ZRANGEBYSCORE => SortedSetRange(cmd, count, ptr, ref storageApi),
                RespCommand.ZREVRANK => SortedSetRank(cmd, count, ptr, ref storageApi),
                RespCommand.ZREMRANGEBYLEX => SortedSetLengthByValue(cmd, count, ptr, ref storageApi),
                RespCommand.ZREMRANGEBYRANK => SortedSetRemoveRange(cmd, count, ptr, ref storageApi),
                RespCommand.ZREMRANGEBYSCORE => SortedSetRemoveRange(cmd, count, ptr, ref storageApi),
                RespCommand.ZLEXCOUNT => SortedSetLengthByValue(cmd, count, ptr, ref storageApi),
                RespCommand.ZPOPMIN => SortedSetPop(cmd, count, ptr, ref storageApi),
                RespCommand.ZRANDMEMBER => SortedSetRandomMember(count, ptr, ref storageApi),
                RespCommand.ZDIFF => SortedSetDifference(count, ptr, ref storageApi),
                RespCommand.ZREVRANGE => SortedSetRange(cmd, count, ptr, ref storageApi),
                RespCommand.ZSCAN => ObjectScan(count, ptr, GarnetObjectType.SortedSet, ref storageApi),
                //SortedSet for Geo Commands
                RespCommand.GEOADD => GeoAdd(count, ptr, ref storageApi),
                RespCommand.GEOHASH => GeoCommands(cmd, count, ptr, ref storageApi),
                RespCommand.GEODIST => GeoCommands(cmd, count, ptr, ref storageApi),
                RespCommand.GEOPOS => GeoCommands(cmd, count, ptr, ref storageApi),
                RespCommand.GEOSEARCH => GeoCommands(cmd, count, ptr, ref storageApi),
                //HLL Commands
                RespCommand.PFADD => HyperLogLogAdd(count, ptr, ref storageApi),
                RespCommand.PFMERGE => HyperLogLogMerge(count, ptr, ref storageApi),
                RespCommand.PFCOUNT => HyperLogLogLength(count, ptr, ref storageApi),
                //Bitmap Commands
                RespCommand.BITOP_AND => NetworkStringBitOperation(count, ptr, BitmapOperation.AND, ref storageApi),
                RespCommand.BITOP_OR => NetworkStringBitOperation(count, ptr, BitmapOperation.OR, ref storageApi),
                RespCommand.BITOP_XOR => NetworkStringBitOperation(count, ptr, BitmapOperation.XOR, ref storageApi),
                RespCommand.BITOP_NOT => NetworkStringBitOperation(count, ptr, BitmapOperation.NOT, ref storageApi),
                RespCommand.BITFIELD => StringBitField(count, ptr, ref storageApi),
                RespCommand.BITFIELD_RO => StringBitFieldReadOnly(count, ptr, ref storageApi),
                // List Commands
                RespCommand.LPUSH => ListPush(cmd, count, ptr, ref storageApi),
                RespCommand.LPUSHX => ListPush(cmd, count, ptr, ref storageApi),
                RespCommand.LPOP => ListPop(cmd, count, ptr, ref storageApi),
                RespCommand.RPUSH => ListPush(cmd, count, ptr, ref storageApi),
                RespCommand.RPUSHX => ListPush(cmd, count, ptr, ref storageApi),
                RespCommand.RPOP => ListPop(cmd, count, ptr, ref storageApi),
                RespCommand.LLEN => ListLength(count, ptr, ref storageApi),
                RespCommand.LTRIM => ListTrim(count, ptr, ref storageApi),
                RespCommand.LRANGE => ListRange(count, ptr, ref storageApi),
                RespCommand.LINDEX => ListIndex(count, ptr, ref storageApi),
                RespCommand.LINSERT => ListInsert(count, ptr, ref storageApi),
                RespCommand.LREM => ListRemove(count, ptr, ref storageApi),
                RespCommand.RPOPLPUSH => ListRightPopLeftPush(count, ptr, ref storageApi),
                RespCommand.LMOVE => ListMove(count, ptr, ref storageApi),
                RespCommand.LSET => ListSet(count, ptr, ref storageApi),
                // Hash Commands
                RespCommand.HSET => HashSet(cmd, count, ptr, ref storageApi),
                RespCommand.HMSET => HashSet(cmd, count, ptr, ref storageApi),
                RespCommand.HGET => HashGet(cmd, count, ptr, ref storageApi),
                RespCommand.HMGET => HashGet(cmd, count, ptr, ref storageApi),
                RespCommand.HGETALL => HashGet(cmd, count, ptr, ref storageApi),
                RespCommand.HDEL => HashDelete(count, ptr, ref storageApi),
                RespCommand.HLEN => HashLength(count, ptr, ref storageApi),
                RespCommand.HSTRLEN => HashStrLength(count, ptr, ref storageApi),
                RespCommand.HEXISTS => HashExists(count, ptr, ref storageApi),
                RespCommand.HKEYS => HashKeys(cmd, count, ptr, ref storageApi),
                RespCommand.HVALS => HashKeys(cmd, count, ptr, ref storageApi),
                RespCommand.HINCRBY => HashIncrement(cmd, count, ptr, ref storageApi),
                RespCommand.HINCRBYFLOAT => HashIncrement(cmd, count, ptr, ref storageApi),
                RespCommand.HSETNX => HashSet(cmd, count, ptr, ref storageApi),
                RespCommand.HRANDFIELD => HashGet(cmd, count, ptr, ref storageApi),
                RespCommand.HSCAN => ObjectScan(count, ptr, GarnetObjectType.Hash, ref storageApi),
                // Set Commands
                RespCommand.SADD => SetAdd(count, ptr, ref storageApi),
                RespCommand.SMEMBERS => SetMembers(count, ptr, ref storageApi),
                RespCommand.SISMEMBER => SetIsMember(count, ptr, ref storageApi),
                RespCommand.SREM => SetRemove(count, ptr, ref storageApi),
                RespCommand.SCARD => SetLength(count, ptr, ref storageApi),
                RespCommand.SPOP => SetPop(count, ptr, ref storageApi),
                RespCommand.SRANDMEMBER => SetRandomMember(count, ptr, ref storageApi),
                RespCommand.SSCAN => ObjectScan(count, ptr, GarnetObjectType.Set, ref storageApi),
                RespCommand.SMOVE => SetMove(count, ptr, ref storageApi),
                RespCommand.SINTER => SetIntersect(count, ptr, ref storageApi),
                RespCommand.SINTERSTORE => SetIntersectStore(count, ptr, ref storageApi),
                RespCommand.SUNION => SetUnion(count, ptr, ref storageApi),
                RespCommand.SUNIONSTORE => SetUnionStore(count, ptr, ref storageApi),
                RespCommand.SDIFF => SetDiff(count, ptr, ref storageApi),
                RespCommand.SDIFFSTORE => SetDiffStore(count, ptr, ref storageApi),
                _ => ProcessOtherCommands(cmd, count, ref storageApi)
            };
            return success;
        }

        private bool ProcessOtherCommands<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (command == RespCommand.CLIENT)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.SUBSCRIBE)
            {
                while (!RespWriteUtils.WriteInteger(1, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.RUNTXP)
            {
                byte* ptr = recvBufferPtr + readHead;
                return NetworkRUNTXP(count, ptr);
            }
            else if (command == RespCommand.CustomTxn)
            {
                if (currentCustomTransaction.NumParams < int.MaxValue && count != currentCustomTransaction.NumParams)
                {
                    while (!RespWriteUtils.WriteError($"ERR Invalid number of parameters to stored proc {currentCustomTransaction.nameStr}, expected {currentCustomTransaction.NumParams}, actual {count}", ref dcurr, dend))
                        SendAndReset();

                    currentCustomTransaction = null;

                    return true;
                }
                else
                {
                    // Perform the operation
                    TryTransactionProc(currentCustomTransaction.id, recvBufferPtr + readHead, recvBufferPtr + endReadHead, customCommandManagerSession.GetCustomTransactionProcedure(currentCustomTransaction.id, txnManager, scratchBufferManager).Item1);
                }

                currentCustomTransaction = null;
            }
            else if (command == RespCommand.CustomCmd)
            {
                if (count != currentCustomCommand.NumKeys + currentCustomCommand.NumParams)
                {
                    while (!RespWriteUtils.WriteError($"ERR Invalid number of parameters, expected {currentCustomCommand.NumKeys + currentCustomCommand.NumParams}, actual {count}", ref dcurr, dend))
                        SendAndReset();

                    currentCustomCommand = null;

                    return true;
                }
                else
                {
                    // Perform the operation
                    TryCustomCommand(recvBufferPtr + readHead, recvBufferPtr + endReadHead, currentCustomCommand.GetRespCommand(), currentCustomCommand.expirationTicks, currentCustomCommand.type, ref storageApi);
                }

                currentCustomCommand = null;
            }
            else if (command == RespCommand.CustomObjCmd)
            {
                if (count != currentCustomObjectCommand.NumKeys + currentCustomObjectCommand.NumParams)
                {
                    while (!RespWriteUtils.WriteError($"ERR Invalid number of parameters, expected {currentCustomObjectCommand.NumKeys + currentCustomObjectCommand.NumParams}, actual {count}", ref dcurr, dend))
                        SendAndReset();

                    currentCustomObjectCommand = null;

                    return true;
                }
                else
                {
                    // Perform the operation
                    TryCustomObjectCommand(recvBufferPtr + readHead, recvBufferPtr + endReadHead, currentCustomObjectCommand.GetRespCommand(), currentCustomObjectCommand.subid, currentCustomObjectCommand.type, ref storageApi);
                }

                currentCustomObjectCommand = null;
            }

            else
            {
                return ProcessAdminCommands(command, count, ref storageApi);
            }
            return true;
        }

        Span<byte> GetCommand(out bool success)
        {
            var ptr = recvBufferPtr + readHead;
            var end = recvBufferPtr + bytesRead;

            // Try the command length
            if (!RespReadUtils.ReadUnsignedLengthHeader(out int length, ref ptr, end))
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

            var result = new ArgSlice(recvBufferPtr + readHead + 2, psize);
            Debug.Assert(*(recvBufferPtr + readHead + 2 + psize) == '\r');
            Debug.Assert(*(recvBufferPtr + readHead + 2 + psize + 1) == '\n');

            readHead += 2 + psize + 2;
            success = true;
            return result;
        }

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
            // #if DEBUG
            // logger?.LogTrace("SEND: [{send}]", Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", ""));
            // Debug.WriteLine($"SEND: [{Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "")}]");
            // #endif

            if ((int)(dcurr - d) > 0)
            {
                // Debug.WriteLine("SEND: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "!") + "]");
                if (storeWrapper.appendOnlyFile != null && storeWrapper.serverOptions.WaitForCommit)
                {
                    var task = storeWrapper.appendOnlyFile.WaitForCommitAsync();
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
                if (storeWrapper.appendOnlyFile != null && storeWrapper.serverOptions.WaitForCommit)
                {
                    var task = storeWrapper.appendOnlyFile.WaitForCommitAsync();
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

        /// <summary>
        /// This method is used to verify slot ownership for provided key.
        /// On error this method writes to response buffer but does not drain recv buffer (caller is responsible for draining).
        /// </summary>
        /// <param name="key">Key bytes</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation.</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkSingleKeySlotVerify(ReadOnlySpan<byte> key, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(key, readOnly, SessionAsking, ref dcurr, ref dend);

        /// <summary>
        /// This method is used to verify slot ownership for provided key.
        /// On error this method writes to response buffer but does not drain recv buffer (caller is responsible for draining).
        /// </summary>
        /// <param name="key">Key bytes</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation.</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkSingleKeySlotVerify(ref SpanByte key, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(new ArgSlice(ref key), readOnly, SessionAsking, ref dcurr, ref dend);

        /// <summary>
        /// This method is used to verify slot ownership for provided key sequence.
        /// On error this method writes to response buffer but does not drain recv buffer (caller is responsible for draining).
        /// </summary>
        /// <param name="keyPtr">Pointer to key bytes</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkSingleKeySlotVerify(byte* keyPtr, int ksize, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(new ArgSlice(keyPtr, ksize), readOnly, SessionAsking, ref dcurr, ref dend);

        /// <summary>
        /// This method is used to verify slot ownership for provided sequence of keys.
        /// On error this method writes to response buffer and drains recv buffer.
        /// </summary>
        /// <param name="keyCount">Number of keys</param>
        /// <param name="ptr">Starting position of RESP formatted key sequence</param>
        /// <param name="interleavedKeys">Whether the sequence of keys are interleaved (e.g. MSET [key1] [value1] [key2] [value2]...) or non-interleaved (e.g. MGET [key1] [key2] [key3])</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <param name="retVal">Used to indicate if parsing succeeded or failed due to lack of expected data</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkArraySlotVerify(int keyCount, byte* ptr, bool interleavedKeys, bool readOnly, out bool retVal)
        {
            retVal = false;
            if (clusterSession != null && clusterSession.NetworkArraySlotVerify(keyCount, ref ptr, recvBufferPtr + bytesRead, interleavedKeys, readOnly, SessionAsking, ref dcurr, ref dend, out retVal))
            {
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }
            return false;
        }

        /// <summary>
        /// This method is used to verify slot ownership for provided array of key argslices.
        /// </summary>
        /// <param name="keys">Array of key ArgSlice</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <param name="count">Key count if different than keys array length</param>
        /// <returns>True when ownership is verified, false otherwise</returns>
        bool NetworkKeyArraySlotVerify(ref ArgSlice[] keys, bool readOnly, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(ref keys, readOnly, SessionAsking, ref dcurr, ref dend, count);
    }
}