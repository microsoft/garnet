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

        GCHandle recvHandle;
        byte* recvBufferPtr;
        int readHead;
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
                networkSender.GetResponseObject();
                ProcessMessages();
                recvBufferPtr = null;
            }
            catch (RespParsingException ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger?.LogCritical($"Aborting open session due to RESP parsing error: {ex.Message}");
                logger?.LogDebug(ex, "RespParsingException in ProcessMessages:");

                // Forward parsing error as RESP error
                while (!RespWriteUtils.WriteError($"ERR Protocol Error: {ex.Message}", ref dcurr, dend))
                    SendAndReset();

                Send(networkSender.GetResponseObjectHead());
                networkSender.Dispose();
            }
            catch (Exception ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger?.LogCritical(ex, "ProcessMessages threw exception:");
                // The session is no longer usable, dispose it
                networkSender.Dispose();
            }
            finally
            {
                networkSender.ReturnResponseObject();
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
            // #endif

            dcurr = networkSender.GetResponseObjectHead();
            dend = networkSender.GetResponseObjectTail();

            var _origReadHead = readHead;

            while (bytesRead - readHead >= 4)
            {
                // NOTE: Possible optimization: Don't parse if only parsing AUTH and not authenticated.
                (RespCommand cmd, byte subcmd) = ParseCommand(out int count, recvBufferPtr + readHead, out bool success);

                var ptr = recvBufferPtr + readHead;

                if (cmd != RespCommand.INVALID)
                {
                    if (txnManager.state != TxnState.None)
                    {
                        if (txnManager.state == TxnState.Running)
                        {
                            success = ProcessBasicCommands(cmd, subcmd, count, ptr, ref lockableGarnetApi);
                        }
                        else success = cmd switch
                        {
                            RespCommand.EXEC => NetworkEXEC(count, ptr),
                            RespCommand.MULTI => NetworkMULTI(count, ptr),
                            RespCommand.DISCARD => NetworkDISCARD(count, ptr),
                            _ => NetworkSKIP(cmd, subcmd, count),
                        };
                    }
                    else
                    {
                        success = ProcessBasicCommands(cmd, subcmd, count, ptr, ref basicGarnetApi);
                    }
                }
                else
                {
                    // Parsing for command name was successful, but the command is unknown
                    if (success)
                    {
                        // Return "Unknown RESP Command" message
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                            SendAndReset();
                    }
                }
                if (!success) break;
                if (latencyMetrics != null) opCount++;
                if (sessionMetrics != null)
                {
                    sessionMetrics.total_commands_processed++;

                    sessionMetrics.total_cluster_commands_processed += cmd.OneIfWrite();
                    sessionMetrics.total_read_commands_processed += cmd.OneIfRead();
                }

                _origReadHead = readHead;
                SessionAsking = (byte)(SessionAsking == 0 ? SessionAsking : SessionAsking - 1);
            }
            readHead = _origReadHead;
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
        private bool ProcessBasicCommands<TGarnetApi>(RespCommand cmd, byte subcmd, int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!_authenticator.IsAuthenticated) return ProcessArrayCommands(cmd, subcmd, count, ref storageApi);

            bool success = cmd switch
            {
                RespCommand.GET => NetworkGET(count, ptr, ref storageApi),
                RespCommand.SET => NetworkSET(count, ptr, ref storageApi),
                RespCommand.SETEX => NetworkSETEX(cmd, count, ptr, false, ref storageApi),
                RespCommand.PSETEX => NetworkSETEX(cmd, count, ptr, true, ref storageApi),
                RespCommand.SETEXNX => NetworkSETEXNX(count, ptr, ref storageApi),
                RespCommand.DEL => NetworkDEL(cmd, count, ptr, ref storageApi),
                RespCommand.RENAME => NetworkRENAME(count, ptr, ref storageApi),
                RespCommand.EXISTS => NetworkEXISTS(count, ptr, ref storageApi),
                RespCommand.EXPIRE => NetworkEXPIRE(count, ptr, RespCommand.EXPIRE, ref storageApi),
                RespCommand.PEXPIRE => NetworkEXPIRE(count, ptr, RespCommand.PEXPIRE, ref storageApi),
                RespCommand.PERSIST => NetworkPERSIST(count, ptr, ref storageApi),
                RespCommand.GETRANGE => NetworkGetRange(count, ptr, ref storageApi),
                RespCommand.TTL => NetworkTTL(count, ptr, RespCommand.TTL, ref storageApi),
                RespCommand.PTTL => NetworkTTL(count, ptr, RespCommand.PTTL, ref storageApi),
                RespCommand.SETRANGE => NetworkSetRange(count, ptr, ref storageApi),
                RespCommand.GETDEL => NetworkGETDEL(count, ptr, ref storageApi),
                RespCommand.APPEND => NetworkAppend(count, ptr, ref storageApi),
                RespCommand.INCR => NetworkIncrement(count, ptr, RespCommand.INCR, ref storageApi),
                RespCommand.INCRBY => NetworkIncrement(count, ptr, RespCommand.INCRBY, ref storageApi),
                RespCommand.DECR => NetworkIncrement(count, ptr, RespCommand.DECR, ref storageApi),
                RespCommand.DECRBY => NetworkIncrement(count, ptr, RespCommand.DECRBY, ref storageApi),
                RespCommand.SETBIT => StringSetBit(count, ptr, ref storageApi),
                RespCommand.GETBIT => StringGetBit(count, ptr, ref storageApi),
                RespCommand.BITCOUNT => StringBitCount(count, ptr, ref storageApi),
                RespCommand.BITPOS => StringBitPosition(count, ptr, ref storageApi),
                RespCommand.PUBLISH => NetworkPUBLISH(count, ptr),
                RespCommand.PING => count == 0 ? NetworkPING(count, ptr) : ProcessArrayCommands(cmd, subcmd, count, ref storageApi),
                RespCommand.ASKING => NetworkASKING(count, ptr),
                RespCommand.MULTI => NetworkMULTI(count, ptr),
                RespCommand.EXEC => NetworkEXEC(count, ptr),
                RespCommand.UNWATCH => NetworkUNWATCH(count, ptr),
                RespCommand.DISCARD => NetworkDISCARD(count, ptr),
                RespCommand.QUIT => NetworkQUIT(count, ptr),
                RespCommand.RUNTXP => NetworkRUNTXP(count, ptr),
                RespCommand.READONLY => NetworkREADONLY(count, ptr),
                RespCommand.READWRITE => NetworkREADWRITE(count, ptr),
                RespCommand.COMMAND => NetworkCOMMAND(count),

                _ => ProcessArrayCommands(cmd, subcmd, count, ref storageApi)
            };
            return success;
        }

        private bool ProcessArrayCommands<TGarnetApi>(RespCommand cmd, byte subcmd, int count, ref TGarnetApi storageApi)
           where TGarnetApi : IGarnetApi
        {
            // Continue reading from the current read head.
            byte* ptr = recvBufferPtr + readHead;

            if (!_authenticator.IsAuthenticated) return ProcessOtherCommands(cmd, subcmd, count, ref storageApi);

            var success = (cmd, subcmd) switch
            {
                (RespCommand.MGET, 0) => NetworkMGET(count, ptr, ref storageApi),
                (RespCommand.MSET, 0) => NetworkMSET(count, ptr, ref storageApi),
                (RespCommand.MSETNX, 0) => NetworkMSETNX(count, ptr, ref storageApi),
                (RespCommand.UNLINK, 0) => NetworkDEL(cmd, count, ptr, ref storageApi),
                (RespCommand.SELECT, 0) => NetworkSELECT(count, ptr),
                (RespCommand.WATCH, 0) => NetworkWATCH(count),
                (RespCommand.STRLEN, 0) => NetworkSTRLEN(count, ptr, ref storageApi),
                (RespCommand.MODULE, 0) => NetworkMODULE(count, ptr, ref storageApi),
                //General key commands
                (RespCommand.DBSIZE, 0) => NetworkDBSIZE(count, ptr, ref storageApi),
                (RespCommand.KEYS, 0) => NetworkKEYS(count, ptr, ref storageApi),
                (RespCommand.SCAN, 0) => NetworkSCAN(count, ptr, ref storageApi),
                (RespCommand.TYPE, 0) => NetworkTYPE(count, ptr, ref storageApi),
                // Pub/sub commands
                (RespCommand.SUBSCRIBE, 0) => NetworkSUBSCRIBE(count, ptr, dend),
                (RespCommand.PSUBSCRIBE, 0) => NetworkPSUBSCRIBE(count, ptr, dend),
                (RespCommand.UNSUBSCRIBE, 0) => NetworkUNSUBSCRIBE(count, ptr, dend),
                (RespCommand.PUNSUBSCRIBE, 0) => NetworkPUNSUBSCRIBE(count, ptr, dend),
                // Custom Object Commands
                (RespCommand.COSCAN, 0) => ObjectScan(cmd, count, ptr, GarnetObjectType.All, ref storageApi),
                // Sorted Set commands
                (RespCommand.ZADD, 0) => SortedSetAdd(count, ptr, ref storageApi),
                (RespCommand.ZREM, 0) => SortedSetRemove(count, ptr, ref storageApi),
                (RespCommand.ZCARD, 0) => SortedSetLength(count, ptr, ref storageApi),
                (RespCommand.ZPOPMAX, 0) => SortedSetPop(cmd, count, ptr, ref storageApi),
                (RespCommand.ZSCORE, 0) => SortedSetScore(count, ptr, ref storageApi),
                (RespCommand.ZMSCORE, 0) => SortedSetScores(count, ptr, ref storageApi),
                (RespCommand.ZCOUNT, 0) => SortedSetCount(count, ptr, ref storageApi),
                (RespCommand.ZINCRBY, 0) => SortedSetIncrement(count, ptr, ref storageApi),
                (RespCommand.ZRANK, 0) => SortedSetRank(cmd, count, ptr, ref storageApi),
                (RespCommand.ZRANGE, 0) => SortedSetRange(cmd, count, ptr, ref storageApi),
                (RespCommand.ZRANGEBYSCORE, 0) => SortedSetRange(cmd, count, ptr, ref storageApi),
                (RespCommand.ZREVRANK, 0) => SortedSetRank(cmd, count, ptr, ref storageApi),
                (RespCommand.ZREMRANGEBYLEX, 0) => SortedSetLengthByValue(cmd, count, ptr, ref storageApi),
                (RespCommand.ZREMRANGEBYRANK, 0) => SortedSetRemoveRange(cmd, count, ptr, ref storageApi),
                (RespCommand.ZREMRANGEBYSCORE, 0) => SortedSetRemoveRange(cmd, count, ptr, ref storageApi),
                (RespCommand.ZLEXCOUNT, 0) => SortedSetLengthByValue(cmd, count, ptr, ref storageApi),
                (RespCommand.ZPOPMIN, 0) => SortedSetPop(cmd, count, ptr, ref storageApi),
                (RespCommand.ZRANDMEMBER, 0) => SortedSetRandomMember(count, ptr, ref storageApi),
                (RespCommand.ZDIFF, 0) => SortedSetDifference(count, ptr, ref storageApi),
                (RespCommand.ZREVRANGE, 0) => SortedSetRange(cmd, count, ptr, ref storageApi),
                (RespCommand.ZSCAN, 0) => ObjectScan(cmd, count, ptr, GarnetObjectType.SortedSet, ref storageApi),
                //SortedSet for Geo Commands
                (RespCommand.GEOADD, 0) => GeoAdd(count, ptr, ref storageApi),
                (RespCommand.GEOHASH, 0) => GeoCommands(cmd, count, ptr, ref storageApi),
                (RespCommand.GEODIST, 0) => GeoCommands(cmd, count, ptr, ref storageApi),
                (RespCommand.GEOPOS, 0) => GeoCommands(cmd, count, ptr, ref storageApi),
                (RespCommand.GEOSEARCH, 0) => GeoCommands(cmd, count, ptr, ref storageApi),
                //HLL Commands
                (RespCommand.PFADD, 0) => HyperLogLogAdd(count, ptr, ref storageApi),
                (RespCommand.PFMERGE, 0) => HyperLogLogMerge(count, ptr, ref storageApi),
                (RespCommand.PFCOUNT, 0) => HyperLogLogLength(count, ptr, ref storageApi),
                //Bitmap Commands
                (RespCommand.BITOP, (byte)BitmapOperation.AND or (byte)BitmapOperation.OR or (byte)BitmapOperation.XOR or (byte)BitmapOperation.NOT) => StringBitOperation(count, ptr, (BitmapOperation)subcmd, ref storageApi),
                (RespCommand.BITFIELD, 0) => StringBitField(count, ptr, ref storageApi),
                (RespCommand.BITFIELD_RO, 0) => StringBitFieldReadOnly(count, ptr, ref storageApi),
                // List Commands
                (RespCommand.LPUSH, 0) => ListPush(cmd, count, ptr, ref storageApi),
                (RespCommand.LPUSHX, 0) => ListPush(cmd, count, ptr, ref storageApi),
                (RespCommand.LPOP, 0) => ListPop(cmd, count, ptr, ref storageApi),
                (RespCommand.RPUSH, 0) => ListPush(cmd, count, ptr, ref storageApi),
                (RespCommand.RPUSHX, 0) => ListPush(cmd, count, ptr, ref storageApi),
                (RespCommand.RPOP, 0) => ListPop(cmd, count, ptr, ref storageApi),
                (RespCommand.LLEN, 0) => ListLength(count, ptr, ref storageApi),
                (RespCommand.LTRIM, 0) => ListTrim(count, ptr, ref storageApi),
                (RespCommand.LRANGE, 0) => ListRange(count, ptr, ref storageApi),
                (RespCommand.LINDEX, 0) => ListIndex(count, ptr, ref storageApi),
                (RespCommand.LINSERT, 0) => ListInsert(count, ptr, ref storageApi),
                (RespCommand.LREM, 0) => ListRemove(count, ptr, ref storageApi),
                (RespCommand.RPOPLPUSH, 0) => ListRightPopLeftPush(count, ptr, ref storageApi),
                (RespCommand.LMOVE, 0) => ListMove(count, ptr, ref storageApi),
                (RespCommand.LSET, 0) => ListSet(count, ptr, ref storageApi),
                // Hash Commands
                (RespCommand.HSET, 0) => HashSet(cmd, count, ptr, ref storageApi),
                (RespCommand.HMSET, 0) => HashSet(cmd, count, ptr, ref storageApi),
                (RespCommand.HGET, 0) => HashGet(cmd, count, ptr, ref storageApi),
                (RespCommand.HMGET, 0) => HashGet(cmd, count, ptr, ref storageApi),
                (RespCommand.HGETALL, 0) => HashGet(cmd, count, ptr, ref storageApi),
                (RespCommand.HDEL, 0) => HashDelete(cmd, count, ptr, ref storageApi),
                (RespCommand.HLEN, 0) => HashLength(cmd, count, ptr, ref storageApi),
                (RespCommand.HSTRLEN, 0) => HashStrLength(cmd, count, ptr, ref storageApi),
                (RespCommand.HEXISTS, 0) => HashExists(cmd, count, ptr, ref storageApi),
                (RespCommand.HKEYS, 0) => HashKeys(cmd, count, ptr, ref storageApi),
                (RespCommand.HVALS, 0) => HashKeys(cmd, count, ptr, ref storageApi),
                (RespCommand.HINCRBY, 0) => HashIncrement(cmd, count, ptr, ref storageApi),
                (RespCommand.HINCRBYFLOAT, 0) => HashIncrement(cmd, count, ptr, ref storageApi),
                (RespCommand.HSETNX, 0) => HashSet(cmd, count, ptr, ref storageApi),
                (RespCommand.HRANDFIELD, 0) => HashGet(cmd, count, ptr, ref storageApi),
                (RespCommand.HSCAN, 0) => ObjectScan(cmd, count, ptr, GarnetObjectType.Hash, ref storageApi),
                // Set Commands
                (RespCommand.SADD, 0) => SetAdd(count, ptr, ref storageApi),
                (RespCommand.SMEMBERS, 0) => SetMembers(count, ptr, ref storageApi),
                (RespCommand.SISMEMBER, 0) => SetIsMember(count, ptr, ref storageApi),
                (RespCommand.SREM, 0) => SetRemove(count, ptr, ref storageApi),
                (RespCommand.SCARD, 0) => SetLength(count, ptr, ref storageApi),
                (RespCommand.SPOP, 0) => SetPop(count, ptr, ref storageApi),
                (RespCommand.SRANDMEMBER, 0) => SetRandomMember(count, ptr, ref storageApi),
                (RespCommand.SSCAN, 0) => ObjectScan(cmd, count, ptr, GarnetObjectType.Set, ref storageApi),
                (RespCommand.SMOVE, 0) => SetMove(count, ptr, ref storageApi),
                (RespCommand.SUNION, 0) => SetUnion(count, ptr, ref storageApi),
                (RespCommand.SUNIONSTORE, 0) => SetUnionStore(count, ptr, ref storageApi),
                (RespCommand.SDIFF, 0) => SetDiff(count, ptr, ref storageApi),
                (RespCommand.SDIFFSTORE, 0) => SetDiffStore(count, ptr, ref storageApi),
                _ => ProcessOtherCommands(cmd, subcmd, count, ref storageApi),
            };
            return success;
        }

        private bool ProcessOtherCommands<TGarnetApi>(RespCommand command, byte subcmd, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ReadOnlySpan<byte> bufSpan = new(recvBufferPtr, bytesRead);

            if (!_authenticator.IsAuthenticated) return ProcessAdminCommands(command, bufSpan, count, ref storageApi);

            if (command == RespCommand.CLIENT)
            {
                if (!CheckACLPermissions(command, RespCommandsInfo.SubCommandIds.None, count, out bool success))
                {
                    return success;
                }

                for (int i = 0; i < count; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.SUBSCRIBE)
            {
                for (int i = 0; i < count; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }
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
                byte* ptr = recvBufferPtr + readHead;
                for (int i = 0; i < count; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }

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
                    TryTransactionProc(currentCustomTransaction.id, ptr, recvBufferPtr + readHead, customCommandManagerSession.GetCustomTransactionProcedure(currentCustomTransaction.id, txnManager, scratchBufferManager).Item1);
                }

                currentCustomTransaction = null;
            }
            else if (command == RespCommand.CustomCmd)
            {
                byte* ptr = recvBufferPtr + readHead;
                for (int i = 0; i < count; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }

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
                    TryCustomCommand(ptr, recvBufferPtr + readHead, currentCustomCommand.GetRespCommand(), currentCustomCommand.expirationTicks, currentCustomCommand.type, ref storageApi);
                }

                currentCustomCommand = null;
            }
            else if (command == RespCommand.CustomObjCmd)
            {
                byte* ptr = recvBufferPtr + readHead;
                for (int i = 0; i < count; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }

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
                    TryCustomObjectCommand(ptr, recvBufferPtr + readHead, currentCustomObjectCommand.GetRespCommand(), currentCustomObjectCommand.subid, currentCustomObjectCommand.type, ref storageApi);
                }

                currentCustomObjectCommand = null;
            }

            else
            {
                return ProcessAdminCommands(command, bufSpan, count, ref storageApi);
            }
            return true;
        }
        bool DrainCommands(ReadOnlySpan<byte> bufSpan, int count)
        {
            for (var i = 0; i < count; i++)
            {
                GetCommand(bufSpan, out bool success1);
                if (!success1) return false;
            }
            return true;
        }

        Span<byte> GetCommand(ReadOnlySpan<byte> bufSpan, out bool success)
        {
            var ptr = recvBufferPtr + readHead;
            var end = recvBufferPtr + bytesRead;

            // Try the command length
            if (!RespReadUtils.ReadLengthHeader(out int length, ref ptr, end))
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
                GarnetException.Throw("Failed to write to response buffer");
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
        /// <returns>True when ownernship is verified, false otherwise</returns>
        bool NetworkSingleKeySlotVerify(byte[] key, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(key, readOnly, SessionAsking, ref dcurr, ref dend);

        /// <summary>
        /// This method is used to verify slot ownership for provided key sequence.
        /// On error this method writes to response buffer but does not drain recv buffer (caller is responsible for draining).
        /// </summary>
        /// <param name="keyPtr">Pointer to key bytes</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <returns>True when ownernship is verified, false otherwise</returns>
        bool NetworkSingleKeySlotVerify(byte* keyPtr, int ksize, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(new ArgSlice(keyPtr, ksize), readOnly, SessionAsking, ref dcurr, ref dend);

        /// <summary>
        /// This method is used to verify slot ownership for provided sequence of keys.
        /// On error this method writes to response buffer and drains recv buffer.
        /// </summary>
        /// <param name="keyCount">Number of keys</param>
        /// <param name="ptr">Starting poistion of RESP formatted key sequence</param>
        /// <param name="interleavedKeys">Whether the sequence of keys are interleaved (e.g. MSET [key1] [value1] [key2] [value2]...) or non-interleaved (e.g. MGET [key1] [key2] [key3])</param>
        /// <param name="readOnly">Whether caller is going to perform a readonly or read/write operation</param>
        /// <param name="retVal">Used to indicate if parsing succeeded or failed due to lack of expected data</param>
        /// <returns>True when ownernship is verified, false otherwise</returns>
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
        /// <returns>True when ownernship is verified, false otherwise</returns>
        bool NetworkKeyArraySlotVerify(ref ArgSlice[] keys, bool readOnly, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(ref keys, readOnly, SessionAsking, ref dcurr, ref dend, count);
    }
}