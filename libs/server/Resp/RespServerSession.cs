// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
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
            catch (Exception ex)
            {
                sessionMetrics?.incr_total_number_resp_server_session_exceptions(1);
                logger?.LogCritical(ex, "ProcessMessages threw exception");
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
                var ptr = recvBufferPtr + readHead;
                var cmd = _authenticator.IsAuthenticated ? FastParseCommand(ptr) : RespCommand.NOAUTH;
                if (cmd == RespCommand.NONE && MakeUpperCase(ptr)) continue;

                bool success;
                if (txnManager.state != TxnState.None)
                {
                    if (txnManager.state == TxnState.Running)
                    {
                        success = ProcessBasicCommands(ptr, cmd, ref lockableGarnetApi);
                    }
                    else success = cmd switch
                    {
                        RespCommand.EXEC => NetworkEXEC(),
                        RespCommand.MULTI => NetworkMULTI(),
                        RespCommand.DISCARD => NetworkDISCARD(),
                        _ => NetworkSKIP(cmd),
                    };
                }
                else
                {
                    success = ProcessBasicCommands(ptr, cmd, ref basicGarnetApi);
                }
                if (!success) break;
                if (latencyMetrics != null) opCount++;
                if (sessionMetrics != null)
                {
                    sessionMetrics.total_commands_processed++;
                    sessionMetrics.total_write_commands_processed += (ulong)(((byte)cmd & 0x80) >> 7);
                    sessionMetrics.total_read_commands_processed += (ulong)(((byte)cmd & 0x40) >> 6);

                    //sessionMetrics.incr_total_commands_processed(1);
                    //sessionMetrics.incr_total_write_commands_processed((byte)cmd);
                    //sessionMetrics.incr_total_read_commands_processed((byte)cmd);
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

        private bool ProcessBasicCommands<TGarnetApi>(byte* ptr, RespCommand cmd, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            bool success = cmd switch
            {
                RespCommand.GET => NetworkGET(ptr, ref storageApi),
                RespCommand.SET => NetworkSET(ptr, ref storageApi),
                RespCommand.SETEX => NetworkSETEX(ptr, false, ref storageApi),
                RespCommand.PSETEX => NetworkSETEX(ptr, true, ref storageApi),
                RespCommand.SETEXNX => NetworkSETEXNX(ptr, ref storageApi),
                RespCommand.DEL => NetworkDEL(ptr, ref storageApi),
                RespCommand.RENAME => NetworkRENAME(ptr, ref storageApi),
                RespCommand.EXISTS => NetworkEXISTS(ptr, ref storageApi),
                RespCommand.EXPIRE => NetworkEXPIRE(ptr, RespCommand.EXPIRE, ref storageApi),
                RespCommand.PEXPIRE => NetworkEXPIRE(ptr, RespCommand.PEXPIRE, ref storageApi),
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

                RespCommand.SETBIT => StringSetBit(ptr, ref storageApi),
                RespCommand.GETBIT => StringGetBit(ptr, ref storageApi),
                RespCommand.BITCOUNT => StringBitCount(ptr, ref storageApi),
                RespCommand.BITPOS => StringBitPosition(ptr, ref storageApi),

                RespCommand.PUBLISH => NetworkPUBLISH(ptr),
                RespCommand.PING => NetworkPING(),
                RespCommand.ASKING => NetworkASKING(),
                RespCommand.MULTI => NetworkMULTI(),
                RespCommand.EXEC => NetworkEXEC(),
                RespCommand.UNWATCH => NetworkUNWATCH(),
                RespCommand.DISCARD => NetworkDISCARD(),
                RespCommand.QUIT => NetworkQUIT(),
                RespCommand.RUNTXP => NetworkRUNTXPFast(ptr),
                RespCommand.READONLY => NetworkREADONLY(),
                RespCommand.READWRITE => NetworkREADWRITE(),
                _ => ProcessArrayCommands(ref storageApi),
            };
            return success;
        }

        private bool ProcessArrayCommands<TGarnetApi>(ref TGarnetApi storageApi)
           where TGarnetApi : IGarnetApi
        {
            if (*(recvBufferPtr + readHead) != '*')
            {
                if (bytesRead - readHead < 2) return false;
                if (*(ushort*)(recvBufferPtr + bytesRead - 2) != 2573)
                    return false;
                readHead = bytesRead;
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERR, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var tmp = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadArrayLength(out int count, ref tmp, recvBufferPtr + bytesRead))
                return false;
            readHead += (int)(tmp - (recvBufferPtr + readHead));

            if (!_authenticator.IsAuthenticated) return ProcessOtherCommands(count, ref storageApi);

            var ptr = recvBufferPtr + readHead;
            var cmd = FastParseArrayCommand(count, recvBufferPtr + readHead);
            var success = cmd switch
            {
                (RespCommand.MGET, 0) => NetworkMGET(count, ptr, ref storageApi),
                (RespCommand.MSET, 0) => NetworkMSET(count, ptr, ref storageApi),
                (RespCommand.MSETNX, 0) => NetworkMSETNX(count, ptr, ref storageApi),
                (RespCommand.DEL, 0) => NetworkDEL(count, ptr, false, ref storageApi),
                (RespCommand.UNLINK, 0) => NetworkDEL(count, ptr, true, ref storageApi),
                (RespCommand.SELECT, 0) => NetworkSELECT(ptr),
                (RespCommand.WATCH, 0) => NetworkWATCH(count, ptr),
                (RespCommand.WATCHMS, 0) => NetworkWATCH(count, ptr, StoreType.Main),
                (RespCommand.WATCHOS, 0) => NetworkWATCH(count, ptr, StoreType.Object),
                (RespCommand.STRLEN, 0) => NetworkSTRLEN(ptr, ref storageApi),
                (RespCommand.EXISTS, 0) => NetworkEXISTS(count, ptr, ref storageApi),
                (RespCommand.MODULE, 0) => NetworkMODULE(ptr, ref storageApi),
                //General key commands
                (RespCommand.DBSIZE, 0) => NetworkDBSIZE(ptr, ref storageApi),
                (RespCommand.KEYS, 0) => NetworkKEYS(count, ptr, ref storageApi),
                (RespCommand.SCAN, 0) => NetworkSCAN(count, ptr, ref storageApi),
                (RespCommand.TYPE, 0) => NetworkTYPE(ptr, ref storageApi),
                // Pub/sub commands
                (RespCommand.SUBSCRIBE, 0) => NetworkSUBSCRIBE(count, ptr, dend),
                (RespCommand.PSUBSCRIBE, 0) => NetworkPSUBSCRIBE(count, ptr, dend),
                (RespCommand.UNSUBSCRIBE, 0) => NetworkUNSUBSCRIBE(count, ptr, dend),
                (RespCommand.PUNSUBSCRIBE, 0) => NetworkPUNSUBSCRIBE(count, ptr, dend),
                // Custom Object Commands
                (RespCommand.All, (byte)RespCommand.COSCAN) => ObjectScan(count, ptr, GarnetObjectType.All, ref storageApi, cmdlength: 23),
                // Sorted Set commands
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZADD) => SortedSetAdd(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZREM) => SortedSetRemove(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZCARD) => SortedSetLength(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMAX) => SortedSetPop(count, ptr, SortedSetOperation.ZPOPMAX, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCORE) => SortedSetScore(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZCOUNT) => SortedSetCount(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZINCRBY) => SortedSetIncrement(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANK) => SortedSetRank(count, ptr, SortedSetOperation.ZRANK, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGE) => SortedSetRange(count, ptr, SortedSetOperation.ZRANGE, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGEBYSCORE) => SortedSetRange(count, ptr, SortedSetOperation.ZRANGEBYSCORE, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANK) => SortedSetRank(count, ptr, SortedSetOperation.ZREVRANK, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYLEX) => SortedSetLengthByValue(count, ptr, SortedSetOperation.ZREMRANGEBYLEX, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYRANK) => SortedSetRemoveRange(count, ptr, SortedSetOperation.ZREMRANGEBYRANK, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYSCORE) => SortedSetRemoveRange(count, ptr, SortedSetOperation.ZREMRANGEBYSCORE, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZLEXCOUNT) => SortedSetLengthByValue(count, ptr, SortedSetOperation.ZLEXCOUNT, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMIN) => SortedSetPop(count, ptr, SortedSetOperation.ZPOPMIN, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANDMEMBER) => SortedSetRandomMember(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZDIFF) => SortedSetDifference(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANGE) => SortedSetReverseRange(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCAN) => ObjectScan(count, ptr, GarnetObjectType.SortedSet, ref storageApi),
                //SortedSet for Geo Commands
                (RespCommand.SortedSet, (byte)SortedSetOperation.GEOADD) => GeoAdd(count, ptr, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.GEOHASH) => GeoCommands(count, ptr, SortedSetOperation.GEOHASH, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.GEODIST) => GeoCommands(count, ptr, SortedSetOperation.GEODIST, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.GEOPOS) => GeoCommands(count, ptr, SortedSetOperation.GEOPOS, ref storageApi),
                (RespCommand.SortedSet, (byte)SortedSetOperation.GEOSEARCH) => GeoCommands(count, ptr, SortedSetOperation.GEOSEARCH, ref storageApi),
                //HLL Commands
                (RespCommand.PFADD, 0) => HyperLogLogAdd(count, ptr, ref storageApi),
                (RespCommand.PFMERGE, 0) => HyperLogLogMerge(count, ptr, ref storageApi),
                (RespCommand.PFCOUNT, 0) => HyperLogLogLength(count, ptr, ref storageApi),
                //Bitmap Commands
                (RespCommand.BITOP, (byte)BitmapOperation.AND or (byte)BitmapOperation.OR or (byte)BitmapOperation.XOR or (byte)BitmapOperation.NOT) => StringBitOperation(count, ptr, (BitmapOperation)cmd.Item2, ref storageApi),
                (RespCommand.BITFIELD, 0) => StringBitField(count, ptr, ref storageApi),
                (RespCommand.BITFIELD_RO, 0) => StringBitFieldReadOnly(count, ptr, ref storageApi),
                // List Commands
                (RespCommand.List, (byte)ListOperation.LPUSH) => ListPush(count, ptr, ListOperation.LPUSH, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LPUSHX) => ListPush(count, ptr, ListOperation.LPUSHX, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LPOP) => ListPop(count, ptr, ListOperation.LPOP, ref storageApi),
                (RespCommand.List, (byte)ListOperation.RPUSH) => ListPush(count, ptr, ListOperation.RPUSH, ref storageApi),
                (RespCommand.List, (byte)ListOperation.RPUSHX) => ListPush(count, ptr, ListOperation.RPUSHX, ref storageApi),
                (RespCommand.List, (byte)ListOperation.RPOP) => ListPop(count, ptr, ListOperation.RPOP, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LLEN) => ListLength(count, ptr, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LTRIM) => ListTrim(count, ptr, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LRANGE) => ListRange(count, ptr, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LINDEX) => ListIndex(count, ptr, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LINSERT) => ListInsert(count, ptr, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LREM) => ListRemove(count, ptr, ref storageApi),
                (RespCommand.List, (byte)ListOperation.RPOPLPUSH) => ListRightPopLeftPush(count, ptr, ref storageApi),
                (RespCommand.List, (byte)ListOperation.LMOVE) => ListMove(count, ptr, ref storageApi),
                // Hash Commands
                (RespCommand.Hash, (byte)HashOperation.HSET) => HashSet(count, ptr, HashOperation.HSET, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HMSET) => HashSet(count, ptr, HashOperation.HMSET, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HGET) => HashGet(count, ptr, HashOperation.HGET, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HMGET) => HashGet(count, ptr, HashOperation.HMGET, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HGETALL) => HashGet(count, ptr, HashOperation.HGETALL, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HDEL) => HashDelete(count, ptr, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HLEN) => HashLength(count, ptr, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HEXISTS) => HashExists(count, ptr, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HKEYS) => HashKeys(count, ptr, HashOperation.HKEYS, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HVALS) => HashKeys(count, ptr, HashOperation.HVALS, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HINCRBY) => HashIncrement(count, ptr, HashOperation.HINCRBY, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HINCRBYFLOAT) => HashIncrement(count, ptr, HashOperation.HINCRBYFLOAT, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HSETNX) => HashSet(count, ptr, HashOperation.HSETNX, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HRANDFIELD) => HashGet(count, ptr, HashOperation.HRANDFIELD, ref storageApi),
                (RespCommand.Hash, (byte)HashOperation.HSCAN) => ObjectScan(count, ptr, GarnetObjectType.Hash, ref storageApi),

                // Set Commands
                (RespCommand.Set, (byte)SetOperation.SADD) => SetAdd(count, ptr, ref storageApi),
                (RespCommand.Set, (byte)SetOperation.SMEMBERS) => SetMembers(count, ptr, ref storageApi),
                (RespCommand.Set, (byte)SetOperation.SREM) => SetRemove(count, ptr, ref storageApi),
                (RespCommand.Set, (byte)SetOperation.SCARD) => SetLength(count, ptr, ref storageApi),
                (RespCommand.Set, (byte)SetOperation.SPOP) => SetPop(count, ptr, ref storageApi),
                (RespCommand.Set, (byte)SetOperation.SSCAN) => ObjectScan(count, ptr, GarnetObjectType.Set, ref storageApi),
                _ => ProcessOtherCommands(count, ref storageApi),
            };
            return success;
        }

        private bool ProcessOtherCommands<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ReadOnlySpan<byte> bufSpan = new(recvBufferPtr, bytesRead);

            var command = GetCommand(bufSpan, out bool success);
            if (!success) return false;

            if (!_authenticator.IsAuthenticated) return ProcessAdminCommands(command, bufSpan, count, ref storageApi);
            if (command.SequenceEqual(CmdStrings.CLIENT))
            {
                for (int i = 0; i < count - 1; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command.SequenceEqual(CmdStrings.SUBSCRIBE))
            {
                for (int i = 0; i < count - 1; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }
                while (!RespWriteUtils.WriteInteger(1, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command.SequenceEqual(CmdStrings.RUNTXP))
            {
                byte* ptr = recvBufferPtr + readHead;
                return NetworkRUNTXP(ptr, count);
            }
            else if (storeWrapper.customCommandManager.Match(command, out CustomTransaction txn))
            {
                byte* ptr = recvBufferPtr + readHead;
                for (int i = 0; i < count - 1; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }

                if (count - 1 != txn.NumParams)
                {
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR Invalid number of parameters to stored proc {txn.nameStr}, expected {txn.NumParams}, actual {count - 1}\r\n"), ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else
                {
                    // Perform the operation
                    TryTransactionProc(txn.id, ptr, recvBufferPtr + readHead, customCommandManagerSession.GetCustomTransactionProcedure(txn.id, txnManager, scratchBufferManager).Item1);
                }
            }
            else if (storeWrapper.customCommandManager.Match(command, out CustomCommand cmd))
            {
                byte* ptr = recvBufferPtr + readHead;
                for (int i = 0; i < count - 1; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }

                if (count - 1 != cmd.NumKeys + cmd.NumParams)
                {
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR Invalid number of parameters, expected {cmd.NumKeys + cmd.NumParams}, actual {count - 1}\r\n"), ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else
                {
                    // Perform the operation
                    TryCustomCommand(ptr, recvBufferPtr + readHead, cmd.GetRespCommand(), cmd.expirationTicks, cmd.type, ref storageApi);
                }
            }
            else if (storeWrapper.customCommandManager.Match(command, out CustomObjectCommand ocmd))
            {
                byte* ptr = recvBufferPtr + readHead;
                for (int i = 0; i < count - 1; i++)
                {
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                }

                if (count - 1 != ocmd.NumKeys + ocmd.NumParams)
                {
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR Invalid number of parameters, expected {ocmd.NumKeys + ocmd.NumParams}, actual {count - 1}\r\n"), ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else
                {
                    // Perform the operation
                    TryCustomObjectCommand(ptr, recvBufferPtr + readHead, ocmd.GetRespCommand(), ocmd.subid, ocmd.type, ref storageApi);
                }
            }
            else
            {
                return ProcessAdminCommands(command, bufSpan, count, ref storageApi);
            }
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

        bool NetworkSingleKeySlotVerify(byte[] key, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(key, readOnly, SessionAsking, ref dcurr, ref dend);

        bool NetworkSingleKeySlotVerify(byte* keyPtr, int ksize, bool readOnly)
            => clusterSession != null && clusterSession.NetworkSingleKeySlotVerify(new ArgSlice(keyPtr, ksize), readOnly, SessionAsking, ref dcurr, ref dend);

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

        bool NetworkKeyArraySlotVerify(ref ArgSlice[] keys, bool readOnly, int count = -1)
            => clusterSession != null && clusterSession.NetworkKeyArraySlotVerify(ref keys, readOnly, SessionAsking, ref dcurr, ref dend, count);
    }
}