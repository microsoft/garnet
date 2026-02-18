// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Garnet.server.TLS;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;
using Tsavorite.devices;

namespace Garnet.test
{
    public struct StoreAddressInfo
    {
        public long BeginAddress;
        public long HeadAddress;
        public long ReadOnlyAddress;
        public long TailAddress;
        public long MemorySize;
        public long ReadCacheBeginAddress;
        public long ReadCacheTailAddress;
    }

    /// <summary>
    /// Get all attributes that start with given prefix.
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class ValuesPrefixAttribute : NUnitAttribute, IParameterDataSource
    {
        readonly string prefix;

        public ValuesPrefixAttribute(string prefix)
        {
            this.prefix = prefix;
        }

        public IEnumerable GetData(IParameterInfo parameter)
        {
            return new ValuesAttribute()
                .GetData(parameter)
                .Cast<object>()
                .Where(e => e.ToString().StartsWith(prefix));
        }
    }

    public enum RevivificationMode
    {
        NoReviv = 0,
        UseReviv = 1,
    }

    internal static class TestUtils
    {
        public static readonly int TestPort = 33278;

        /// <summary>
        /// Test server end point
        /// </summary>
        public static EndPoint EndPoint = new IPEndPoint(IPAddress.Loopback, TestPort);

        /// <summary>
        /// Whether to use a test progress logger
        /// </summary>
        static readonly bool useTestLogger = false;

        internal static string CustomRespCommandInfoJsonPath = "CustomRespCommandsInfo.json";
        internal static string CustomRespCommandDocsJsonPath = "CustomRespCommandsDocs.json";

        private static bool CustomCommandsInfoInitialized;
        private static bool CustomCommandsDocsInitialized;
        private static IReadOnlyDictionary<string, RespCommandsInfo> RespCustomCommandsInfo;
        private static IReadOnlyDictionary<string, RespCommandDocs> RespCustomCommandsDocs;

        internal static string AzureTestContainer
        {
            get
            {
                var container = "Garnet.test".Replace('.', '-').ToLowerInvariant();
                return container;
            }
        }
        internal static string AzureTestDirectory => TestContext.CurrentContext.Test.MethodName;
        internal const string AzureEmulatedStorageString = "UseDevelopmentStorage=true;";
        internal static AzureStorageNamedDeviceFactoryCreator AzureStorageNamedDeviceFactoryCreator =
            IsRunningAzureTests ? new AzureStorageNamedDeviceFactoryCreator(AzureEmulatedStorageString, null) : null;

        public const string certFile = "testcert.pfx";
        public const string certPassword = "placeholder";

        internal static bool IsRunningAzureTests
        {
            get
            {
                if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")) ||
                    "yes".Equals(Environment.GetEnvironmentVariable("RUNAZURETESTS")) ||
                    IsAzuriteRunning())
                {
                    return true;
                }
                return false;
            }
        }

        internal static bool IsRunningAsGitHubAction
        => "true".Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), StringComparison.OrdinalIgnoreCase);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void AssertEqualUpToExpectedLength(string expectedResponse, byte[] response)
        {
            ClassicAssert.AreEqual(expectedResponse, Encoding.ASCII.GetString(response, 0, expectedResponse.Length));
        }

        /// <summary>
        /// Get command info for custom commands defined in custom commands json file
        /// </summary>
        /// <param name="customCommandsInfo">Mapping between command name and command info</param>
        /// <param name="logger">Logger</param>
        /// <returns></returns>
        internal static bool TryGetCustomCommandsInfo(out IReadOnlyDictionary<string, RespCommandsInfo> customCommandsInfo, ILogger logger = null)
        {
            customCommandsInfo = default;

            if (!CustomCommandsInfoInitialized && !TryInitializeCustomCommandsInfo(logger)) return false;

            customCommandsInfo = RespCustomCommandsInfo;
            return true;
        }

        /// <summary>
        /// Get command info for custom commands defined in custom commands json file
        /// </summary>
        /// <param name="customCommandsDocs">Mapping between command name and command info</param>
        /// <param name="logger">Logger</param>
        /// <returns></returns>
        internal static bool TryGetCustomCommandsDocs(out IReadOnlyDictionary<string, RespCommandDocs> customCommandsDocs, ILogger logger = null)
        {
            customCommandsDocs = default;

            if (!CustomCommandsDocsInitialized && !TryInitializeCustomCommandsDocs(logger)) return false;

            customCommandsDocs = RespCustomCommandsDocs;
            return true;
        }

        private static bool TryInitializeCustomCommandsInfo(ILogger logger)
        {
            if (!TryGetRespCommandData<RespCommandsInfo>(CustomRespCommandInfoJsonPath, logger, out var tmpCustomCommandsInfo))
                return false;

            RespCustomCommandsInfo = tmpCustomCommandsInfo;
            CustomCommandsInfoInitialized = true;
            return true;
        }

        private static bool TryInitializeCustomCommandsDocs(ILogger logger)
        {
            if (!TryGetRespCommandData<RespCommandDocs>(CustomRespCommandDocsJsonPath, logger, out var tmpCustomCommandsDocs))
                return false;

            RespCustomCommandsDocs = tmpCustomCommandsDocs;
            CustomCommandsDocsInitialized = true;
            return true;
        }

        private static bool TryGetRespCommandData<TData>(string resourcePath, ILogger logger, out IReadOnlyDictionary<string, TData> commandData)
        where TData : class, IRespCommandData<TData>
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
            var commandsInfoProvider = RespCommandsDataProviderFactory.GetRespCommandsDataProvider<TData>();

            return commandsInfoProvider.TryImportRespCommandsData(resourcePath,
                streamProvider, out commandData, logger);
        }

        static bool IsAzuriteRunning()
        {
            // If Azurite is running, it will run on localhost and listen on port 10000 and/or 10001.
            var expectedIp = IPAddress.Loopback;
            var expectedPorts = new[] { 10000, 10001 };

            var activeTcpListeners = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners();

            var relevantListeners = activeTcpListeners.Where(t =>
                    expectedPorts.Contains(t.Port) &&
                    t.Address == expectedIp)
                .ToList();

            return relevantListeners.Any();
        }

        internal static void IgnoreIfNotRunningAzureTests()
        {
            // Need this environment variable set AND Azure Storage Emulator running
            if (!IsRunningAzureTests)
                Assert.Ignore("Environment variable RunAzureTests is not defined");
        }

        /// <summary>
        /// Create GarnetServer
        /// </summary>
        public static GarnetServer CreateGarnetServer(
            string logCheckpointDir,
            EndPoint[] endpoints = null,
            bool disablePubSub = false,
            bool tryRecover = false,
            bool lowMemory = false,
            string memorySize = default,
            string objectStoreLogMemorySize = default,
            string pageSize = default,
            bool enableAOF = false,
            bool enableTLS = false,
            bool disableObjects = false,
            int metricsSamplingFreq = -1,
            bool latencyMonitor = false,
            int commitFrequencyMs = 0,
            bool commitWait = false,
            bool useAzureStorage = false,
            string defaultPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
            string objectStorePageSize = default,
            string objectStoreHeapMemorySize = default,
            string objectStoreIndexSize = "16k",
            string objectStoreIndexMaxSize = default,
            string objectStoreReadCacheHeapMemorySize = default,
            string indexSize = "1m",
            string indexMaxSize = default,
            string[] extensionBinPaths = null,
            bool extensionAllowUnsignedAssemblies = true,
            bool getSG = false,
            int indexResizeFrequencySecs = 60,
            IAuthenticationSettings authenticationSettings = null,
            ConnectionProtectionOption enableDebugCommand = ConnectionProtectionOption.Yes,
            ConnectionProtectionOption enableModuleCommand = ConnectionProtectionOption.No,
            bool enableLua = false,
            bool enableReadCache = false,
            bool enableObjectStoreReadCache = false,
            ILogger logger = null,
            IEnumerable<string> loadModulePaths = null,
            string pubSubPageSize = null,
            bool asyncReplay = false,
            LuaMemoryManagementMode luaMemoryMode = LuaMemoryManagementMode.Native,
            string luaMemoryLimit = "",
            TimeSpan? luaTimeout = null,
            LuaLoggingMode luaLoggingMode = LuaLoggingMode.Enable,
            IEnumerable<string> luaAllowedFunctions = null,
            string unixSocketPath = null,
            UnixFileMode unixSocketPermission = default,
            int slowLogThreshold = 0,
            TextWriter logTo = null,
            bool enableCluster = false,
            int expiredKeyDeletionScanFrequencySecs = -1,
            bool useReviv = false,
            bool useInChainRevivOnly = false,
            bool useLogNullDevice = false,
            bool enableVectorSetPreview = true
        )
        {
            if (useAzureStorage)
                IgnoreIfNotRunningAzureTests();
            var logDir = useLogNullDevice ? null : logCheckpointDir;
            if (useAzureStorage && !useLogNullDevice)
                logDir = $"{AzureTestContainer}/{AzureTestDirectory}";

            if (logCheckpointDir != null && !useAzureStorage && !useLogNullDevice) logDir = new DirectoryInfo(string.IsNullOrEmpty(logDir) ? "." : logDir).FullName;

            var checkpointDir = logCheckpointDir;
            if (useAzureStorage)
                checkpointDir = $"{AzureTestContainer}/{AzureTestDirectory}";

            if (logCheckpointDir != null && !useAzureStorage) checkpointDir = new DirectoryInfo(string.IsNullOrEmpty(checkpointDir) ? "." : checkpointDir).FullName;

            if (useAcl)
            {
                if (authenticationSettings != null)
                {
                    throw new ArgumentException($"Cannot set both {nameof(useAcl)} and {nameof(authenticationSettings)}");
                }

                authenticationSettings = new AclAuthenticationPasswordSettings(aclFile, defaultPassword);
            }
            else if (defaultPassword != null)
            {
                if (authenticationSettings != null)
                {
                    throw new ArgumentException($"Cannot set both {nameof(defaultPassword)} and {nameof(authenticationSettings)}");
                }

                authenticationSettings = new PasswordAuthenticationSettings(defaultPassword);
            }

            // Increase minimum thread pool size to 16 if needed
            int threadPoolMinThreads = 0;
            ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);
            if (workerThreads < 16 || completionPortThreads < 16) threadPoolMinThreads = 16;

            GarnetServerOptions opts = new(logger)
            {
                EnableStorageTier = logDir != null,
                LogDir = logDir,
                CheckpointDir = checkpointDir,
                EndPoints = endpoints ?? ([EndPoint]),
                DisablePubSub = disablePubSub,
                Recover = tryRecover,
                IndexSize = indexSize,
                ObjectStoreIndexSize = objectStoreIndexSize,
                EnableAOF = enableAOF,
                EnableLua = enableLua,
                CommitFrequencyMs = commitFrequencyMs,
                WaitForCommit = commitWait,
                TlsOptions = enableTLS ? new GarnetTlsOptions(
                    certFileName: certFile,
                    certPassword: certPassword,
                    clientCertificateRequired: true,
                    certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                    issuerCertificatePath: null,
                    null, 0, false, null, logger: logger)
                : null,
                DisableObjects = disableObjects,
                QuietMode = true,
                MetricsSamplingFrequency = metricsSamplingFreq,
                LatencyMonitor = latencyMonitor,
                DeviceFactoryCreator = useAzureStorage ?
                        logger == null ? TestUtils.AzureStorageNamedDeviceFactoryCreator : new AzureStorageNamedDeviceFactoryCreator(AzureEmulatedStorageString, logger)
                        : new LocalStorageNamedDeviceFactoryCreator(logger: logger),
                AuthSettings = authenticationSettings,
                ExtensionBinPaths = extensionBinPaths,
                ExtensionAllowUnsignedAssemblies = extensionAllowUnsignedAssemblies,
                EnableScatterGatherGet = getSG,
                IndexResizeFrequencySecs = indexResizeFrequencySecs,
                ThreadPoolMinThreads = threadPoolMinThreads,
                LoadModuleCS = loadModulePaths,
                EnableCluster = enableCluster,
                EnableDebugCommand = enableDebugCommand,
                EnableModuleCommand = enableModuleCommand,
                EnableReadCache = enableReadCache,
                EnableObjectStoreReadCache = enableObjectStoreReadCache,
                ReplicationOffsetMaxLag = asyncReplay ? -1 : 0,
                LuaOptions = enableLua ? new LuaOptions(luaMemoryMode, luaMemoryLimit, luaTimeout ?? Timeout.InfiniteTimeSpan, luaLoggingMode, luaAllowedFunctions ?? [], logger) : null,
                UnixSocketPath = unixSocketPath,
                UnixSocketPermission = unixSocketPermission,
                SlowLogThreshold = slowLogThreshold,
                ExpiredKeyDeletionScanFrequencySecs = expiredKeyDeletionScanFrequencySecs,
                EnableVectorSetPreview = enableVectorSetPreview,
            };

            if (!string.IsNullOrEmpty(memorySize))
                opts.MemorySize = memorySize;

            if (!string.IsNullOrEmpty(objectStoreLogMemorySize))
                opts.ObjectStoreLogMemorySize = objectStoreLogMemorySize;

            if (!string.IsNullOrEmpty(pageSize))
                opts.PageSize = pageSize;

            if (!string.IsNullOrEmpty(pubSubPageSize))
                opts.PubSubPageSize = pubSubPageSize;

            if (!string.IsNullOrEmpty(objectStorePageSize))
                opts.ObjectStorePageSize = objectStorePageSize;

            if (!string.IsNullOrEmpty(objectStoreHeapMemorySize))
                opts.ObjectStoreHeapMemorySize = objectStoreHeapMemorySize;

            if (!string.IsNullOrEmpty(objectStoreReadCacheHeapMemorySize))
                opts.ObjectStoreReadCacheHeapMemorySize = objectStoreReadCacheHeapMemorySize;

            if (indexMaxSize != default) opts.IndexMaxSize = indexMaxSize;
            if (objectStoreIndexMaxSize != default) opts.ObjectStoreIndexMaxSize = objectStoreIndexMaxSize;

            if (lowMemory)
            {
                opts.MemorySize = opts.ObjectStoreLogMemorySize = memorySize == default ? "1024" : memorySize;
                opts.PageSize = opts.ObjectStorePageSize = pageSize == default ? "512" : pageSize;
                if (enableReadCache)
                {
                    opts.ReadCacheMemorySize = opts.MemorySize;
                    opts.ReadCachePageSize = opts.PageSize;
                }

                if (enableObjectStoreReadCache)
                {
                    opts.ObjectStoreReadCacheLogMemorySize = opts.MemorySize;
                    opts.ObjectStoreReadCachePageSize = opts.PageSize;
                }
            }

            ILoggerFactory loggerFactory = null;
            if (useTestLogger || logTo != null)
            {
                loggerFactory = LoggerFactory.Create(builder =>
                {
                    if (useTestLogger)
                    {
                        _ = builder.AddProvider(new NUnitLoggerProvider(TestContext.Progress, TestContext.CurrentContext.Test.MethodName, null, false, false, LogLevel.Trace));
                    }

                    if (logTo != null)
                    {
                        _ = builder.AddProvider(new NUnitLoggerProvider(logTo, logLevel: LogLevel.Trace));
                    }

                    _ = builder.SetMinimumLevel(LogLevel.Trace);
                });
            }

            if (useReviv)
            {
                opts.UseRevivBinsPowerOf2 = true;
                opts.RevivBinBestFitScanLimit = 0;
                opts.RevivNumberOfBinsToSearch = int.MaxValue;
                opts.RevivifiableFraction = 1;
                opts.RevivInChainOnly = false;
                opts.RevivBinRecordCounts = [];
                opts.RevivBinRecordSizes = [];
                opts.RevivObjBinRecordCount = 256;
            }

            if (useInChainRevivOnly)
            {
                opts.RevivInChainOnly = true;
            }

            return new GarnetServer(opts, loggerFactory);
        }

        /// <summary>
        /// Create logger factory for given TextWriter and loglevel
        /// E.g. Use with TestContext.Progress to print logs while test is running.
        /// </summary>
        /// <param name="textWriter"></param>
        /// <param name="logLevel"></param>
        /// <param name="scope"></param>
        /// <param name="skipCmd"></param>
        /// <param name="recvOnly"></param>
        /// <param name="matchLevel"></param>
        /// <returns></returns>
        public static ILoggerFactory CreateLoggerFactoryInstance(TextWriter textWriter, LogLevel logLevel, string scope = "", HashSet<string> skipCmd = null, bool recvOnly = false, bool matchLevel = false)
        {
            return LoggerFactory.Create(builder =>
            {
                builder.AddProvider(new NUnitLoggerProvider(textWriter, scope, skipCmd, recvOnly, matchLevel, logLevel));
                builder.SetMinimumLevel(logLevel);
            });
        }

        public static (GarnetServer[] Nodes, GarnetServerOptions[] Options) CreateGarnetCluster(
            string checkpointDir,
            EndPointCollection endpoints,
            bool enableCluster = true,
            bool disablePubSub = false,
            bool disableObjects = false,
            bool tryRecover = false,
            bool enableAOF = false,
            int timeout = -1,
            int gossipDelay = 1,
            bool UseAzureStorage = false,
            bool UseTLS = false,
            bool cleanClusterConfig = false,
            bool lowMemory = false,
            string MemorySize = default,
            string PageSize = default,
            string SegmentSize = "1g",
            bool FastAofTruncate = false,
            string AofMemorySize = "64m",
            bool OnDemandCheckpoint = false,
            int CommitFrequencyMs = 0,
            bool useAofNullDevice = false,
            bool DisableStorageTier = false,
            bool EnableIncrementalSnapshots = false,
            bool FastCommit = true,
            string authUsername = null,
            string authPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
            X509CertificateCollection certificates = null,
            ILoggerFactory loggerFactory = null,
            AadAuthenticationSettings authenticationSettings = null,
            int metricsSamplingFrequency = 0,
            bool enableLua = false,
            bool asyncReplay = false,
            bool enableDisklessSync = false,
            int replicaDisklessSyncDelay = 1,
            string replicaDisklessSyncFullSyncAofThreshold = null,
            LuaMemoryManagementMode luaMemoryMode = LuaMemoryManagementMode.Native,
            string luaMemoryLimit = "",
            EndPoint clusterAnnounceEndpoint = null,
            bool luaTransactionMode = false,
            DeviceType deviceType = DeviceType.Default,
            int clusterReplicationReestablishmentTimeout = 0,
            string aofSizeLimit = "",
            int compactionFrequencySecs = 0,
            LogCompactionType compactionType = LogCompactionType.Scan,
            bool latencyMonitory = false,
            int metricSamplingFrequencySecs = 0,
            int loggingFrequencySecs = 5,
            int checkpointThrottleFlushDelayMs = 0,
            bool clusterReplicaResumeWithData = false,
            int replicaSyncTimeout = 60,
            int expiredObjectCollectionFrequencySecs = 0,
            ClusterPreferredEndpointType clusterPreferredEndpointType = ClusterPreferredEndpointType.Ip,
            string clusterAnnounceHostname = null)
        {
            if (UseAzureStorage)
                IgnoreIfNotRunningAzureTests();
            var nodes = new GarnetServer[endpoints.Count];
            var opts = new GarnetServerOptions[nodes.Length];
            for (var i = 0; i < nodes.Length; i++)
            {
                var endpoint = (IPEndPoint)endpoints[i];

                opts[i] = GetGarnetServerOptions(
                    checkpointDir,
                    checkpointDir,
                    endpoint,
                    enableCluster: enableCluster,
                    disablePubSub,
                    disableObjects,
                    tryRecover,
                    enableAOF,
                    timeout,
                    gossipDelay,
                    UseAzureStorage,
                    useTLS: UseTLS,
                    cleanClusterConfig: cleanClusterConfig,
                    lowMemory: lowMemory,
                    memorySize: MemorySize,
                    pageSize: PageSize,
                    segmentSize: SegmentSize,
                    fastAofTruncate: FastAofTruncate,
                    aofMemorySize: AofMemorySize,
                    onDemandCheckpoint: OnDemandCheckpoint,
                    commitFrequencyMs: CommitFrequencyMs,
                    useAofNullDevice: useAofNullDevice,
                    disableStorageTier: DisableStorageTier,
                    enableIncrementalSnapshots: EnableIncrementalSnapshots,
                    fastCommit: FastCommit,
                    authUsername: authUsername,
                    authPassword: authPassword,
                    useAcl: useAcl,
                    aclFile: aclFile,
                    certificates: certificates,
                    logger: loggerFactory?.CreateLogger("GarnetServer"),
                    aadAuthenticationSettings: authenticationSettings,
                    metricsSamplingFrequency: metricsSamplingFrequency,
                    enableLua: enableLua,
                    asyncReplay: asyncReplay,
                    enableDisklessSync: enableDisklessSync,
                    replicaDisklessSyncDelay: replicaDisklessSyncDelay,
                    replicaDisklessSyncFullSyncAofThreshold: replicaDisklessSyncFullSyncAofThreshold,
                    luaMemoryMode: luaMemoryMode,
                    luaMemoryLimit: luaMemoryLimit,
                    clusterAnnounceEndpoint: clusterAnnounceEndpoint,
                    luaTransactionMode: luaTransactionMode,
                    deviceType: deviceType,
                    clusterReplicationReestablishmentTimeout: clusterReplicationReestablishmentTimeout,
                    aofSizeLimit: aofSizeLimit,
                    compactionFrequencySecs: compactionFrequencySecs,
                    compactionType: compactionType,
                    latencyMonitory: latencyMonitory,
                    loggingFrequencySecs: loggingFrequencySecs,
                    checkpointThrottleFlushDelayMs: checkpointThrottleFlushDelayMs,
                    clusterReplicaResumeWithData: clusterReplicaResumeWithData,
                    replicaSyncTimeout: replicaSyncTimeout,
                    expiredObjectCollectionFrequencySecs: expiredObjectCollectionFrequencySecs,
                    clusterPreferredEndpointType: clusterPreferredEndpointType,
                    clusterAnnounceHostname: clusterAnnounceHostname);

                ClassicAssert.IsNotNull(opts);

                if (opts[i].EndPoints[0] is IPEndPoint ipEndpoint)
                {
                    var iter = 0;
                    while (!IsPortAvailable(ipEndpoint.Port))
                    {
                        ClassicAssert.Less(30, iter, "Failed to connect within 30 seconds");
                        TestContext.Progress.WriteLine($"Waiting for Port {ipEndpoint.Port} to become available for {TestContext.CurrentContext.WorkerId}:{iter++}");
                        Thread.Sleep(1000);
                    }
                }

                nodes[i] = new GarnetServer(opts[i], loggerFactory);
            }
            return (nodes, opts);
        }

        public static GarnetServerOptions GetGarnetServerOptions(
            string checkpointDir,
            string logDir,
            EndPoint endpoint,
            bool enableCluster = true,
            bool disablePubSub = false,
            bool disableObjects = false,
            bool tryRecover = false,
            bool enableAOF = false,
            int timeout = -1,
            int gossipDelay = 5,
            bool useAzureStorage = false,
            bool useTLS = false,
            bool cleanClusterConfig = false,
            bool lowMemory = false,
            string memorySize = default,
            string pageSize = default,
            string segmentSize = "1g",
            bool fastAofTruncate = false,
            string aofMemorySize = "64m",
            bool onDemandCheckpoint = false,
            int commitFrequencyMs = 0,
            bool useAofNullDevice = false,
            bool disableStorageTier = false,
            bool enableIncrementalSnapshots = false,
            bool fastCommit = true,
            string authUsername = null,
            string authPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
            X509CertificateCollection certificates = null,
            AadAuthenticationSettings aadAuthenticationSettings = null,
            int metricsSamplingFrequency = 0,
            bool enableLua = false,
            bool asyncReplay = false,
            bool enableDisklessSync = false,
            int replicaDisklessSyncDelay = 1,
            string replicaDisklessSyncFullSyncAofThreshold = null,
            ILogger logger = null,
            LuaMemoryManagementMode luaMemoryMode = LuaMemoryManagementMode.Native,
            string luaMemoryLimit = "",
            TimeSpan? luaTimeout = null,
            LuaLoggingMode luaLoggingMode = LuaLoggingMode.Enable,
            IEnumerable<string> luaAllowedFunctions = null,
            string unixSocketPath = null,
            EndPoint clusterAnnounceEndpoint = null,
            bool luaTransactionMode = false,
            DeviceType deviceType = DeviceType.Default,
            int clusterReplicationReestablishmentTimeout = 0,
            string aofSizeLimit = "",
            int compactionFrequencySecs = 0,
            LogCompactionType compactionType = LogCompactionType.Scan,
            bool latencyMonitory = false,
            int loggingFrequencySecs = 5,
            int checkpointThrottleFlushDelayMs = 0,
            bool clusterReplicaResumeWithData = false,
            int replicaSyncTimeout = 60,
            int expiredObjectCollectionFrequencySecs = 0,
            ClusterPreferredEndpointType clusterPreferredEndpointType = ClusterPreferredEndpointType.Ip,
            string clusterAnnounceHostname = null,
            bool enableVectorSetPreview = true)
        {
            if (useAzureStorage)
                IgnoreIfNotRunningAzureTests();

            if (useAzureStorage)
            {
                logDir = Path.Join(AzureTestContainer, AzureTestDirectory);
                checkpointDir = Path.Join(AzureTestContainer, AzureTestDirectory);
            }

            if (endpoint is IPEndPoint ipEndpoint)
            {
                logDir = Path.Join(logDir, ipEndpoint.Port.ToString());
                checkpointDir = Path.Join(checkpointDir, ipEndpoint.Port.ToString());
            }
            else if (endpoint is UnixDomainSocketEndPoint && !string.IsNullOrEmpty(unixSocketPath))
            {
                var socketFileName = Path.GetFileName(unixSocketPath);

                logDir = Path.Join(logDir, socketFileName);
                checkpointDir = Path.Join(checkpointDir, socketFileName);
            }
            else throw new NotSupportedException("Unsupported endpoint type.");

            if (!useAzureStorage)
            {
                logDir = Path.GetFullPath(logDir);
                checkpointDir = Path.GetFullPath(checkpointDir);
            }

            IAuthenticationSettings authenticationSettings = null;
            if (useAcl && aadAuthenticationSettings != null)
            {
                authenticationSettings = new AclAuthenticationAadSettings(aclFile, authPassword, aadAuthenticationSettings);
            }
            else if (useAcl)
            {
                authenticationSettings = new AclAuthenticationPasswordSettings(aclFile, authPassword);
            }
            else if (authPassword != null)
            {
                authenticationSettings = new PasswordAuthenticationSettings(authPassword);
            }

            GarnetServerOptions opts = new(logger)
            {
                ThreadPoolMinThreads = 100,
                SegmentSize = segmentSize,
                ObjectStoreSegmentSize = segmentSize,
                EnableStorageTier = useAzureStorage || (!disableStorageTier && logDir != null),
                LogDir = disableStorageTier ? null : logDir,
                CheckpointDir = checkpointDir,
                EndPoints = [endpoint],
                DisablePubSub = disablePubSub,
                DisableObjects = disableObjects,
                EnableDebugCommand = ConnectionProtectionOption.Yes,
                EnableModuleCommand = ConnectionProtectionOption.Yes,
                Recover = tryRecover,
                IndexSize = "1m",
                ObjectStoreIndexSize = "16k",
                EnableCluster = enableCluster,
                CleanClusterConfig = cleanClusterConfig,
                ClusterTimeout = timeout,
                QuietMode = true,
                EnableAOF = enableAOF,
                MemorySize = "1g",
                GossipDelay = gossipDelay,
                EnableFastCommit = fastCommit,
                MetricsSamplingFrequency = metricsSamplingFrequency,
                TlsOptions = useTLS ? new GarnetTlsOptions(
                    certFileName: certFile,
                    certPassword: certPassword,
                    clientCertificateRequired: true,
                    certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                    issuerCertificatePath: null,
                    certSubjectName: null,
                    certificateRefreshFrequency: 0,
                    enableCluster: true,
                    clientTargetHost: null,
                    serverCertificateRequired: true,
                    tlsServerOptionsOverride: null,
                    clusterTlsClientOptionsOverride: new SslClientAuthenticationOptions
                    {
                        ClientCertificates = certificates ?? [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = ValidateServerCertificate,
                    },
                    logger: logger)
                : null,
                DeviceFactoryCreator = useAzureStorage ?
                    logger == null ? TestUtils.AzureStorageNamedDeviceFactoryCreator : new AzureStorageNamedDeviceFactoryCreator(AzureEmulatedStorageString, logger)
                    : new LocalStorageNamedDeviceFactoryCreator(logger: logger),
                FastAofTruncate = fastAofTruncate,
                AofMemorySize = aofMemorySize,
                AofSizeLimit = aofSizeLimit,
                OnDemandCheckpoint = onDemandCheckpoint,
                CommitFrequencyMs = commitFrequencyMs,
                UseAofNullDevice = useAofNullDevice,
                EnableIncrementalSnapshots = enableIncrementalSnapshots,
                AuthSettings = useAcl ? authenticationSettings : (authPassword != null ? authenticationSettings : null),
                ClusterUsername = authUsername,
                ClusterPassword = authPassword,
                EnableLua = enableLua,
                LuaTransactionMode = luaTransactionMode,
                ReplicationOffsetMaxLag = asyncReplay ? -1 : 0,
                LuaOptions = enableLua ? new LuaOptions(luaMemoryMode, luaMemoryLimit, luaTimeout ?? Timeout.InfiniteTimeSpan, luaLoggingMode, luaAllowedFunctions ?? [], logger) : null,
                UnixSocketPath = unixSocketPath,
                ReplicaDisklessSync = enableDisklessSync,
                ReplicaDisklessSyncDelay = replicaDisklessSyncDelay,
                ReplicaDisklessSyncFullSyncAofThreshold = replicaDisklessSyncFullSyncAofThreshold,
                ClusterAnnounceEndpoint = clusterAnnounceEndpoint,
                ClusterAnnounceHostname = clusterAnnounceHostname,
                ClusterPreferredEndpointType = clusterPreferredEndpointType,
                DeviceType = deviceType,
                ClusterReplicationReestablishmentTimeout = clusterReplicationReestablishmentTimeout,
                CompactionFrequencySecs = compactionFrequencySecs,
                CompactionType = compactionType,
                LatencyMonitor = latencyMonitory,
                LoggingFrequency = loggingFrequencySecs,
                CheckpointThrottleFlushDelayMs = checkpointThrottleFlushDelayMs,
                ClusterReplicaResumeWithData = clusterReplicaResumeWithData,
                ReplicaSyncTimeout = replicaSyncTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(replicaSyncTimeout),
                EnableVectorSetPreview = enableVectorSetPreview,
                ExpiredObjectCollectionFrequencySecs = expiredObjectCollectionFrequencySecs,
            };

            if (lowMemory)
            {
                opts.MemorySize = opts.ObjectStoreLogMemorySize = memorySize == default ? "1024" : memorySize;
                opts.PageSize = opts.ObjectStorePageSize = pageSize == default ? "512" : pageSize;
            }

            return opts;
        }

        public static bool IsPortAvailable(int port)
        {
            bool inUse = true;

            IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            IPEndPoint[] ipEndPoints = ipProperties.GetActiveTcpListeners();

            foreach (IPEndPoint endPoint in ipEndPoints)
            {
                if (endPoint.Port == port)
                {
                    inUse = false;
                    break;
                }
            }

            return inUse;
        }

        /// <summary>
        /// Create config options for SE.Redis client
        /// </summary>
        public static ConfigurationOptions GetConfig(
            EndPointCollection endpoints = default,
            bool allowAdmin = false,
            bool disablePubSub = false,
            bool useTLS = false,
            string authUsername = null,
            string authPassword = null,
            X509CertificateCollection certificates = null,
            RedisProtocol? protocol = null)
        {
            var cmds = RespCommandsInfo.TryGetRespCommandNames(out var names)
                ? new HashSet<string>(names)
                : new HashSet<string>();

            if (disablePubSub)
            {
                cmds.Remove("SUBSCRIBE");
                cmds.Remove("PUBLISH");
            }

            var defaultEndPoints = endpoints == default ? [EndPoint] : endpoints;
            var configOptions = new ConfigurationOptions
            {
                EndPoints = defaultEndPoints,
                CommandMap = CommandMap.Create(cmds),
                ConnectTimeout = (int)TimeSpan.FromSeconds(Debugger.IsAttached ? 100 : 2).TotalMilliseconds,
                SyncTimeout = (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
                AsyncTimeout = (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
                AllowAdmin = allowAdmin,
                ReconnectRetryPolicy = new LinearRetry((int)TimeSpan.FromSeconds(10).TotalMilliseconds),
                ConnectRetry = 5,
                IncludeDetailInExceptions = true,
                AbortOnConnectFail = true,
                Password = authPassword,
                User = authUsername,
                ClientName = TestContext.CurrentContext.Test.MethodName,
                Protocol = protocol,
            };

            if (Debugger.IsAttached)
            {
                configOptions.SyncTimeout = (int)TimeSpan.FromHours(2).TotalMilliseconds;
                configOptions.AsyncTimeout = (int)TimeSpan.FromHours(2).TotalMilliseconds;
            }

            if (useTLS)
            {
                configOptions.Ssl = true;
                configOptions.SslHost = "GarnetTest";
                configOptions.SslClientAuthenticationOptions = (host) =>
                (
                    new SslClientAuthenticationOptions
                    {
                        ClientCertificates = certificates ?? [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = ValidateServerCertificate,
                    }
                );
            }
            return configOptions;
        }

        public static GarnetClient GetGarnetClient(EndPoint endpoint = null, bool useTLS = false, bool recordLatency = false, client.LightEpoch epoch = null)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new GarnetClient(endpoint ?? EndPoint, sslOptions, recordLatency: recordLatency, epoch: epoch);
        }

        public static GarnetClientSession GetGarnetClientSession(bool useTLS = false, bool raw = false, EndPoint endPoint = null)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new GarnetClientSession(endPoint ?? EndPoint, new(), tlsOptions: sslOptions, rawResult: raw);
        }

        public static LightClientRequest CreateRequest(LightClient.OnResponseDelegateUnsafe onReceive = null, bool useTLS = false, CountResponseType countResponseType = CountResponseType.Tokens)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new LightClientRequest(EndPoint, 0, onReceive, sslOptions, countResponseType);
        }

        public static string GetHostName(ILogger logger = null)
        {
            try
            {
                var serverName = Environment.MachineName; // host name sans domain
                var fqhn = Dns.GetHostEntry(serverName).HostName; // fully qualified hostname
                return fqhn;
            }
            catch (SocketException ex)
            {
                logger?.LogError(ex, "GetHostName threw an error");
            }

            return "";
        }

        public static EndPointCollection GetShardEndPoints(int shards, IPAddress address, int port)
        {
            EndPointCollection endPoints = [];
            for (var i = 0; i < shards; i++)
                endPoints.Add(address, port + i);
            return endPoints;
        }

        internal static string MethodTestDir => UnitTestWorkingDir();

        /// <summary>
        /// Find root test based on prefix Garnet.test
        /// </summary>
        internal static string RootTestsProjectPath =>
            TestContext.CurrentContext.TestDirectory.Split("Garnet.test")[0];

        /// <summary>
        /// Build path for unit test working directory using Guid
        /// </summary>
        /// <param name="category"></param>
        /// <param name="includeGuid"></param>
        /// <returns></returns>
        internal static string UnitTestWorkingDir(string category = null, bool includeGuid = false)
        {
            // Include process id to avoid conflicts between parallel test runs
            var testPath = $"{Environment.ProcessId}_{TestContext.CurrentContext.Test.ClassName}_{TestContext.CurrentContext.Test.MethodName}";
            var rootPath = Path.Combine(RootTestsProjectPath, ".tmp", testPath);

            if (category != null)
                rootPath = Path.Combine(rootPath, category);

            return includeGuid ? Path.Combine(rootPath, Guid.NewGuid().ToString()) : rootPath;
        }

        /// <summary>
        /// Delete a directory recursively
        /// </summary>
        /// <param name="path">The folder to delete</param>
        /// <param name="wait">If true, loop on exceptions that are retryable, and verify the directory no longer exists. Generally true on SetUp, false on TearDown</param>
        internal static void DeleteDirectory(string path, bool wait = false)
        {
            while (true)
            {
                try
                {
                    if (!Directory.Exists(path))
                        return;
                    foreach (string directory in Directory.GetDirectories(path))
                        DeleteDirectory(directory, wait);
                    break;
                }
                catch
                {
                }
            }

            bool retry = true;
            while (retry)
            {
                // Exceptions may happen due to a handle briefly remaining held after Dispose().
                retry = false;
                try
                {
                    if (Directory.Exists(path))
                        Directory.Delete(path, true);
                }
                catch (Exception ex) when (ex is IOException ||
                                           ex is UnauthorizedAccessException)
                {
                    if (!wait)
                    {
                        try { Directory.Delete(path, true); }
                        catch { }
                        return;
                    }
                    retry = true;
                }
            }
        }

        /// <summary>
        /// Delegate to use in TLS certificate validation
        /// Test certificate should be issued by "CN=Garnet"
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="certificate"></param>
        /// <param name="chain"></param>
        /// <param name="sslPolicyErrors"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static bool ValidateServerCertificate(
          object sender,
          X509Certificate certificate,
          X509Chain chain,
          SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors)
            {
                // Check chain elements
                foreach (var itemInChain in chain.ChainElements)
                {
                    if (itemInChain.Certificate.Issuer.Contains("CN=Garnet"))
                        return true;
                }
            }
            throw new Exception($"Certicate errors found {sslPolicyErrors}!");
        }

        public static void CreateTestLibrary(string[] namespaces, string[] referenceFiles, string[] filesToCompile, string dstFilePath)
        {
            if (File.Exists(dstFilePath))
            {
                File.Delete(dstFilePath);
            }

            foreach (var referenceFile in referenceFiles)
            {
                ClassicAssert.IsTrue(File.Exists(referenceFile), $"File '{Path.GetFullPath(referenceFile)}' does not exist.");
            }

            var references = referenceFiles.Select(f => MetadataReference.CreateFromFile(f));

            foreach (var fileToCompile in filesToCompile)
            {
                ClassicAssert.IsTrue(File.Exists(fileToCompile), $"File '{Path.GetFullPath(fileToCompile)}' does not exist.");
            }

            var explicitUsings = @"
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
";

            var parseFunc = new Func<string, SyntaxTree>(filePath =>
            {
                var source = $"{explicitUsings}{Environment.NewLine}{File.ReadAllText(filePath)}";
                var stringText = SourceText.From(source, Encoding.UTF8);
                return SyntaxFactory.ParseSyntaxTree(stringText,
                    CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Latest), string.Empty);
            });

            var syntaxTrees = filesToCompile.Select(f => parseFunc(f));

            var compilationOptions = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                .WithAllowUnsafe(true)
                .WithOverflowChecks(true)
                .WithOptimizationLevel(OptimizationLevel.Release)
                .WithUsings(namespaces);


            var compilation = CSharpCompilation.Create(Path.GetFileName(dstFilePath), syntaxTrees, references, compilationOptions);

            try
            {
                var result = compilation.Emit(dstFilePath);
                ClassicAssert.IsTrue(result.Success, string.Join(Environment.NewLine, result.Diagnostics.Select(d => d.ToString())));
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        public static StoreAddressInfo GetStoreAddressInfo(IServer server, bool includeReadCache = false, bool isObjectStore = false)
        {
            StoreAddressInfo result = default;
            var info = isObjectStore ? server.Info("OBJECTSTORE") : server.Info("STORE");
            foreach (var section in info)
            {
                foreach (var entry in section)
                {
                    if (entry.Key.Equals("Log.BeginAddress"))
                        result.BeginAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.HeadAddress"))
                        result.HeadAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.SafeReadOnlyAddress"))
                        result.ReadOnlyAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.TailAddress"))
                        result.TailAddress = long.Parse(entry.Value);
                    else if (entry.Key.Equals("Log.MemorySizeBytes"))
                        result.MemorySize = long.Parse(entry.Value);
                    else if (includeReadCache && entry.Key.Equals("ReadCache.BeginAddress"))
                        result.ReadCacheBeginAddress = long.Parse(entry.Value);
                    else if (includeReadCache && entry.Key.Equals("ReadCache.TailAddress"))
                        result.ReadCacheTailAddress = long.Parse(entry.Value);
                }
            }
            return result;
        }

        /// <summary>
        /// Get effective memory size based on configured memory size and page size.
        /// </summary>
        /// <param name="memorySize">Memory size string</param>
        /// <param name="pageSize">Page size string</param>
        /// <param name="parsedPageSize">Parsed page size</param>
        /// <returns>Effective memory size</returns>
        public static long GetEffectiveMemorySize(string memorySize, string pageSize, out long parsedPageSize)
        {
            parsedPageSize = ServerOptions.ParseSize(pageSize, out _);
            var parsedMemorySize = 1L << GarnetServerOptions.MemorySizeBits(memorySize, pageSize, out var epc);
            return parsedMemorySize - (epc * parsedPageSize);
        }

        /// <summary>
        /// Get a random alphanumeric string of specified length
        /// </summary>
        /// <param name="len">Length of string</param>
        /// <returns>Random alphanumeric string</returns>
        public static string GetRandomString(int len)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return RandomNumberGenerator.GetString(chars, len);
        }

        internal static void OnTearDown(bool waitForDelete = false, ILogger logger = null)
        {
            DeleteDirectory(MethodTestDir, wait: waitForDelete);
            var count = Tsavorite.core.LightEpoch.ActiveInstanceCount();
            if (count != 0)
            {
                // Reset all instances to avoid impacting other tests
                Tsavorite.core.LightEpoch.ResetAllInstances();
                logger?.LogError("Tsavorite.core.LightEpoch instances still active: {count}", count);
                Assert.Fail($"Tsavorite.core.LightEpoch instances still active: {count}");
            }

            var count2 = client.LightEpoch.ActiveInstanceCount();
            if (count2 != 0)
            {
                // Reset all instances to avoid impacting other tests
                client.LightEpoch.ResetAllInstances();
                logger?.LogError("Garnet.client.LightEpoch instances still active: {count2}", count2);
                Assert.Fail($"Garnet.client.LightEpoch instances still active: {count2}");
            }
        }
    }
}