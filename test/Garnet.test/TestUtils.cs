// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Security;
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

    internal static class TestUtils
    {
        /// <summary>
        /// Address
        /// </summary>
        public static string Address = "127.0.0.1";

        /// <summary>
        /// Port
        /// </summary>
        public static int Port = 33278;

        /// <summary>
        /// Whether to use a test progress logger
        /// </summary>
        static readonly bool useTestLogger = false;

        private static int procId = Process.GetCurrentProcess().Id;
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
            IPAddress expectedIp = new([127, 0, 0, 1]);
            var expectedPorts = new[] { 10000, 10001 };

            var activeTcpListeners = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners();

            var relevantListeners = activeTcpListeners.Where(t =>
                    expectedPorts.Contains(t.Port) &&
                    t.Address.Equals(expectedIp))
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
            bool disablePubSub = false,
            bool tryRecover = false,
            bool lowMemory = false,
            string MemorySize = default,
            string PageSize = default,
            bool enableAOF = false,
            bool EnableTLS = false,
            bool DisableObjects = false,
            int metricsSamplingFreq = -1,
            bool latencyMonitor = false,
            int commitFrequencyMs = 0,
            bool commitWait = false,
            bool UseAzureStorage = false,
            string defaultPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
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
            bool enableLua = false,
            bool enableReadCache = false,
            bool enableObjectStoreReadCache = false,
            ILogger logger = null,
            IEnumerable<string> loadModulePaths = null,
            string pubSubPageSize = null,
            bool asyncReplay = false)
        {
            if (UseAzureStorage)
                IgnoreIfNotRunningAzureTests();
            var _LogDir = logCheckpointDir;
            if (UseAzureStorage)
                _LogDir = $"{AzureTestContainer}/{AzureTestDirectory}";

            if (logCheckpointDir != null && !UseAzureStorage) _LogDir = new DirectoryInfo(string.IsNullOrEmpty(_LogDir) ? "." : _LogDir).FullName;

            var _CheckpointDir = logCheckpointDir;
            if (UseAzureStorage)
                _CheckpointDir = $"{AzureTestContainer}/{AzureTestDirectory}";

            if (logCheckpointDir != null && !UseAzureStorage) _CheckpointDir = new DirectoryInfo(string.IsNullOrEmpty(_CheckpointDir) ? "." : _CheckpointDir).FullName;

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
                EnableStorageTier = logCheckpointDir != null,
                LogDir = _LogDir,
                CheckpointDir = _CheckpointDir,
                Address = Address,
                Port = Port,
                DisablePubSub = disablePubSub,
                Recover = tryRecover,
                IndexSize = indexSize,
                ObjectStoreIndexSize = objectStoreIndexSize,
                EnableAOF = enableAOF,
                EnableLua = enableLua,
                CommitFrequencyMs = commitFrequencyMs,
                WaitForCommit = commitWait,
                TlsOptions = EnableTLS ? new GarnetTlsOptions(
                    certFileName: certFile,
                    certPassword: certPassword,
                    clientCertificateRequired: true,
                    certificateRevocationCheckMode: X509RevocationMode.NoCheck,
                    issuerCertificatePath: null,
                    null, 0, false, null, logger: logger)
                : null,
                DisableObjects = DisableObjects,
                QuietMode = true,
                MetricsSamplingFrequency = metricsSamplingFreq,
                LatencyMonitor = latencyMonitor,
                DeviceFactoryCreator = UseAzureStorage ?
                      () => new AzureStorageNamedDeviceFactory(AzureEmulatedStorageString, logger)
                    : () => new LocalStorageNamedDeviceFactory(logger: logger),
                AuthSettings = authenticationSettings,
                ExtensionBinPaths = extensionBinPaths,
                ExtensionAllowUnsignedAssemblies = extensionAllowUnsignedAssemblies,
                EnableScatterGatherGet = getSG,
                IndexResizeFrequencySecs = indexResizeFrequencySecs,
                ThreadPoolMinThreads = threadPoolMinThreads,
                LoadModuleCS = loadModulePaths,
                EnableReadCache = enableReadCache,
                EnableObjectStoreReadCache = enableObjectStoreReadCache,
                ReplicationOffsetMaxLag = asyncReplay ? -1 : 0
            };

            if (!string.IsNullOrEmpty(pubSubPageSize))
                opts.PubSubPageSize = pubSubPageSize;

            if (!string.IsNullOrEmpty(objectStoreHeapMemorySize))
                opts.ObjectStoreHeapMemorySize = objectStoreHeapMemorySize;

            if (!string.IsNullOrEmpty(objectStoreReadCacheHeapMemorySize))
                opts.ObjectStoreReadCacheHeapMemorySize = objectStoreReadCacheHeapMemorySize;

            if (indexMaxSize != default) opts.IndexMaxSize = indexMaxSize;
            if (objectStoreIndexMaxSize != default) opts.ObjectStoreIndexMaxSize = objectStoreIndexMaxSize;

            if (lowMemory)
            {
                opts.MemorySize = opts.ObjectStoreLogMemorySize = MemorySize == default ? "1024" : MemorySize;
                opts.PageSize = opts.ObjectStorePageSize = PageSize == default ? "512" : PageSize;
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

            if (useTestLogger)
            {
                var loggerFactory = LoggerFactory.Create(builder =>
                {
                    builder.AddProvider(new NUnitLoggerProvider(TestContext.Progress, TestContext.CurrentContext.Test.MethodName, null, false, false, LogLevel.Trace));
                    builder.SetMinimumLevel(LogLevel.Trace);
                });

                return new GarnetServer(opts, loggerFactory);
            }
            else
            {
                return new GarnetServer(opts);
            }
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

        public static GarnetServer[] CreateGarnetCluster(
            string checkpointDir,
            EndPointCollection endpoints,
            bool disablePubSub = false,
            bool disableObjects = false,
            bool disableEpochCollision = false,
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
            bool MainMemoryReplication = false,
            string AofMemorySize = "64m",
            bool OnDemandCheckpoint = false,
            int CommitFrequencyMs = 0,
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
            bool asyncReplay = false)
        {
            if (UseAzureStorage)
                IgnoreIfNotRunningAzureTests();
            GarnetServer[] nodes = new GarnetServer[endpoints.Count];
            for (int i = 0; i < nodes.Length; i++)
            {
                IPEndPoint endpoint = (IPEndPoint)endpoints[i];

                var opts = GetGarnetServerOptions(
                    checkpointDir,
                    checkpointDir,
                    endpoint.Port,
                    disablePubSub,
                    disableObjects,
                    disableEpochCollision,
                    tryRecover,
                    enableAOF,
                    timeout,
                    gossipDelay,
                    UseAzureStorage,
                    UseTLS: UseTLS,
                    cleanClusterConfig: cleanClusterConfig,
                    lowMemory: lowMemory,
                    MemorySize: MemorySize,
                    PageSize: PageSize,
                    SegmentSize: SegmentSize,
                    MainMemoryReplication: MainMemoryReplication,
                    AofMemorySize: AofMemorySize,
                    OnDemandCheckpoint: OnDemandCheckpoint,
                    CommitFrequencyMs: CommitFrequencyMs,
                    DisableStorageTier: DisableStorageTier,
                    EnableIncrementalSnapshots: EnableIncrementalSnapshots,
                    FastCommit: FastCommit,
                    authUsername: authUsername,
                    authPassword: authPassword,
                    useAcl: useAcl,
                    aclFile: aclFile,
                    certificates: certificates,
                    logger: loggerFactory?.CreateLogger("GarnetServer"),
                    aadAuthenticationSettings: authenticationSettings,
                    metricsSamplingFrequency: metricsSamplingFrequency,
                    enableLua: enableLua,
                    asyncReplay: asyncReplay);

                ClassicAssert.IsNotNull(opts);
                int iter = 0;
                while (!IsPortAvailable(opts.Port))
                {
                    ClassicAssert.Less(30, iter, "Failed to connect within 30 seconds");
                    TestContext.Progress.WriteLine($"Waiting for Port {opts.Port} to become available for {TestContext.CurrentContext.WorkerId}:{iter++}");
                    Thread.Sleep(1000);
                }
                nodes[i] = new GarnetServer(opts, loggerFactory);
            }
            return nodes;
        }

        public static GarnetServerOptions GetGarnetServerOptions(
            string checkpointDir,
            string logDir,
            int Port,
            bool disablePubSub = false,
            bool disableObjects = false,
            bool disableEpochCollision = false,
            bool tryRecover = false,
            bool enableAOF = false,
            int timeout = -1,
            int gossipDelay = 5,
            bool UseAzureStorage = false,
            bool UseTLS = false,
            bool cleanClusterConfig = false,
            bool lowMemory = false,
            string MemorySize = default,
            string PageSize = default,
            string SegmentSize = "1g",
            bool MainMemoryReplication = false,
            string AofMemorySize = "64m",
            bool OnDemandCheckpoint = false,
            int CommitFrequencyMs = 0,
            bool DisableStorageTier = false,
            bool EnableIncrementalSnapshots = false,
            bool FastCommit = true,
            string authUsername = null,
            string authPassword = null,
            bool useAcl = false, // NOTE: Temporary until ACL is enforced as default
            string aclFile = null,
            X509CertificateCollection certificates = null,
            AadAuthenticationSettings aadAuthenticationSettings = null,
            int metricsSamplingFrequency = 0,
            bool enableLua = false,
            bool asyncReplay = false,
            ILogger logger = null)
        {
            if (UseAzureStorage)
                IgnoreIfNotRunningAzureTests();
            var _LogDir = logDir + $"/{Port}";
            if (UseAzureStorage)
                _LogDir = $"{AzureTestContainer}/{AzureTestDirectory}/{Port}";
            if (logDir != null && !UseAzureStorage) _LogDir = new DirectoryInfo(string.IsNullOrEmpty(_LogDir) ? "." : _LogDir).FullName;

            var _CheckpointDir = checkpointDir + $"/{Port}";
            if (UseAzureStorage)
                _CheckpointDir = $"{AzureTestContainer}/{AzureTestDirectory}/{Port}";
            if (!UseAzureStorage) _CheckpointDir = new DirectoryInfo(string.IsNullOrEmpty(_CheckpointDir) ? "." : _CheckpointDir).FullName;

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
                SegmentSize = SegmentSize,
                ObjectStoreSegmentSize = SegmentSize,
                EnableStorageTier = UseAzureStorage ? true : DisableStorageTier ? false : logDir != null,
                LogDir = DisableStorageTier ? null : _LogDir,
                CheckpointDir = _CheckpointDir,
                Address = Address,
                Port = Port,
                DisablePubSub = disablePubSub,
                DisableObjects = disableObjects,
                DisableEpochCollision = disableEpochCollision,
                Recover = tryRecover,
                IndexSize = "1m",
                ObjectStoreIndexSize = "16k",
                EnableCluster = true,
                CleanClusterConfig = cleanClusterConfig,
                ClusterTimeout = timeout,
                QuietMode = true,
                EnableAOF = enableAOF,
                MemorySize = "1g",
                GossipDelay = gossipDelay,
                EnableFastCommit = FastCommit,
                MetricsSamplingFrequency = metricsSamplingFrequency,
                TlsOptions = UseTLS ? new GarnetTlsOptions(
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
                        ClientCertificates = certificates ?? [new X509Certificate2(certFile, certPassword)],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = ValidateServerCertificate,
                    },
                    logger: logger)
                : null,
                DeviceFactoryCreator = UseAzureStorage ?
                    () => new AzureStorageNamedDeviceFactory(AzureEmulatedStorageString, logger)
                    : () => new LocalStorageNamedDeviceFactory(logger: logger),
                MainMemoryReplication = MainMemoryReplication,
                AofMemorySize = AofMemorySize,
                OnDemandCheckpoint = OnDemandCheckpoint,
                CommitFrequencyMs = CommitFrequencyMs,
                EnableIncrementalSnapshots = EnableIncrementalSnapshots,
                AuthSettings = useAcl ? authenticationSettings : (authPassword != null ? authenticationSettings : null),
                ClusterUsername = authUsername,
                ClusterPassword = authPassword,
                EnableLua = enableLua,
                ReplicationOffsetMaxLag = asyncReplay ? -1 : 0
            };

            if (lowMemory)
            {
                opts.MemorySize = opts.ObjectStoreLogMemorySize = MemorySize == default ? "1024" : MemorySize;
                opts.PageSize = opts.ObjectStorePageSize = PageSize == default ? "512" : PageSize;
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
            int port = default,
            bool allowAdmin = false,
            bool disablePubSub = false,
            bool useTLS = false,
            string authUsername = null,
            string authPassword = null,
            X509CertificateCollection certificates = null)
        {
            var cmds = RespCommandsInfo.TryGetRespCommandNames(out var names)
                ? new HashSet<string>(names)
                : new HashSet<string>();

            if (disablePubSub)
            {
                cmds.Remove("SUBSCRIBE");
                cmds.Remove("PUBLISH");
            }

            EndPointCollection defaultEndPoints = endpoints == default ? new() { { Address, port == default ? Port : port }, } : endpoints;
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
                        ClientCertificates = certificates ?? [new X509Certificate2(certFile, certPassword)],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = ValidateServerCertificate,
                    }
                );
            }
            return configOptions;
        }

        public static GarnetClient GetGarnetClient(bool useTLS = false, bool recordLatency = false)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [new X509Certificate2(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new GarnetClient(Address, Port, sslOptions, recordLatency: recordLatency);
        }

        public static GarnetClientSession GetGarnetClientSession(bool useTLS = false, bool recordLatency = false)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [new X509Certificate2(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new GarnetClientSession(Address, Port, new(), tlsOptions: sslOptions);
        }

        public static LightClientRequest CreateRequest(LightClient.OnResponseDelegateUnsafe onReceive = null, bool useTLS = false, CountResponseType countResponseType = CountResponseType.Tokens)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [new X509Certificate2(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = ValidateServerCertificate,
                };
            }
            return new LightClientRequest(Address, Port, 0, onReceive, sslOptions, countResponseType);
        }

        public static EndPointCollection GetEndPoints(int shards, int port = default)
        {
            Port = port == default ? Port : port;
            EndPointCollection endPoints = [];
            for (int i = 0; i < shards; i++)
                endPoints.Add(IPAddress.Parse("127.0.0.1"), Port + i);
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

            var parseFunc = new Func<string, SyntaxTree>(filePath =>
            {
                var source = File.ReadAllText(filePath);
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
    }
}