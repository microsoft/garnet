// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IdentityModel.Tokens.Jwt;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using System.Text;
using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;
using Garnet.server.Auth.Aad;
using Garnet.server.Auth.Settings;
using Microsoft.IdentityModel.Tokens;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Base class for operations benchmarks
    /// </summary>
    public abstract unsafe class OperationsBase
    {
        /// <summary>
        /// Parameters
        /// </summary>
        [ParamsSource(nameof(OperationParamsProvider))]
        public OperationParams Params { get; set; }

        /// <summary>
        /// Operation parameters provider
        /// </summary>
        public IEnumerable<OperationParams> OperationParamsProvider()
        {
            if (ParamsNone)
                yield return new(false, false);
            if (ParamsACL)
                yield return new(true, false);
            if (ParamsAOF)
                yield return new(false, true);
            if (ParamsAAD)
                yield return new(false, false, useAad: true);
        }

        /// <summary>
        /// Clear and set all params that are set by cmdline arg --opparams
        /// </summary>
        public static void SetAllParams(bool isEnabled)
        {
            ParamsNone = isEnabled;
            ParamsACL = isEnabled;
            ParamsAOF = isEnabled;
            ParamsAAD = isEnabled;
        }

        /// <summary>
        /// Set by cmdline arg --opparams
        /// </summary>
        internal static bool ParamsNone = true;
        internal static bool ParamsACL = true;
        internal static bool ParamsAOF = true;
        internal static bool ParamsAAD = true;

        /// <summary>
        /// Batch size per method invocation
        /// With a batchSize of 100, we have a convenient conversion of latency to throughput:
        ///   5 us = 20 Mops/sec
        ///  10 us = 10 Mops/sec
        ///  20 us =  5 Mops/sec
        ///  25 us =  4 Mops/sec
        /// 100 us =  1 Mops/sec
        /// </summary>
        internal const int batchSize = 100;
        internal EmbeddedRespServer server;
        internal RespServerSession session;
        internal RespServerSession subscribeSession;

        // Pre-built AUTH RESP command sent once after the session is created on AAD-enabled
        // runs — null otherwise. Authenticating once leaves the session in the same state
        // a production AAD-authenticated client would be in by the time benchmark iterations
        // start, so the hot path includes the IsAuthenticated check per command.
        private byte[] aadAuthCommand;

        /// <summary>
        /// Setup
        /// </summary>
        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true,
                EnableLua = true,
                DisablePubSub = true,
                LuaOptions = new(LuaMemoryManagementMode.Native, "", Timeout.InfiniteTimeSpan, LuaLoggingMode.Enable, []),
            };

            // Allow derived benchmarks to customize the server configuration (e.g. a larger-than-memory store
            // backed by a device). Called before the AOF/auth options below so those still take effect.
            ConfigureServerOptions(opts);

            if (opts.DeviceType != Tsavorite.core.DeviceType.Default)
            {
                // Nothing to create here: the device is built downstream by GarnetServerOptions.GetSettings()
                // (called from GarnetServer.CreateStore). Its EnableStorageTier branch calls GetInitializedDeviceFactory()
                // -> LocalStorageNamedDeviceFactory.Get() -> Devices.CreateLogDevice(deviceType: LocalMemory,
                // numCompletionThreads: opts.DeviceCompletionThreads), which builds a LocalMemoryDevice with latencyUs:0
                // (a pure in-memory device, so we measure the Tsavorite pending codepaths rather than real device IO),
                // and assigns it to kvSettings.LogDevice before TsavoriteKV is constructed. A derived benchmark that sets
                // DeviceCompletionThreads = 0 selects parallelism:0 (inline completion: copy + callback on the submitting
                // thread, no completion thread or ring handoff) — see LTM.RawStringOperations, which uses that to avoid the
                // cross-thread/cross-socket handoff variance without any process/thread pinning.
                //
                // The only requirement on our side is that the precondition for that branch holds, so fail loudly on a
                // misconfiguration that would otherwise be silently downgraded to a NullDevice (the non-tiered fallback).
                if (opts.DeviceType != Tsavorite.core.DeviceType.LocalMemory)
                    throw new InvalidOperationException($"Operations benchmarks only support the fast in-memory LocalMemoryDevice for the IO path, not {opts.DeviceType}.");
                if (!opts.EnableStorageTier)
                    throw new InvalidOperationException("A non-default DeviceType requires EnableStorageTier=true; otherwise GetSettings() falls back to a NullDevice and the device is never used.");
            }

            if (Params.useAof)
            {
                opts.EnableAOF = true;
                opts.UseAofNullDevice = true;   // TODO: Should this change to a LocalMemoryDevice as well (perhaps optionally) to test the write impact against that?
                opts.FastAofTruncate = true;
                opts.CommitFrequencyMs = -1;
                opts.AofPageSize = "128m";
                opts.AofMemorySize = "256m";
            }

            string aclFile = null;
            try
            {
                if (Params.useACLs)
                {
                    aclFile = Path.GetTempFileName();
                    File.WriteAllText(aclFile, @"user default on nopass -@all +ping +set +get +setex +incr +decr +incrby +decrby +zadd +zrem +lpush +lpop +sadd +srem +hset +hdel +publish +subscribe +@custom");
                    opts.AuthSettings = new AclAuthenticationPasswordSettings(aclFile);
                }
                else if (Params.useAad)
                {
                    opts.AuthSettings = BuildAadAuthSettings(out aadAuthCommand);
                }

                server = new EmbeddedRespServer(opts, null, new GarnetServerEmbedded());
                session = server.GetRespSession();

                if (aadAuthCommand is not null)
                {
                    SlowConsumeMessage(aadAuthCommand);
                }
            }
            finally
            {
                if (aclFile != null)
                    File.Delete(aclFile);
            }
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            session.Dispose();
            subscribeSession?.Dispose();
            server.Dispose();
        }

        /// <summary>
        /// Hook for derived benchmarks to customize the <see cref="GarnetServerOptions"/> before the embedded server is
        /// created (e.g. to configure a larger-than-memory store backed by a device). Default is a no-op.
        /// </summary>
        protected virtual void ConfigureServerOptions(GarnetServerOptions opts) { }

        // Builds an AAD auth settings + an AUTH RESP command carrying a freshly-minted,
        // in-process-signed JWT valid for 12 hours. Self-contained: no external IdP / no
        // network IO.
        private static AadAuthenticationSettings BuildAadAuthSettings(out byte[] authCommand)
        {
            const string issuer = "https://bdn.benchmark.local/";
            const string audience = "bdn-bench-audience";
            const string appId = "bdn-bench-app-id";

            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("BDN.benchmark AAD signing key — used only for in-process JWT validation."));
            var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
            var claims = new[] { new Claim("appidacr", "1"), new Claim("appid", appId) };
            var jwt = new JwtSecurityTokenHandler().WriteToken(new JwtSecurityToken(issuer, audience, claims, expires: DateTime.UtcNow.AddHours(12), signingCredentials: creds));

            var settings = new AadAuthenticationSettings([appId], [audience], [issuer], new BdnIssuerSigningTokenProvider([key]), validateUsername: false);

            // RESP: AUTH default <jwt>
            authCommand = Encoding.UTF8.GetBytes($"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n${jwt.Length}\r\n{jwt}\r\n");
            return settings;
        }

        // Stub IssuerSigningTokenProvider that returns the pre-baked signing keys without
        // attempting OpenID metadata discovery (authority="" disables the refresh timer).
        private sealed class BdnIssuerSigningTokenProvider : IssuerSigningTokenProvider
        {
            public BdnIssuerSigningTokenProvider(IReadOnlyCollection<SecurityKey> signingTokens)
                : base(string.Empty, signingTokens, refreshTokens: false, logger: null) { }
        }

        protected void Send(Request request)
        {
            _ = session.TryConsumeMessages(request.bufferPtr, request.buffer.Length);
        }

        protected unsafe void SetupOperation(ref Request request, ReadOnlySpan<byte> operation, int batchSize = batchSize)
        {
            request.buffer = GC.AllocateArray<byte>(operation.Length * batchSize, pinned: true);
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
            for (int i = 0; i < batchSize; i++)
                operation.CopyTo(new Span<byte>(request.buffer).Slice(i * operation.Length));
        }

        protected unsafe void SetupOperation(ref Request request, string operation, int batchSize = batchSize)
        {
            request.buffer = GC.AllocateUninitializedArray<byte>(operation.Length * batchSize, pinned: true);
            for (var i = 0; i < batchSize; i++)
            {
                var start = i * operation.Length;
                Encoding.UTF8.GetBytes(operation, request.buffer.AsSpan().Slice(start, operation.Length));
            }
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
        }

        protected unsafe void SetupOperation(ref Request request, List<byte> operationBytes)
        {
            request.buffer = GC.AllocateUninitializedArray<byte>(operationBytes.Count, pinned: true);
            operationBytes.CopyTo(request.buffer);
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
        }

        protected void SlowConsumeMessage(ReadOnlySpan<byte> message)
        {
            Request request = default;
            SetupOperation(ref request, message, 1);
            Send(request);
        }
    }
}