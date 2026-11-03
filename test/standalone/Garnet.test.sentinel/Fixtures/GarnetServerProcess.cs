// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;

namespace Garnet.test.sentinel.Fixtures
{
    /// <summary>
    /// Spawns the GarnetServer executable in a subprocess, listens on a chosen TCP port,
    /// and waits for the standard "* Ready to accept connections" banner.
    ///
    /// The binary path is discovered by reflecting on the main/GarnetServer assembly
    /// (same trick as Garnet.test/TestProcess.cs:14-18). This relies on the test project
    /// referencing main/GarnetServer, which copies the executable into the test bin output.
    /// </summary>
    internal sealed class GarnetServerProcess : ProcessWrapper
    {
        private readonly string[] _extraArgs;
        private readonly bool _enableCluster;

        public GarnetServerProcess(int port, bool enableCluster = false, params string[] extraArgs)
            : base(port)
        {
            _enableCluster = enableCluster;
            _extraArgs = extraArgs;
        }

        protected override string ExecutablePath
        {
            get
            {
                // The GarnetServer project's Program.cs type is the simplest anchor:
                // its assembly location points at the deployed binary.
                var asmPath = typeof(global::Garnet.Program).Assembly.Location;
                var dir = Path.GetDirectoryName(asmPath) ?? ".";
                var name = Path.GetFileNameWithoutExtension(asmPath);
                // Windows build emits GarnetServer.exe; the rest emit the dll with
                // an apphost of the same name. The standard convention in this repo
                // is to invoke the .dll via dotnet, but on Linux the apphost is
                // generated automatically when PublishAot/UseAppHost is set.
                // For our purposes, locate either GarnetServer (apphost) or
                // GarnetServer.dll.
                var apphost = Path.Combine(dir, OperatingSystem.IsWindows() ? $"{name}.exe" : name);
                if (File.Exists(apphost)) return apphost;
                var dll = Path.Combine(dir, $"{name}.dll");
                if (File.Exists(dll)) return dll;
                throw new FileNotFoundException(
                    $"Could not locate GarnetServer binary near {asmPath}. " +
                    "Ensure the test project references main/GarnetServer.");
            }
        }

        protected override IReadOnlyList<string> BuildArguments()
        {
            var args = new List<string>
            {
                "--bind", "127.0.0.1",
                "--port", Port.ToString(),
                "--no-pubsub",         // smaller surface for the PoC
                "--no-obj",            // skip object-store, faster startup
                "--enable-debug-command", "local",
            };
            if (_enableCluster) args.Add("--cluster-enabled");
            if (_extraArgs != null) args.AddRange(_extraArgs);
            return args;
        }

        protected override string ReadyLineMarker => "Ready to accept connections";

        // DEBUG PANIC is what the in-repo fixture uses (TestProcess.cs:160).
        protected override string? PoliteShutdownCommand => "DEBUG PANIC";
    }
}
