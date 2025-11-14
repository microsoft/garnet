using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    internal class GarnetServerTestProcess : IDisposable
    {
        private Process process;
        private readonly LightClientRequest lightClientRequest;

        public ConfigurationOptions Options { get; }

        internal GarnetServerTestProcess(Dictionary<string, string> env, int port = 7000)
        {
            var a = Assembly.GetAssembly(typeof(Program));
            var name = a.Location;
            var pos = name.LastIndexOf('.');

            using var cts = new CancellationTokenSource();

            if (Debugger.IsAttached)
            {
                // If debugging, give us a bit longer before timeouts start happening
                cts.CancelAfter(300_000);
            }
            else
            {
                cts.CancelAfter(30_000);
            }

            while (!TestUtils.IsPortAvailable(port))
            {
                if (cts.IsCancellationRequested)
                {
                    throw new GarnetException($"Port {port} is not available, and did not become available before timeout");
                }

                // Wait for port to be available
                Thread.Sleep(1_000);
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                name = string.Concat(name.AsSpan(0, pos), ".exe");
            }
            else
            {
                name = name.Substring(0, pos);
            }

            var endPoint = new IPEndPoint(IPAddress.Loopback, port);
            Options = TestUtils.GetConfig([endPoint]);

            // We don't have to disable objects, it's done to improve startup time a bit.
            var psi = new ProcessStartInfo(name, ["--bind", "127.0.0.1", "--port", port.ToString(), "--enable-debug-command", "local", "--no-pubsub", "--no-obj"])
            {
                CreateNoWindow = true,
                RedirectStandardInput = true,
                RedirectStandardOutput = true
            };

            if (env != null)
            {
                foreach (var e in env)
                    psi.Environment.Add(e.Key, e.Value);
            }

            process = Process.Start(psi);
            ClassicAssert.NotNull(process);

            // Block until the startup message to ensure process is up.
            while (true)
            {
                var line = process.StandardOutput.ReadLineAsync(cts.Token).AsTask().GetAwaiter().GetResult();

                if (line == null)
                {
                    throw new GarnetException("StandardOutput completed before GarnetServer was ready for connections");
                }
                else if (line.Equals("* Ready to accept connections", StringComparison.Ordinal))
                {
                    break;
                }
            }

            lightClientRequest = new LightClientRequest(endPoint, 0);
        }

        ~GarnetServerTestProcess()
        {
            // If somehow we leak this, still try and kill the process
            KillProcess(process, lightClientRequest);
            process = null;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);

            KillProcess(process, lightClientRequest);
            process = null;
        }

        private static void KillProcess(Process process, LightClientRequest lightClientRequest)
        {
            if (process != null)
            {
                try
                {
                    if (process.HasExited)
                    {
                        return;
                    }

                    using (process)
                    {
                        // We want to be sure the process is down, otherwise it may conflict
                        // with a future run. First, we'll ask nicely and then kill it.
                        using (lightClientRequest)
                        {
                            try
                            {
                                // More reliable than QUIT.
                                _ = lightClientRequest.SendCommand("DEBUG PANIC");
                            }
                            catch { }
                        }

                        try
                        {
                            process.Kill();
                        }
                        catch { }

                        process.Close();
                    }
                }
                catch { }
            }
        }
    }
}