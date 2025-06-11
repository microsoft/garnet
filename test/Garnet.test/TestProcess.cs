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
        private readonly Process process = default;
        private readonly Stopwatch stopWatch = default;
        private readonly LightClientRequest lightClientRequest = default;

        public ConfigurationOptions Options { get; }

        internal GarnetServerTestProcess(Dictionary<string, string> env = default, int port = 7000)
        {
            var a = Assembly.GetAssembly(typeof(Garnet.Program));
            var name = a.Location;
            var pos = name.LastIndexOf('.');

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

            if (env != default)
            {
                foreach (var e in env)
                    psi.Environment.Add(e.Key, e.Value);
            }

            process = Process.Start(psi);
            ClassicAssert.NotNull(process);

            // Block until the startup message to ensure process is up.
            var dummy = new char[1];
            _ = process.StandardOutput.ReadBlock(dummy, 0, 1);

            // Give it a bit more time
            Thread.Sleep(100);
            lightClientRequest = new LightClientRequest(endPoint, 0);

            stopWatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            if (stopWatch != default)
            {
                stopWatch.Stop();
                Console.WriteLine(stopWatch.ElapsedMilliseconds);
            }

            if (process != default)
            {
                // We want to be sure the process is down, otherwise it may conflict
                // with a future run. First, we'll ask nicely and then kill it.
                try
                {
                    // More reliable than QUIT.
                    _ = lightClientRequest.SendCommand("DEBUG PANIC");
                    lightClientRequest.Dispose();
                }
                catch { }

                try { process.Kill(); }
                catch { }

                process.Close();
            }
        }
    }
}