using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    internal class TestProcess : IDisposable
    {
        private Process p = default;
        private Stopwatch stopWatch = default;

        internal TestProcess(Dictionary<string, string> env,
                             out ConfigurationOptions opts,
                             int port = 7000)
        {
            var a = Assembly.GetAssembly(typeof(server.Program));
            var name = a.Location;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                name = name.Replace(".dll", ".exe");
            }
            else
            {
                name = name.Replace(".dll", "");
            }

            opts = TestUtils.GetConfig([new IPEndPoint(IPAddress.Loopback, port)]);

            var psi = new ProcessStartInfo(name)
            {
                CreateNoWindow = true,
                RedirectStandardInput = true,
                RedirectStandardOutput = true
            };
            psi.Environment.Add("GARNET_TEST_PORT", port.ToString());
            foreach (var e in env)
                psi.Environment.Add(e.Key, e.Value);

            p = Process.Start(psi);
            ClassicAssert.NotNull(p);

            // Block until the startup message to ensure process is up.
            var dummy = new char[1];
            _ = p.StandardOutput.ReadBlock(dummy, 0, 1);

            stopWatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            if (stopWatch != default)
            {
                stopWatch.Stop();
                Console.WriteLine(stopWatch.ElapsedMilliseconds);
            }

            if (p != default)
            {
                try { p.Kill(); }
                catch { }

                p.Close();
            }
        }
    }
}