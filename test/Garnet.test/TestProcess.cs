using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    internal class GarnetServerTestProcess : IDisposable
    {
        private Process process;
        private readonly LightClientRequest lightClientRequest;

        public ConfigurationOptions Options { get; }

        public StringBuilder OutputLog { get; }

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
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };

            if (env != null)
            {
                foreach (var e in env)
                    psi.Environment.Add(e.Key, e.Value);
            }

            process = Process.Start(psi);
            ClassicAssert.NotNull(process);

            var released = false;
            using var waitFor = new SemaphoreSlim(0, 1);

            OutputLog = new();
            _ = OutputLog.AppendLine($"Started PID: {process.Id}");
            foreach (var arg in psi.ArgumentList)
            {
                _ = OutputLog.AppendLine($"Arg: {arg}");
            }

            foreach (var (k, v) in psi.Environment)
            {
                _ = OutputLog.AppendLine($"Env: {k}={v}");
            }

            process.OutputDataReceived +=
                (obj, lineArgs) =>
                {
                    if (lineArgs.Data == null)
                    {
                        return;
                    }

                    lock (OutputLog)
                    {
                        _ = OutputLog.AppendLine($"[>OUT] {lineArgs.Data}");
                    }

                    if (!released && lineArgs.Data.Equals("* Ready to accept connections", StringComparison.Ordinal))
                    {
                        _ = waitFor.Release();
                        released = true;
                    }
                };
            process.ErrorDataReceived +=
                (obj, lineArgs) =>
                {
                    if (lineArgs.Data == null)
                    {
                        return;
                    }

                    lock (OutputLog)
                    {
                        _ = OutputLog.AppendLine($"[!ERR] {lineArgs.Data}");
                    }
                };
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            // Block until the startup message to ensure process is up.
            try
            {
                waitFor.Wait(cts.Token);
            }
            catch
            {
                RecordTestOutput(OutputLog);

                throw;
            }

            lightClientRequest = new LightClientRequest(endPoint, 0);
        }

        ~GarnetServerTestProcess()
        {
            // If somehow we leak this, still try and kill the process
            KillProcess(process, null, lightClientRequest);
            process = null;
        }

        private static void RecordTestOutput(StringBuilder outputLog)
        {
            lock (outputLog)
            {
                TestContext.Write(outputLog.ToString());
            }
        }

        public void RecordTestOutput()
        => RecordTestOutput(OutputLog);

        public void Dispose()
        {
            GC.SuppressFinalize(this);

            KillProcess(process, OutputLog, lightClientRequest);
            process = null;
        }

        private static void KillProcess(Process process, StringBuilder outputLog, LightClientRequest lightClientRequest)
        {
            if (process != null)
            {
                try
                {
                    using (process)
                    {
                        if (process.HasExited)
                        {
                            return;
                        }

                        // We want to be sure the process is down, otherwise it may conflict
                        // with a future run. First, we'll ask nicely and then kill it.
                        try
                        {
                            using (lightClientRequest)
                            {
                                // More reliable than QUIT.
                                _ = lightClientRequest.SendCommand("DEBUG PANIC");

                            }
                        }
                        catch
                        {
                            // LightClientRequest might already be disposed or collected, ignore any errors
                        }

                        try
                        {
                            process.Kill();
                        }
                        catch
                        {
                            // Process might be disposed or collected, ignore any errors
                        }

                        if (!process.WaitForExit(10_000))
                        {
                            if (outputLog != null)
                            {
                                // If we have an output log, we're not on the finalizer thread - so try and write results to test log

                                lock (outputLog)
                                {
                                    _ = outputLog.AppendLine("!!Process did not exit when expected!!");
                                }

                                RecordTestOutput(outputLog);
                            }
                            else
                            {
                                // No output log implies bad things are happening, so just go straight to standard out

                                Console.WriteLine($"PID: {process.Id} did not exit when expected");
                            }
                        }

                        process.Close();
                    }
                }
                catch
                {
                    // Best effort
                }
            }
        }
    }
}