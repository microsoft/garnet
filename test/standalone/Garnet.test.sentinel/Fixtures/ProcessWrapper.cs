// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Modeled on the spawn-and-wait-for-ready pattern in test/standalone/Garnet.test/TestProcess.cs
// (GarnetServerTestProcess). Refactored to a generic base so we can drive
// GarnetServer, redis-server, and redis-sentinel subprocesses uniformly.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

namespace Garnet.test.sentinel.Fixtures
{
    /// <summary>
    /// Base class for a managed subprocess started for a test. Captures stdout/stderr,
    /// waits for a configurable "ready" line on stdout, and disposes by sending a
    /// polite shutdown command (when supported) and then killing the process.
    ///
    /// Concrete subclasses (GarnetServerProcess, RedisServerProcess, RedisSentinelProcess)
    /// supply the binary path, the arguments, and the ready-line string.
    /// </summary>
    internal abstract class ProcessWrapper : IDisposable
    {
        private Process? _process;
        private readonly StringBuilder _outputLog = new();
        private readonly object _logLock = new();
        private bool _ready;
        private bool _disposed;

        /// <summary>The TCP port the subprocess listens on (if applicable).</summary>
        public int Port { get; }

        /// <summary>Cumulative captured stdout/stderr, useful for test failure dumps.</summary>
        public string OutputLog
        {
            get { lock (_logLock) return _outputLog.ToString(); }
        }

        /// <summary>True after the configured ready-line has been observed on stdout.</summary>
        public bool IsReady => _ready;

        /// <summary>
        /// True if the subprocess has terminated. Used by tests that need to
        /// assert the server survived a particular interaction (e.g. an assertion
        /// failure in a command handler would otherwise take the process down).
        /// </summary>
        public bool HasExited
        {
            get
            {
                var p = _process;
                if (p == null) return true;
                try { return p.HasExited; } catch { return true; }
            }
        }

        /// <summary>OS PID of the spawned process, or 0 if not yet started / exited.</summary>
        public int ProcessId => _process != null ? _process.Id : 0;

        /// <summary>Absolute path of the binary that was launched.</summary>
        protected abstract string ExecutablePath { get; }

        /// <summary>Subclass-provided argument list, joined with spaces.</summary>
        protected abstract IReadOnlyList<string> BuildArguments();

        /// <summary>
        /// Substring that, when seen on a single stdout line, marks the process
        /// as ready to accept client connections.
        /// </summary>
        protected abstract string ReadyLineMarker { get; }

        /// <summary>
        /// Optional RESP command sent on Dispose to ask the process to exit cleanly
        /// before falling back to Process.Kill. Return null to skip.
        /// </summary>
        protected abstract string? PoliteShutdownCommand { get; }

        protected ProcessWrapper(int port)
        {
            Port = port;
        }

        /// <summary>
        /// Starts the subprocess, captures stdout/stderr, and blocks until the
        /// configured ready-line is seen or the timeout elapses.
        /// </summary>
        public void Start(TimeSpan? readyTimeout = null)
        {
            if (_process != null) throw new InvalidOperationException("Process already started.");

            var timeout = readyTimeout ?? TimeSpan.FromSeconds(Debugger.IsAttached ? 300 : 30);
            using var cts = new CancellationTokenSource(timeout);

            // Block until the chosen TCP port is free, mirroring GarnetServerTestProcess.
            while (IsPortInUse(Port))
            {
                if (cts.IsCancellationRequested)
                    throw new IOException($"Port {Port} did not become free within {timeout}.");
                Thread.Sleep(50);
            }

            var psi = new ProcessStartInfo(ExecutablePath)
            {
                CreateNoWindow = true,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            foreach (var a in BuildArguments()) psi.ArgumentList.Add(a);

            // The .NET apphost looks for the runtime via DOTNET_ROOT, defaulting
            // to /usr/share/dotnet or /usr/lib/dotnet. When tests run inside
            // `dotnet test`, the parent process has dotnet on PATH, but a child
            // process spawned here gets a fresh environment. Forward DOTNET_ROOT
            // pointing at the dotnet install root (parent of shared/) so the
            // GarnetServer apphost can locate the runtime in ~/.dotnet.
            if (!psi.Environment.ContainsKey("DOTNET_ROOT") || string.IsNullOrEmpty(psi.Environment["DOTNET_ROOT"]))
            {
                var runtimeDir = System.Runtime.InteropServices.RuntimeEnvironment.GetRuntimeDirectory();
                // runtimeDir is e.g. /home/skyline/.dotnet/shared/Microsoft.NETCore.App/10.0.12/
                // DOTNET_ROOT wants the install root: /home/skyline/.dotnet/
                var trimmed = runtimeDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                var installRoot = Path.GetDirectoryName(Path.GetDirectoryName(trimmed));
                if (!string.IsNullOrEmpty(installRoot))
                    psi.Environment["DOTNET_ROOT"] = installRoot;
            }

            AppendLog($"CMD: {psi.FileName} {string.Join(' ', psi.ArgumentList)}");

            _process = Process.Start(psi)
                ?? throw new InvalidOperationException($"Failed to start {ExecutablePath}");

            using var ready = new SemaphoreSlim(0, 1);

            _process.OutputDataReceived += (_, args) =>
            {
                if (args.Data == null) return;
                AppendLog($"[out] {args.Data}");
                if (!_ready && args.Data.Contains(ReadyLineMarker, StringComparison.Ordinal))
                {
                    _ready = true;
                    try { ready.Release(); } catch (SemaphoreFullException) { /* already released */ }
                }
            };
            _process.ErrorDataReceived += (_, args) =>
            {
                if (args.Data == null) return;
                AppendLog($"[err] {args.Data}");
            };
            _process.BeginOutputReadLine();
            _process.BeginErrorReadLine();

            try
            {
                ready.Wait(cts.Token);
            }
            catch (OperationCanceledException)
            {
                AppendLog($"!! Process did not emit '{ReadyLineMarker}' within {timeout}.");
                SafeKill();
                throw new TimeoutException(
                    $"{ExecutablePath} on port {Port} did not become ready within {timeout}.");
            }
        }

        private void AppendLog(string line)
        {
            lock (_logLock) _outputLog.AppendLine(line);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            GC.SuppressFinalize(this);
            SafeKill();
        }

        ~ProcessWrapper() => SafeKill();

        private void SafeKill()
        {
            var p = _process;
            if (p == null) return;
            try
            {
                if (!p.HasExited)
                {
                    // Polite shutdown, if the subclass asks for one.
                    var bye = PoliteShutdownCommand;
                    if (bye != null)
                    {
                        try
                        {
                            using var stdin = p.StandardInput;
                            stdin.WriteLine(bye);
                            // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                            if (p.WaitForExit(2000)) return;
                        }
                        catch
                        {
                            // Fall through to kill.
                        }
                    }

                    try { p.Kill(); } catch { /* already gone */ }
                    if (!p.WaitForExit(5000))
                        AppendLog($"!! Process {p.Id} did not exit within 5s of Kill.");
                }
            }
            catch (Exception ex)
            {
                AppendLog($"!! Dispose error: {ex.Message}");
            }
            finally
            {
                try { p.Dispose(); } catch { /* ignore */ }
                _process = null;
            }
        }

        /// <summary>
        /// Lifted from Garnet.test.TestUtils.IsPortAvailable. Kept local so this
        /// project doesn't need InternalsVisibleTo into Garnet.test.
        /// </summary>
        private static bool IsPortInUse(int port)
        {
            try
            {
                var tcpProps = System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties();
                var listeners = tcpProps.GetActiveTcpListeners();
                foreach (var ep in listeners)
                    if (ep.Port == port) return true;
                var connections = tcpProps.GetActiveTcpConnections();
                foreach (var c in connections)
                    if (c.LocalEndPoint.Port == port) return true;
            }
            catch
            {
                // If we can't tell, assume it's free; the subsequent start will fail loudly.
            }
            return false;
        }
    }
}
