// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Text;
using Embedded.server;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Tsavorite.core;

namespace Garnet.fuzz.Targets
{
    /// <summary>
    /// Fuzz target for all of <see cref="GarnetServer"/>, using <see cref="EmbeddedRespServer"/> for hosting.
    /// </summary>
    public sealed class GarnetEndToEnd : IFuzzerTarget
    {
        private static DirectoryInfo? LogDir;
        private static DirectoryInfo? CheckpointDir;

        /// <inheritdoc/>
        public static void Fuzz(ReadOnlySpan<byte> input)
        {
            IFuzzerTarget.PrepareInput(ref input);

            try
            {
                using var server = CreateServer();

                // Do a bit of stretching so input looks more like a RESP command
                //
                // We explore up to N parts,
                //   0 - *<num>\r\n (array header)
                //   1 - $<len>\r\n (string header)
                //   2 - <data>\r\n (string data)
                //   3 - $<len>\r\n (second string header)
                //   4 - <data>\r\n> (second string data)
                //   5 - $<len>\r\n (third string header)
                //   6 - <data>\r\n> (third string data)
                //
                // Though in each case we sometimes produce invalid commands
                //
                // As an escape to let the fuzzer do it's thing, we also just straight copy input
                // for a final special case.

                var buff = new List<byte>();

                if (input.Length >= 2 && input[0] == byte.MaxValue)
                {
                    // Pass fuzzer input through unmodified
                    buff.AddRange(input[1..]);
                }
                else
                {
                    // Do nudging in the direction of RESP to help the fuzzer explore more of the Garnet code

                    var numParts = input.IsEmpty ? 0 : input[0] % 7;

                    var remainingInput = input.IsEmpty ? input : input[1..];

                    var lastStringLength = -1;

                    for (var i = 0; i <= numParts; i++)
                    {
                        switch (i)
                        {
                            case 0:
                                AddArrayHeader(buff, numParts, ref input);
                                break;
                            case 1:
                            case 3:
                            case 5:
                                AddStringHeader(buff, ref input, out lastStringLength);
                                break;
                            case 2:
                            case 4:
                            case 6:
                                AddStringData(buff, lastStringLength, ref input);
                                break;
                            default:
                                throw new InvalidOperationException($"Unexpected part number {i}");
                        }
                    }
                }

                var commandBuffer = buff.ToArray();

                using var session = server.GetRespSession();

                unsafe
                {
                    fixed (byte* ptr = commandBuffer)
                    {
                        _ = session.TryConsumeMessages(ptr, commandBuffer.Length);
                    }
                }
            }
            finally
            {
                try
                {
                    LogDir?.Delete(recursive: true);
                }
                catch
                {
                    // Best effort
                }

                try
                {
                    CheckpointDir?.Delete(recursive: true);
                }
                catch
                {
                    // Best effort
                }
            }

            static void AddStringData(List<byte> buff, int lastStringLength, ref ReadOnlySpan<byte> input)
            {
                var controlByte = input.IsEmpty ? 0 : input[0];
                input = input.IsEmpty ? input : input[1..];

                if (controlByte < 128)
                {
                    // Produce valid string data

                    var data = lastStringLength <= input.Length ? input[..lastStringLength] : input;
                    input = input[data.Length..];

                    buff.AddRange(data);

                    // Consistently pad data is necessary
                    var padding = lastStringLength - data.Length;
                    buff.AddRange(Enumerable.Repeat((byte)controlByte, padding));

                    buff.AddRange([(byte)'\r', (byte)'\n']);
                }
                else
                {
                    // Produce invalid string data

                    // A few different possibilities
                    //   0 - too little data
                    //   1 - too much data
                    //   2 - no \r
                    //   3 - no \n
                    //   4 - copy bytes out of input
                    var invalidKind = controlByte % 5;
                    switch (invalidKind)
                    {
                        case 0:
                            {
                                var tooLittle = Math.Max(lastStringLength - 1, 0);
                                var data = lastStringLength <= input.Length ? input[..tooLittle] : input;
                                input = input[data.Length..];

                                buff.AddRange(data);

                                // Consistently pad data is necessary
                                var padding = tooLittle - data.Length;
                                buff.AddRange(Enumerable.Repeat((byte)controlByte, padding));

                                buff.AddRange([(byte)'\r', (byte)'\n']);
                            }
                            break;
                        case 1:
                            {
                                var tooMuch = lastStringLength + 1;
                                var data = tooMuch <= input.Length ? input[..tooMuch] : input;
                                input = input[data.Length..];

                                buff.AddRange(data);

                                // Consistently pad data is necessary
                                var padding = tooMuch - data.Length;
                                buff.AddRange(Enumerable.Repeat((byte)controlByte, padding));

                                buff.AddRange([(byte)'\r', (byte)'\n']);
                            }
                            break;
                        case 2:
                            {
                                var data = lastStringLength <= input.Length ? input[..lastStringLength] : input;
                                input = input[data.Length..];

                                buff.AddRange(data);

                                // Consistently pad data is necessary
                                var padding = lastStringLength - data.Length;
                                buff.AddRange(Enumerable.Repeat((byte)controlByte, padding));

                                buff.Add((byte)'\n');
                            }
                            break;
                        case 3:
                            {
                                var data = lastStringLength <= input.Length ? input[..lastStringLength] : input;
                                input = input[data.Length..];

                                buff.AddRange(data);

                                // Consistently pad data is necessary
                                var padding = lastStringLength - data.Length;
                                buff.AddRange(Enumerable.Repeat((byte)controlByte, padding));

                                buff.Add((byte)'\r');
                            }
                            break;
                        case 4:
                            {
                                var data = lastStringLength + 2 <= input.Length ? input[..lastStringLength] : input;
                                input = input[data.Length..];

                                buff.AddRange(data);

                                var padding = (lastStringLength + 2) - data.Length;
                                buff.AddRange(Enumerable.Repeat((byte)controlByte, padding));
                            }
                            break;
                        default:
                            throw new InvalidOperationException($"Unexpected invalid string data option {invalidKind}");
                    }
                }
            }

            // Create a string header for a RESP string based on input
            static void AddStringHeader(List<byte> buff, ref ReadOnlySpan<byte> input, out int stringLength)
            {
                var controlByte = input.IsEmpty ? 0 : input[0];
                input = input.IsEmpty ? input : input[1..];

                if (controlByte < 128)
                {
                    // Produce a valid string header
                    stringLength = Math.Min(Math.Max((input.Length / 2) - 1, 0), 32);

                    buff.AddRange(Encoding.ASCII.GetBytes($"${stringLength}\r\n"));
                }
                else
                {
                    // Produce an invalid header

                    stringLength = controlByte % 33;

                    // A few different possibilities
                    //   0 - wrong sigil (ie. not *)
                    //   1 - not a number
                    //   2 - no \r
                    //   3 - no \n
                    //   4 - copy bytes out of input
                    var invalidKind = controlByte % 5;
                    switch (invalidKind)
                    {
                        case 0:
                            buff.AddRange(Encoding.ASCII.GetBytes($"{(char)('$' + controlByte)}{stringLength}\r\n"));
                            break;
                        case 1:
                            buff.AddRange(Encoding.ASCII.GetBytes($"${(char)controlByte}\r\n"));
                            break;
                        case 2:
                            buff.AddRange(Encoding.ASCII.GetBytes($"${stringLength}\n"));
                            break;
                        case 3:
                            buff.AddRange(Encoding.ASCII.GetBytes($"${stringLength}\r"));
                            break;
                        case 4:
                            if (input.Length >= 4)
                            {
                                buff.AddRange(input[..4]);
                                input = input[4..];
                            }
                            else
                            {
                                buff.AddRange(buff);
                                input = [];
                            }
                            break;
                        default:
                            throw new InvalidOperationException($"Unexpected invalid string header option {invalidKind}");
                    }
                }
            }

            // Create an array header for a RESP command based on input and part counts
            static void AddArrayHeader(List<byte> buff, int numParts, ref ReadOnlySpan<byte> input)
            {
                if (numParts == 0)
                {
                    buff.AddRange([(byte)'*', (byte)'0', (byte)'\r', (byte)'\n']);
                }
                else
                {
                    var numStrings = (numParts / 2) + (numParts % 2);

                    var controlByte = input.IsEmpty ? 0 : input[0];
                    input = input.IsEmpty ? input : input[1..];

                    if (controlByte < 128)
                    {
                        // Produce a valid header
                        buff.AddRange([(byte)'*', (byte)('0' + numStrings), (byte)'\r', (byte)'\n']);
                    }
                    else
                    {
                        // Produce an invalid header
                        //
                        // A few different possibilities
                        //   0 - wrong sigil (ie. not *)
                        //   1 - not a number
                        //   2 - no \r
                        //   3 - no \n
                        //   4 - copy bytes out of input
                        var invalidKind = controlByte % 5;
                        switch (invalidKind)
                        {
                            case 0:
                                buff.AddRange([(byte)('*' + controlByte), (byte)('0' + numStrings), (byte)'\r', (byte)'\n']);
                                break;
                            case 1:
                                buff.AddRange([(byte)('*' + controlByte), (byte)controlByte, (byte)'\r', (byte)'\n']);
                                break;
                            case 2:
                                buff.AddRange([(byte)'*', (byte)('0' + numStrings), (byte)'\n']);
                                break;
                            case 3:
                                buff.AddRange([(byte)'*', (byte)('0' + numStrings), (byte)'\r']);
                                break;
                            case 4:
                                if (input.Length >= 4)
                                {
                                    buff.AddRange(input[..4]);
                                    input = input[4..];
                                }
                                else
                                {
                                    buff.AddRange(buff);
                                    input = [];
                                }
                                break;
                            default:
                                throw new InvalidOperationException($"Unexpected invalid array header option {invalidKind}");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Create a <see cref="EmbeddedRespServer"/> with some reasonable default settings.
        /// </summary>
        private static EmbeddedRespServer CreateServer()
        {
            CheckpointDir = MakeTempDir("checkpoint-");
            LogDir = MakeTempDir("log-");

            GarnetServerOptions opts = new()
            {
                ThreadPoolMinThreads = 100,
                SegmentSize = "1g",
                ObjectLogSegmentSize = "1g",
                EnableStorageTier = true,
                LogDir = LogDir.FullName,
                CheckpointDir = CheckpointDir.FullName,
                EndPoints = [new IPEndPoint(IPAddress.Loopback, 1234)],
                DisablePubSub = false,
                DisableObjects = false,
                EnableDebugCommand = ConnectionProtectionOption.Yes,
                Recover = false,
                IndexSize = "1m",
                EnableCluster = true,
                CleanClusterConfig = true,
                ClusterTimeout = -1,
                QuietMode = true,
                EnableAOF = true,
                MemorySize = "1g",
                GossipDelay = 5,
                EnableFastCommit = true,
                MetricsSamplingFrequency = 0,
                TlsOptions = null,
                DeviceFactoryCreator = new LocalStorageNamedDeviceFactoryCreator(),
                FastAofTruncate = true,
                AofMemorySize = "64m",
                OnDemandCheckpoint = true,
                CommitFrequencyMs = -1,
                EnableIncrementalSnapshots = true,
                AuthSettings = null,
                ClusterUsername = "cluster-user",
                ClusterPassword = "cluster-pass",
                EnableLua = true,
                ReplicationOffsetMaxLag = -1,
                LuaOptions = new LuaOptions(LuaMemoryManagementMode.Native, "10m", TimeSpan.FromSeconds(2), LuaLoggingMode.Enable, []),
                UnixSocketPath = null,
                ReplicaDisklessSync = true,
                ReplicaDisklessSyncDelay = 1,
                ClusterAnnounceEndpoint = null,
            };

            var server = new EmbeddedRespServer(opts);
            server.Start();

            return server;

            static DirectoryInfo MakeTempDir(string prefix)
            {
                while (true)
                {
                    try
                    {
                        var tempFile = Path.Combine(Path.GetTempPath(), $"{prefix}{Guid.NewGuid()}");
                        return Directory.CreateDirectory(tempFile);
                    }
                    catch
                    {
                        // What?
                    }
                }
            }
        }
    }
}