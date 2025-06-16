// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.common.Parsing;
using Garnet.server;

namespace Garnet.fuzz.Targets
{
    /// <summary>
    /// Fuzz target for <see cref="RespCommand"/> parsing.
    /// </summary>
    public sealed class RespCommandParsing : IFuzzerTarget
    {
        /// <inheritdoc/>
        public static void Fuzz(ReadOnlySpan<byte> input)
        {
            IFuzzerTarget.PrepareInput(ref input);

            var session = new RespServerSession();

            try
            {
                var success = session.FuzzParseCommandBuffer(input, out var cmd);

                if (success)
                {
                    // Validate (slowly) that the input buffer SHOULD have been parsed this way

                    if (input[0] != (byte)'*')
                    {
                        IFuzzerTarget.RaiseErrorForInput("Resp request should start with *", input);
                    }

                    var arrCrIx = input.IndexOf((byte)'\r');
                    if (arrCrIx == -1)
                    {
                        IFuzzerTarget.RaiseErrorForInput("Resp request should have \\r after array item count", input);
                    }

                    var arrItems = input[1..arrCrIx];
                    if (arrItems.ContainsAnyExceptInRange((byte)'0', (byte)'9'))
                    {
                        IFuzzerTarget.RaiseErrorForInput("Resp request should have number following *", input);
                    }

                    if (input[arrCrIx + 1] != (byte)'\n')
                    {
                        IFuzzerTarget.RaiseErrorForInput("Resp request should have \\n after array item count", input);
                    }

                    if (!int.TryParse(arrItems, out var arrItemCount) || arrItemCount < 1)
                    {
                        IFuzzerTarget.RaiseErrorForInput("Should not have accepted array item count", input);
                    }

                    if (input[arrCrIx + 2] != (byte)'$')
                    {
                        IFuzzerTarget.RaiseErrorForInput("Should not have accepted non-bulk string", input);
                    }

                    var cmdCrIx = input[(arrCrIx + 2)..].IndexOf((byte)'\r');
                    if (cmdCrIx == -1)
                    {
                        IFuzzerTarget.RaiseErrorForInput("Resp request should have \\r after bulk string length", input);
                    }

                    cmdCrIx += arrCrIx + 2;
                    if (input[cmdCrIx + 1] != (byte)'\n')
                    {
                        IFuzzerTarget.RaiseErrorForInput("Resp request should have \\n after bulk string length", input);
                    }

                    var strLength = input[(arrCrIx + 3)..cmdCrIx];
                    if (strLength.ContainsAnyExceptInRange((byte)'0', (byte)'9'))
                    {
                        IFuzzerTarget.RaiseErrorForInput("Resp request should have number following $", input);
                    }

                    if (!int.TryParse(strLength, out var strLengthCount) || strLengthCount < 1)
                    {
                        IFuzzerTarget.RaiseErrorForInput("Should not have accepted string length", input);
                    }

                    if (input[cmdCrIx + 2 + strLengthCount] != (byte)'\r')
                    {
                        IFuzzerTarget.RaiseErrorForInput("Command name should have trailing \\r", input);
                    }

                    if (input[cmdCrIx + 2 + strLengthCount + 1] != (byte)'\n')
                    {
                        IFuzzerTarget.RaiseErrorForInput("Command name should have trailing \\n", input);
                    }

                    var cmdSlice = input.Slice(cmdCrIx + 2, strLengthCount);
                    var cmdStr = Encoding.UTF8.GetString(cmdSlice);

                    if (!Enum.TryParse<RespCommand>(cmdStr, ignoreCase: true, out var parsedCommand))
                    {
                        IFuzzerTarget.RaiseErrorForInput("Command name should not have been accepted", input);
                    }

                    if (parsedCommand != cmd)
                    {
                        IFuzzerTarget.RaiseErrorForInput($"Command name does match enum parse: {parsedCommand}", input);
                    }

                    if (parsedCommand.ToString() != cmdStr.ToUpperInvariant())
                    {
                        IFuzzerTarget.RaiseErrorForInput($"Command string equivalent is not as expected: {parsedCommand} != {cmdStr.ToUpperInvariant()}", input);
                    }

                    var isParentCmd =
                        cmd is RespCommand.ACL or RespCommand.BITOP or RespCommand.CLIENT or RespCommand.CLUSTER or
                        RespCommand.CONFIG or RespCommand.LATENCY or RespCommand.MEMORY or RespCommand.MODULE or
                        RespCommand.PUBSUB or RespCommand.SCRIPT or RespCommand.SLOWLOG;

                    if (isParentCmd)
                    {
                        IFuzzerTarget.RaiseErrorForInput($"Accepted parent command, which should never be parsed by itself: {parsedCommand}", input);
                    }
                }
            }
            catch (RespParsingException)
            {
                // Intentional parsing errors are expected and fine
            }
        }
    }
}