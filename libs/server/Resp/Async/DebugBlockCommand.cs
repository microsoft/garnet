// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Longest delay <c>DEBUG BLOCK</c> accepts, so a typo cannot park a session indefinitely.
        /// </summary>
        const double MaxDebugBlockSeconds = 3600;

        /// <summary>
        /// <c>DEBUG BLOCK seconds [key]</c>. Parks the session for the given number of seconds, then replies
        /// <c>+OK</c>, or the value of <c>key</c> if one was given.
        /// </summary>
        /// <remarks>
        /// Written the way any blocking command should be: a straight-line <c>async</c> body. The session is
        /// released back to the network while the body waits, so the number of connections that can be parked
        /// at once is unrelated to the size of the thread pool. The key read after the wait demonstrates two
        /// properties of the suspension: the command's parsed arguments survive it, because the receive
        /// buffer is not shifted while a command is suspended, and storage is reachable on resume, because
        /// the resume re-enters the session's epoch and response-object scope.
        /// </remarks>
        bool NetworkDebugBlock()
        {
            if (parseState.Count is < 2 or > 3)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(
                    nameof(CmdStrings.BLOCK), nameof(RespCommand.DEBUG));
            }

            if (!TryGetDebugBlockDelay(out var delay))
                return true;

            ValueTask body;
            using (BeginAsyncCommand())
                body = DebugBlockBodyAsync(delay, parseState.Count == 3);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask DebugBlockBodyAsync(TimeSpan delay, bool readKey)
        {
            if (delay > TimeSpan.Zero)
                await SessionDelay.WaitAsync(delay).ConfigureAwait(false);

            if (readKey)
                DebugBlockReadKey();
            else
                WriteDirect(CmdStrings.RESP_OK);
        }

        /// <summary>
        /// Reads the key argument of a resumed <c>DEBUG BLOCK</c> and writes it as the reply. Separated from
        /// the body because an <c>async</c> method cannot hold a reference to the storage API struct.
        /// </summary>
        void DebugBlockReadKey()
        {
            if (consistentReadActive)
                _ = DebugBlockReadKey(ref consistentReadGarnetApi);
            else
                _ = DebugBlockReadKey(ref basicGarnetApi);
        }

        bool DebugBlockReadKey<TGarnetApi>(ref TGarnetApi storageApi) where TGarnetApi : IGarnetApi
        {
            StringInput input = new(RespCommand.GET, arg1: -1);

            var key = parseState.GetArgSliceByRef(2);
            var output = GetStringOutput();

            switch (storageApi.GET(key, ref input, ref output))
            {
                case GarnetStatus.WRONGTYPE:
                    WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                    break;
                case GarnetStatus.OK:
                    ProcessOutput(output.SpanByteAndMemory);
                    break;
                default:
                    Debug.Assert(output.SpanByteAndMemory.IsSpanByte);
                    WriteNull();
                    break;
            }

            return true;
        }

        /// <summary>
        /// <c>DEBUG BLOCKON name</c>. Parks the session until another session runs <c>DEBUG SIGNAL name</c>,
        /// then replies <c>+OK</c>.
        /// </summary>
        /// <remarks>
        /// The shape of every real blocking command: a session parks on a name and a writer on a different
        /// connection releases it. Unlike <c>DEBUG BLOCK</c>, whose wakeup time is fixed in advance, the
        /// wakeup here is produced by another session's command and can therefore arrive at any point of this
        /// session's park, including before it has finished unwinding.
        /// </remarks>
        bool NetworkDebugBlockOn()
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(
                    nameof(CmdStrings.BLOCKON), nameof(RespCommand.DEBUG));
            }

            var name = parseState.GetString(1);

            ValueTask body;
            using (BeginAsyncCommand())
                body = DebugBlockOnBodyAsync(name);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask DebugBlockOnBodyAsync(string name)
        {
            var registry = storeWrapper.sessionSignalRegistry;
            var signal = SessionSignal;

            registry.Register(name, signal);
            try
            {
                await signal.WaitAsync().ConfigureAwait(false);
            }
            finally
            {
                // A wait that ended by teardown rather than by a signal is still listed.
                registry.Unregister(name, signal);
            }

            WriteDirect(CmdStrings.RESP_OK);
        }

        /// <summary>
        /// <c>DEBUG SIGNAL name</c>. Wakes every session parked on <c>name</c> and replies with how many.
        /// </summary>
        bool NetworkDebugSignal()
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(
                    nameof(CmdStrings.SIGNAL), nameof(RespCommand.DEBUG));
            }

            var woken = storeWrapper.sessionSignalRegistry.SignalAll(parseState.GetString(1));
            WriteInt32(woken);
            return true;
        }

        /// <summary>
        /// Parses the delay argument, in seconds, writing a RESP error and returning false if it is not a
        /// number in range.
        /// </summary>
        bool TryGetDebugBlockDelay(out TimeSpan delay)
        {
            delay = default;

            var raw = parseState.GetString(1);
            if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds) ||
                double.IsNaN(seconds) || seconds < 0 || seconds > MaxDebugBlockSeconds)
            {
                WriteError($"ERR timeout is not a float or out of range (0..{MaxDebugBlockSeconds})");
                return false;
            }

            delay = TimeSpan.FromSeconds(seconds);
            return true;
        }
    }
}