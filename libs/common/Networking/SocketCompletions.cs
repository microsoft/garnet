// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Controls the threads that run socket completions on Linux and macOS. There a small set of socket event
    /// threads (epoll or kqueue) observe readiness, and by default each completion is queued to the thread pool, so
    /// every receive pays a thread hop before its input is processed. A socket that prefers inline completions has
    /// them run on the event thread itself, which removes the hop but holds that thread, and every other socket
    /// assigned to it, for as long as the completion runs. Windows has no socket event threads.
    /// </summary>
    public static class SocketCompletions
    {
        const string EventThreadCountVariable = "DOTNET_SYSTEM_NET_SOCKETS_THREAD_COUNT";

        static readonly Type eventEngineType = OperatingSystem.IsWindows() ? null :
            typeof(Socket).Assembly.GetType("System.Net.Sockets.SocketAsyncEngine");

        /// <summary>
        /// Whether sockets can be made to run their completions inline: Linux and macOS, on a runtime that exposes
        /// the internal <c>Socket.PreferInlineCompletions</c> setter, which it keeps reachable for this purpose.
        /// </summary>
        public static bool InlineCompletionsSupported { get; } = !OperatingSystem.IsWindows() &&
            typeof(Socket).GetMethod("set_PreferInlineCompletions", BindingFlags.Instance | BindingFlags.NonPublic, [typeof(bool)]) is not null;

        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "set_PreferInlineCompletions")]
        static extern void SetPreferInlineCompletions(Socket socket, bool value);

        /// <summary>
        /// Runs the completions of <paramref name="socket"/> on the socket event thread that observes them. Requires
        /// <see cref="InlineCompletionsSupported"/>, and must precede the first asynchronous operation on the socket.
        /// </summary>
        /// <param name="socket">The socket</param>
        public static void RunInline(Socket socket) => SetPreferInlineCompletions(socket, true);

        /// <summary>
        /// The number of socket event threads the process runs, or -1 where that cannot be determined: on Windows, or
        /// on a runtime whose socket engine is laid out differently. Reading it creates the threads if no socket
        /// operation has yet, fixing their number for the life of the process.
        /// </summary>
        public static int EventThreadCount
        {
            get
            {
                if (eventEngineType is null)
                    return -1;

                RuntimeHelpers.RunClassConstructor(eventEngineType.TypeHandle);
                return eventEngineType.GetField("s_engines", BindingFlags.Static | BindingFlags.NonPublic)?.GetValue(null) is Array engines
                    ? engines.Length : -1;
            }
        }

        /// <summary>
        /// Asks for <paramref name="count"/> socket event threads. The runtime reads the number from the
        /// <c>DOTNET_SYSTEM_NET_SOCKETS_THREAD_COUNT</c> environment variable once, when the first asynchronous socket
        /// operation in the process creates the threads, so the request takes effect only if none has run yet. The
        /// variable is set for that moment alone and then restored.
        /// </summary>
        /// <param name="count">The number of event threads, at least 1</param>
        /// <returns>The number of event threads the process runs, as <see cref="EventThreadCount"/></returns>
        public static int ConfigureEventThreads(int count)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
            if (eventEngineType is null)
                return -1;

            var previous = Environment.GetEnvironmentVariable(EventThreadCountVariable);
            Environment.SetEnvironmentVariable(EventThreadCountVariable, count.ToString(CultureInfo.InvariantCulture));
            try
            {
                RuntimeHelpers.RunClassConstructor(eventEngineType.TypeHandle);
            }
            finally
            {
                Environment.SetEnvironmentVariable(EventThreadCountVariable, previous);
            }

            return EventThreadCount;
        }

        /// <summary>
        /// Whether <c>DOTNET_SYSTEM_NET_SOCKETS_THREAD_COUNT</c> is set in the environment.
        /// </summary>
        public static bool EventThreadCountSetInEnvironment => Environment.GetEnvironmentVariable(EventThreadCountVariable) is not null;
    }
}