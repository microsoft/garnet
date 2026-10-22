// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Garnet.server
{
    /// <summary>
    /// Where the command a session is running can be run again from, should it meet a key another session holds locked
    /// (see <see cref="RespServerSession.OnLockContended"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Armed as the command is dispatched, with the receive-buffer offset of its first byte and the reply-buffer position
    /// it starts writing at. Disarmed as soon as running the command again would no longer be the same as running it
    /// once: it has changed a record (<see cref="FunctionsState.OnKeyChanged(long)"/>) or sent part of its reply. A
    /// disarmed point keeps the command, so a command that records its own progress can still
    /// be continued (see <see cref="RespServerSession.LockWaitClass"/>).
    /// </para>
    /// <para>
    /// One per session, shared by the functions state of each of its storage sessions, so the store callbacks that
    /// change a record disarm it with a single store into a line the dispatch loop has just written.
    /// </para>
    /// </remarks>
    internal sealed unsafe class CommandRestartPoint
    {
        /// <summary>
        /// The command (bits 32-47) and the receive-buffer offset of its first byte (bits 0-31). Negative once disarmed.
        /// </summary>
        long point = long.MinValue;

        /// <summary>
        /// Reply-buffer position the command started writing at, or null once a send has handed what was before it to the
        /// socket and the command is to start again at the head of the next response object.
        /// </summary>
        internal byte* replyStart;

        /// <summary>
        /// Whether the command can still be run again from the start.
        /// </summary>
        internal bool IsArmed => point >= 0;

        /// <summary>
        /// The command last armed for, armed or not.
        /// </summary>
        internal RespCommand Command => (RespCommand)(ushort)((point & long.MaxValue) >> 32);

        /// <summary>
        /// Receive-buffer offset of the first byte of the command last armed for.
        /// </summary>
        internal int ReadHead => (int)(uint)point;

        /// <summary>
        /// Arms the point for a command about to be dispatched.
        /// </summary>
        /// <param name="cmd">The command.</param>
        /// <param name="readHead">Receive-buffer offset of its first byte.</param>
        /// <param name="dcurr">Reply-buffer position it starts writing at.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Arm(RespCommand cmd, int readHead, byte* dcurr)
        {
            point = ((long)(ushort)cmd << 32) | (uint)readHead;
            replyStart = dcurr;
        }

        /// <summary>
        /// Disarms the point; idempotent.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Disarm() => point |= long.MinValue;

        /// <summary>
        /// Records <paramref name="cmd"/> as being continued from its own recorded progress: disarmed, so it is not run
        /// again from the start, but keeping the command so a further contention continues it again.
        /// </summary>
        /// <param name="cmd">The command being continued.</param>
        internal void Continue(RespCommand cmd) => point = long.MinValue | ((long)(ushort)cmd << 32);

        /// <summary>
        /// Called as a send hands the reply buffer up to <paramref name="dcurr"/> to the socket.
        /// </summary>
        /// <param name="dcurr">End of what is sent.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnSend(byte* dcurr)
        {
            // Nothing of the command's own reply went: it starts again at the head of the next response object
            if (dcurr == replyStart)
                replyStart = null;
            else
                Disarm();
        }
    }
}