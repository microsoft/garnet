// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Coordinates command admission and draining for client pause and shutdown.
    /// </summary>
    internal sealed class ClientPauseManager
    {
        readonly object sync = new();
        readonly ConcurrentDictionary<Participant, byte> participants = new();
        long clientDeadline;
        int clientMode, shutdownMode, mode;
        bool stopped;

        internal bool IsWritePaused => GetMode() != 0;

        int GetMode()
        {
            if (Volatile.Read(ref mode) == 0) return 0;
            lock (sync)
            {
                if (clientMode != 0 && Environment.TickCount64 >= clientDeadline)
                {
                    clientMode = 0;
                    UpdateMode();
                }
                return mode;
            }
        }

        void UpdateMode()
        {
            Interlocked.Exchange(ref mode, Math.Max(clientMode, shutdownMode));
            Monitor.PulseAll(sync);
        }

        internal Participant Register()
        {
            var participant = new Participant(this);
            participants.TryAdd(participant, 0);
            return participant;
        }

        internal void Pause(long milliseconds, bool all, RespServerSession requester)
        {
            lock (sync)
            {
                var now = Environment.TickCount64;
                if (clientDeadline <= now) clientMode = 0;
                clientDeadline = Math.Max(clientMode == 0 ? 0 : clientDeadline, now + milliseconds);
                clientMode = Math.Max(clientMode, all ? 2 : 1);
                UpdateMode();
            }
            WaitForDrain(requester);
        }

        internal void Unpause()
        {
            lock (sync) { clientMode = 0; UpdateMode(); }
        }

        internal void SetShutdownPause(bool all, RespServerSession requester)
        {
            lock (sync) { shutdownMode = all ? 2 : 1; UpdateMode(); }
            WaitForDrain(requester);
        }

        internal void ClearShutdownPause()
        {
            lock (sync) { shutdownMode = 0; UpdateMode(); }
        }

        void WaitForDrain(RespServerSession requester)
        {
            while (true)
            {
                var currentMode = GetMode();
                var pending = false;
                foreach (var participant in participants.Keys)
                {
                    if (participant == requester?.pauseParticipant) continue;
                    var active = Volatile.Read(ref participant.active);
                    if (active != 0 && (currentMode == 2 || currentMode == 1 && active == 2)) { pending = true; break; }
                }
                if (!pending) return;
                Thread.Sleep(1);
            }
        }

        internal void Stop()
        {
            lock (sync) { stopped = true; clientMode = shutdownMode = 0; UpdateMode(); }
        }

        internal sealed class Participant : IDisposable
        {
            readonly ClientPauseManager owner;
            internal int active;
            bool disposed;
            internal Participant(ClientPauseManager owner) => this.owner = owner;

            internal bool TryEnter(bool write)
            {
                if (Volatile.Read(ref disposed) || Volatile.Read(ref owner.stopped)) throw new ObjectDisposedException(nameof(ClientPauseManager));
                Interlocked.Exchange(ref active, write ? 2 : 1);
                var currentMode = owner.GetMode();
                if (currentMode == 0 || currentMode == 1 && !write) return true;
                Exit();
                return false;
            }

            internal void Wait(bool write)
            {
                lock (owner.sync)
                {
                    while (true)
                    {
                        if (disposed || owner.stopped) throw new ObjectDisposedException(nameof(ClientPauseManager));
                        var currentMode = owner.GetMode();
                        if (currentMode == 0 || currentMode == 1 && !write) return;
                        var delay = owner.clientMode == 0 ? Timeout.Infinite : (int)Math.Clamp(owner.clientDeadline - Environment.TickCount64, 1, int.MaxValue);
                        Monitor.Wait(owner.sync, delay);
                    }
                }
            }

            internal void Enter(bool write)
            {
                while (!TryEnter(write)) Wait(write);
            }

            internal void Exit() => Interlocked.Exchange(ref active, 0);
            public void Dispose()
            {
                lock (owner.sync) { disposed = true; Exit(); owner.participants.TryRemove(this, out _); owner.UpdateMode(); }
            }
        }
    }
}