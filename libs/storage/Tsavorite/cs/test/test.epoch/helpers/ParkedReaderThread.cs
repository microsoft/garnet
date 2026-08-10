// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.epoch
{
    /// <summary>
    /// A thread that enters a protected region and stays there until released. Several tests need
    /// an epoch pinned open by somebody other than the test thread, because that is the only way to
    /// stop <see cref="LightEpoch.SafeToReclaimEpoch"/> from advancing and drain actions from firing.
    /// </summary>
    internal sealed class ParkedReaderThread : IDisposable
    {
        readonly Thread thread;
        readonly ManualResetEventSlim release = new();
        long announcedEpoch;

        /// <summary>The epoch this reader announced when it entered.</summary>
        internal long AnnouncedEpoch => Volatile.Read(ref announcedEpoch);

        internal ParkedReaderThread(LightEpoch epoch)
        {
            using var entered = new ManualResetEventSlim();
            thread = new Thread(() =>
            {
                epoch.Resume();
                Volatile.Write(ref announcedEpoch, epoch.TestHookThisThreadAnnouncedEpoch());
                entered.Set();
                release.Wait();
                epoch.Suspend();
            })
            { IsBackground = true, Name = nameof(ParkedReaderThread) };

            thread.Start();
            entered.Wait();
        }

        /// <summary>Let the reader suspend, and wait until it has.</summary>
        internal void LeaveAndJoin()
        {
            release.Set();
            thread.Join();
        }

        public void Dispose()
        {
            LeaveAndJoin();
            release.Dispose();
        }
    }
}