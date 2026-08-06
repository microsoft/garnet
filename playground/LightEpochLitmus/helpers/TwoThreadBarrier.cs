// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.epoch.litmus
{
    /// <summary>
    /// Two-thread lockstep barrier, plus the shutdown protocol. The reclaimer owns the deadline and
    /// runs one extra pass with <see cref="Stop"/> set; that alone can strand it, because the reader
    /// may observe Stop on its way out and never enter that pass, so <see cref="Depart"/> releases
    /// whoever is left waiting.
    /// </summary>
    internal sealed class TwoThreadBarrier
    {
        private int startCount;
        private int startSense;
        private int endCount;
        private int endSense;
        private volatile bool stop;
        private volatile bool abandoned;

        internal bool Stop => stop;

        /// <summary>Leave the barrier for good, releasing a partner that is -- or later ends up -- waiting for it.</summary>
        internal void Depart() => abandoned = true;

        /// <summary>Release the reader and let it observe <see cref="Stop"/> on its next pass.</summary>
        internal void Shutdown()
        {
            stop = true;
            WaitAtStart();
            WaitAtEnd();
            Depart();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitAtStart()
        {
            var sense = Volatile.Read(ref startSense);
            if (Interlocked.Increment(ref startCount) == 2)
            {
                startCount = 0;
                Volatile.Write(ref startSense, sense ^ 1);
                return;
            }

            var spinner = new SpinWait();
            while (Volatile.Read(ref startSense) == sense && !abandoned)
                spinner.SpinOnce(-1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitAtEnd()
        {
            var sense = Volatile.Read(ref endSense);
            if (Interlocked.Increment(ref endCount) == 2)
            {
                endCount = 0;
                Volatile.Write(ref endSense, sense ^ 1);
                return;
            }

            var spinner = new SpinWait();
            while (Volatile.Read(ref endSense) == sense && !abandoned)
                spinner.SpinOnce(-1);
        }
    }
}