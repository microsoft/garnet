/*
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace HdrHistogram.Utilities
{
    /// <summary>
    /// <see cref="WriterReaderPhaser"/> instances provide an asymmetric means for synchronizing the execution of wait-free "writer" critical sections against a "reader phase flip" that needs to make sure no writer critical sections that were active at the beginning of the flip are still active after the flip is done.
    /// Multiple writers and multiple readers are supported.
    /// </summary>
    /// <remarks>
    /// <para>
    /// While a<see cref="WriterReaderPhaser"/> can be useful in multiple scenarios, a specific and common use case is that of safely managing "double buffered" data stream access.
    /// This style of access allows writers to proceed without being blocked, while readers gain access to stable and unchanging buffer samples
    /// </para>
    /// 
    /// <blockquote>
    /// NOTE: <see cref="WriterReaderPhaser" /> writers are wait-free on architectures that support wait-free atomic increment operations.
    /// They remain lock-free (but not wait-free) on architectures that do not support wait-free atomic increment operations.
    /// </blockquote>
    /// <see cref="WriterReaderPhaser"/> "writers" are wait free, "readers" block for other "readers", and "readers" are only blocked by "writers" whose critical was entered before the reader's <seealso cref="FlipPhase()"/> attempt.
    /// <para>
    /// When used to protect an actively recording data structure, the assumptions on how readers and writers act are:
    /// <list type="bullet">
    /// <item>There are two sets of data structures("active" and "inactive")</item>
    /// <item>Writing is done to the perceived active version(as perceived by the writer), and only within critical sections delineated by <see cref="WriterCriticalSectionEnter()"/> and <see cref="WriterCriticalSectionExit(long)"/>).</item>
    /// <item> Only readers switch the perceived roles of the active and inactive data structures.
    /// They do so only while under <see cref="ReaderLock()"/>, and only before calling <see cref="FlipPhase()"/>.</item>
    /// </list>
    /// When the above assumptions are met, <see cref="WriterReaderPhaser"/> guarantees that the inactive data structures are not being modified by any writers while being read while under <seealso cref="ReaderLock()"/> protection after a <see cref="FlipPhase()"/> operation.
    /// </para>
    /// </remarks>
    public class WriterReaderPhaser
    {
        private readonly object _readerLock = new object();

        private long _startEpoch = 0;
        private long _evenEndEpoch = 0;
        private long _oddEndEpoch = long.MinValue;

        private static long GetAndIncrement(ref long value)
        {
            var updatedValue = Interlocked.Increment(ref value);
            return updatedValue - 1;    //previous value;
        }
        private static long GetAndSet(ref long value, long newValue)
        {
            return Interlocked.Exchange(ref value, newValue);
        }
        private static void LazySet(ref long value, long newValue)
        {
            Interlocked.Exchange(ref value, newValue);
        }

        /// <summary>
        /// Indicate entry to a critical section containing a write operation.
        /// </summary>
        /// <returns>
        /// an (opaque) value associated with the critical section entry, 
        /// which MUST be provided to the matching <see cref="WriterCriticalSectionExit"/> call.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This call is wait-free on architectures that support wait free atomic increment operations,
        /// and is lock-free on architectures that do not.
        /// </para>
        /// <para>
        /// <see cref="WriterCriticalSectionEnter"/> must be matched with a subsequent <see cref="WriterCriticalSectionExit"/>
        /// in order for CriticalSectionPhaser synchronization to function properly.
        /// </para>
        /// <para>
        /// The <seealso cref="IDisposable"/> pattern could have been used but was not due to the high allocation count it would have incurred.
        /// </para>
        /// </remarks>
        public long WriterCriticalSectionEnter()
        {
            return GetAndIncrement(ref _startEpoch);
        }

        /// <summary>
        /// Indicate exit from a critical section containing a write operation.
        /// </summary>
        /// <param name="criticalValueAtEnter">the opaque value (token) returned from the matching <see cref="WriterCriticalSectionEnter()"/> call.</param>
        /// <remarks>
        /// This call is wait-free on architectures that support wait free atomic increment operations, and is lock-free on architectures that do not.
        /// <para>
        /// <see cref="WriterCriticalSectionExit(long)"/> must be matched with a preceding  <see cref="WriterCriticalSectionEnter"/> call, and must be provided with the matching <see cref="WriterCriticalSectionEnter"/> call's return value, in order for CriticalSectionPhaser synchronization to function properly.
        /// </para>
        /// </remarks>
        public void WriterCriticalSectionExit(long criticalValueAtEnter)
        {
            if (criticalValueAtEnter < 0)
            {
                GetAndIncrement(ref _oddEndEpoch);
            }
            else
            {
                GetAndIncrement(ref _evenEndEpoch);
            }
        }

        /// <summary>
        /// Enter to a critical section containing a read operation (mutually excludes against other <see cref="ReaderLock()"/> calls).
        /// <see cref="ReaderLock"/> DOES NOT provide synchronization against <see cref="WriterCriticalSectionEnter"/> calls.
        /// Use <see cref="FlipPhase()"/> to synchronize reads against writers.
        /// </summary>
        public void ReaderLock()
        {
            Monitor.Enter(_readerLock);
        }

        /// <summary>
        /// Exit from a critical section containing a read operation(relinquishes mutual exclusion against other <see cref="ReaderLock()"/> calls).
        /// </summary>
        public void ReaderUnlock()
        {
            Monitor.Exit(_readerLock);
        }


        /// <summary>
        /// Flip a phase in the <see cref="WriterReaderPhaser"/> instance, <see cref="FlipPhase(System.TimeSpan)"/> can only be called while holding the <see cref="ReaderLock()"/>.
        /// </summary>
        /// <param name="yieldPeriod">The amount of time to sleep in each yield if yield loop is needed.</param>
        /// <remarks>
        /// <seealso cref="FlipPhase(System.TimeSpan)"/> will return only after all writer critical sections (protected by <see cref="WriterCriticalSectionEnter()"/> and <see cref="WriterCriticalSectionExit(long)"/> that may have been in flight when the <see cref="FlipPhase(System.TimeSpan)"/> call were made had completed.
        /// <para>
        /// No actual writer critical section activity is required for <see cref="FlipPhase(System.TimeSpan)"/> to succeed.
        /// </para>
        /// <para>
        /// However, <see cref="FlipPhase(System.TimeSpan)"/> is lock-free with respect to calls to <see cref="WriterCriticalSectionEnter()"/> and <see cref="WriterCriticalSectionExit(long)"/>. 
        /// It may spin-wait for for active writer critical section code to complete.
        /// </para>
        /// </remarks>
        public void FlipPhase(TimeSpan yieldPeriod)
        {
            if (!Monitor.IsEntered(_readerLock))
            {
                throw new InvalidOperationException("FlipPhase can only be called while holding the reader lock");
            }

            var isNextPhaseEven = (_startEpoch < 0); // Current phase is odd...

            long initialStartValue;
            // First, clear currently unused [next] phase end epoch (to proper initial value for phase):
            if (isNextPhaseEven)
            {
                initialStartValue = 0;
                LazySet(ref _evenEndEpoch, initialStartValue);
            }
            else
            {
                initialStartValue = long.MinValue;
                LazySet(ref _oddEndEpoch, initialStartValue);
            }

            // Next, reset start value, indicating new phase, and retain value at flip:
            //long startValueAtFlip = startEpochUpdater.getAndSet(this, initialStartValue);
            long startValueAtFlip = GetAndSet(ref _startEpoch, initialStartValue);

            // Now, spin until previous phase end value catches up with start value at flip:
            bool caughtUp = false;
            do
            {
                if (isNextPhaseEven)
                {
                    caughtUp = (_oddEndEpoch == startValueAtFlip);
                }
                else
                {
                    caughtUp = (_evenEndEpoch == startValueAtFlip);
                }
                if (!caughtUp)
                {
                    //TODO: Revist this and check if a SpinWiat is actually preferable here? -LC
                    if (yieldPeriod == TimeSpan.Zero)
                    {
                        Task.Yield().GetAwaiter().GetResult();
                    }
                    else
                    {
                        //Thread.Sleep(yieldPeriod);
                        Task.Delay(yieldPeriod).GetAwaiter().GetResult();
                    }
                }
            } while (!caughtUp);
        }

        /// <summary>
        /// Flip a phase in the <see cref="WriterReaderPhaser"/> instance, <see cref="FlipPhase()"/> can only be called while holding the <see cref="ReaderLock()"/>.
        /// </summary>
        /// <remarks>
        /// <seealso cref="FlipPhase()"/> will return only after all writer critical sections (protected by <see cref="WriterCriticalSectionEnter()"/> and <see cref="WriterCriticalSectionExit(long)"/> that may have been in flight when the <see cref="FlipPhase(System.TimeSpan)"/> call were made had completed.
        /// <para>
        /// No actual writer critical section activity is required for <see cref="FlipPhase()"/> to succeed.
        /// </para>
        /// <para>
        /// However, <see cref="FlipPhase()"/> is lock-free with respect to calls to <see cref="WriterCriticalSectionEnter()"/> and <see cref="WriterCriticalSectionExit(long)"/>. 
        /// It may spin-wait for for active writer critical section code to complete.
        /// </para>
        /// </remarks>
        public void FlipPhase()
        {
            FlipPhase(TimeSpan.Zero);
        }
    }
}
