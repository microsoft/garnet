/*
 * Written by Matt Warren, and released to the public domain,
 * as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 *
 * This is a .NET port of the original Java version, which was written by
 * Gil Tene as described in
 * https://github.com/HdrHistogram/HdrHistogram
 */
using System.Threading;

namespace HdrHistogram.Utilities
{
    /// <summary>
    /// This is a basic implementation/port, of just the methods that are required internally.
    /// </summary>
    internal sealed class AtomicLongArray
    {
        private readonly long[] _counts;

        public AtomicLongArray(int arrayLength)
        {
            _counts = new long[arrayLength];
        }

        public int Length => _counts.Length;

        public long this[int index]
        {
            get { return Interlocked.Read(ref _counts[index]); }
            set
            {
                LazySet(index, value);
            }
        }

        public long IncrementAndGet(int index)
        {
            return Interlocked.Add(ref _counts[index], 1);
        }

        public long AddAndGet(int index, long value)
        {
            return Interlocked.Add(ref _counts[index], value);
        }

        private void LazySet(int index, long value)
        {
            // TODO Revisit this, work out which method is the same as lazySet!!!
            // Note this is only called when we are clearing out the AtomicHistogram (From AtomicHistogram clearCounts()),
            // so it's not performance critical (i.e. not on Hot-Path), but still worth looking at

            // From http://stackoverflow.com/questions/8381440/how-is-lazyset-in-javas-atomic-classes-implemented/8420284#8420284
            // For people who like to think of these operations in terms of machine-level barriers on common multiprocessors, lazySet provides a 
            // preceding store-store barrier (which is either a no-op or very cheap on current platforms), but no store-load barrier (which is 
            // usually the expensive part of a volatile-write)

            // From http://mechanitis.blogspot.co.uk/2011/10/mike-and-i-debut-our-new-disruptor.html?showComment=1320601640614&_sm_au_=iVVrQRjv5k6LJ0k5#c4014181423217401642
            // The main difference between lazySet and a volatile write is that the lazySet does not guarantee that the value is made immediately visible, 
            // i.e. store buffers are not immediate flushed out to memory. The value will still become visible, eventually. 
            // The guarantee that the lazySet provides is that the data will be made visible in the correct order. I.e. the write to the ring buffer will 
            // occur before the update of the associated sequence.

            // http://mechanical-sympathy.blogspot.co.uk/2011/08/disruptor-20-released.html?showComment=1322219765503&_sm_au_=iVVrQRjv5k6LJ0k5#c8612572394778861289
            // The JVM/JIT is able to optimise and in the case of x86 the AtomicLong.lazySet() is simply a software, rather than hardware, memory barrier. 
            // On other platforms it may need a hardware memory barrier/fence, e.g. ARM.

            // From http://mentablog.soliveirajr.com/2012/12/asynchronous-logging-versus-memory-mapped-files/
            // The Java atomic variables (AtomicLong, AtomicInteger, etc.) have get() and set() methods that work as reads and writes on volatile variables with 
            // the additional feature of a semi-volatile write. When you change the value of a atomic variable through its lazySet() method, the value is written 
            // to the local cache of the core but not immediately to main memory. As a result, it will take an indefinite amount of time for the change to be 
            // flushed out to main memory so that other threads can see the new value.

            _counts[index] = value;
            // Is this right, is that all we need??? We definitely don't want an Interlocked here, that's too much!!
            //Thread.MemoryBarrier();
            Interlocked.MemoryBarrier();

            //Volatile.Read (only emits half-fence (acquire fence) as opposed to Thread.VolatileRead which emits a full-fence

            // From http://msdn.microsoft.com/en-us/magazine/jj863136.aspx
            // An operation that’s closely related to Interlocked methods is Thread.MemoryBarrier, which can be thought of as a dummy Interlocked operation. 
            // Just like an Interlocked method, Thread.Memory­Barrier can’t be reordered with any prior or subsequent memory operations. Unlike an Interlocked 
            // method, though, Thread.MemoryBarrier has no side effect; it simply constrains memory reorderings.
        }
    }
}