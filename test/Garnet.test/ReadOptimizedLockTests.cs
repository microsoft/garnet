// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Allure.NUnit;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class ReadOptimizedLockTests
    {
        [TestCase(123)]
        [TestCase(0)]
        [TestCase(1)]
        [TestCase(-1)]
        [TestCase(int.MaxValue)]
        [TestCase(int.MinValue)]
        public void BasicLocks(int hash)
        {
            var lockContext = new ReadOptimizedLock(16);

            var gotShared0 = lockContext.TryAcquireSharedLock(hash, out var sharedToken0);
            ClassicAssert.IsTrue(gotShared0);

            var gotShared1 = lockContext.TryAcquireSharedLock(hash, out var sharedToken1);
            ClassicAssert.IsTrue(gotShared1);

            var gotExclusive = lockContext.TryAcquireExclusiveLock(hash, out _);
            ClassicAssert.IsFalse(gotExclusive);

            lockContext.ReleaseSharedLock(sharedToken0);
            lockContext.ReleaseSharedLock(sharedToken1);

            var gotExclusiveAgain = lockContext.TryAcquireExclusiveLock(hash, out var exclusiveToken);
            ClassicAssert.IsTrue(gotExclusiveAgain);

            var gotSharedAgain = lockContext.TryAcquireSharedLock(hash, out _);
            ClassicAssert.IsFalse(gotSharedAgain);

            lockContext.ReleaseExclusiveLock(exclusiveToken);
        }

        [Test]
        public void IndexCalculations()
        {
            const int Iters = 10_000;

            var lockContext = new ReadOptimizedLock(16);

            var rand = new Random(2025_11_17_00);

            var offsets = new HashSet<int>();

            for (var i = 0; i < Iters; i++)
            {
                offsets.Clear();

                // Bunch of random hashes, including negative ones, to prove reasonable calculations
                var hash = (int)rand.NextInt64();

                var hintBase = (int)rand.NextInt64();

                for (var j = 0; j < Environment.ProcessorCount; j++)
                {
                    var offset = lockContext.CalculateIndex(hash, hintBase + j);
                    ClassicAssert.True(offsets.Add(offset));
                }

                foreach (var offset in offsets)
                {
                    var tooClose = offsets.Except([offset]).Where(x => Math.Abs(x - offset) < ReadOptimizedLock.CacheLineSizeBytes / sizeof(int));
                    ClassicAssert.IsEmpty(tooClose);
                }
            }
        }

        [TestCase(1)]
        [TestCase(4)]
        [TestCase(16)]
        [TestCase(64)]
        [TestCase(128)]
        public void Threaded(int hashCount)
        {
            // Guard some number of distinct value "slots" (defined by hashes)
            // 
            // Runs threads which (randomly) either read values, write values, or read (then promote) and write.
            //
            // Reads check for correctness.
            // Writes are done "plain" with no other locking or coherency enforcement.

            const int Iters = 100_000;
            const int LongsPerSlot = 4;

            var lockContext = new ReadOptimizedLock(Math.Min(Math.Max(hashCount / 2, 1), Environment.ProcessorCount));

            var threads = new Thread[Math.Max(Environment.ProcessorCount, 4)];

            using var threadStart = new SemaphoreSlim(0, threads.Length);

            var globalRandom = new Random(2025_11_17_01);

            var hashes = new int[hashCount];
            for (var i = 0; i < hashes.Length; i++)
            {
                var nextHash = (int)globalRandom.NextInt64();
                if (hashes.AsSpan()[..i].Contains(nextHash))
                {
                    i--;
                    continue;
                }
                hashes[i] = nextHash;
            }

            var values = new long[hashes.Length][];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = new long[LongsPerSlot];
            }

            // Spin up a bunch of mutators
            for (var i = 0; i < threads.Length; i++)
            {
                var threadRandom = new Random(2025_11_17_01 + ((i + 1) * 100_000));

                threads[i] =
                    new(
                        () =>
                        {
                            threadStart.Wait();

                            for (var j = 0; j < Iters; j++)
                            {
                                var hashIx = threadRandom.Next(hashes.Length);
                                var hash = hashes[hashIx];

                                switch (threadRandom.Next(5))
                                {
                                    // Try: Read and verify
                                    case 0:
                                        {
                                            if (lockContext.TryAcquireSharedLock(hash, out var sharedLockToken))
                                            {
                                                var sub = values[hashIx];
                                                for (var k = 1; k < sub.Length; k++)
                                                {
                                                    ClassicAssert.AreEqual(sub[0], sub[k]);
                                                }

                                                lockContext.ReleaseSharedLock(sharedLockToken);
                                            }
                                            else
                                            {
                                                j--;
                                            }
                                        }
                                        break;

                                    // Try: Lock, modify
                                    case 1:
                                        {
                                            if (lockContext.TryAcquireExclusiveLock(hash, out var exclusiveLockToken))
                                            {
                                                var sub = values[hashIx];
                                                var newValue = threadRandom.NextInt64();
                                                for (var k = 0; k < sub.Length; k++)
                                                {
                                                    sub[k] = newValue;
                                                }

                                                lockContext.ReleaseExclusiveLock(exclusiveLockToken);
                                            }
                                            else
                                            {
                                                j--;
                                            }
                                        }
                                        break;

                                    // Demand: Read and verify
                                    case 2:
                                        {
                                            lockContext.AcquireSharedLock(hash, out var sharedLockToken);
                                            var sub = values[hashIx];
                                            for (var k = 1; k < sub.Length; k++)
                                            {
                                                ClassicAssert.AreEqual(sub[0], sub[k]);
                                            }

                                            lockContext.ReleaseSharedLock(sharedLockToken);
                                        }

                                        break;

                                    // Demand: Lock, modify
                                    case 3:
                                        {
                                            lockContext.AcquireExclusiveLock(hash, out var exclusiveLockToken);
                                            var sub = values[hashIx];
                                            var newValue = threadRandom.NextInt64();
                                            for (var k = 0; k < sub.Length; k++)
                                            {
                                                sub[k] = newValue;
                                            }

                                            lockContext.ReleaseExclusiveLock(exclusiveLockToken);
                                        }

                                        break;

                                    // Try: Read, verify, promote, modify
                                    case 4:
                                        {
                                            if (lockContext.TryAcquireSharedLock(hash, out var sharedLockToken))
                                            {
                                                var sub = values[hashIx];
                                                for (var k = 1; k < sub.Length; k++)
                                                {
                                                    ClassicAssert.AreEqual(sub[0], sub[k]);
                                                }

                                                if (lockContext.TryPromoteSharedLock(hash, sharedLockToken, out var exclusiveLockToken))
                                                {
                                                    var newValue = threadRandom.NextInt64();
                                                    for (var k = 0; k < sub.Length; k++)
                                                    {
                                                        sub[k] = newValue;
                                                    }

                                                    lockContext.ReleaseExclusiveLock(exclusiveLockToken);
                                                }
                                                else
                                                {
                                                    lockContext.ReleaseSharedLock(sharedLockToken);

                                                    j--;
                                                }
                                            }
                                            else
                                            {
                                                j--;
                                            }
                                        }

                                        break;

                                    // There is no Demand version of Promote because that is not safe in general

                                    default: throw new InvalidOperationException($"Unexpected op");
                                }
                            }
                        }
                    )
                    {
                        Name = $"{nameof(Threaded)} #{i}"
                    };
                threads[i].Start();
            }

            // Let threads run
            _ = threadStart.Release(threads.Length);

            // Wait for threads to finish
            foreach (var thread in threads)
            {
                thread.Join();
            }

            // Validate correctness of final state
            foreach (var vals in values)
            {
                for (var k = 1; k < vals.Length; k++)
                {
                    ClassicAssert.AreEqual(vals[0], vals[k]);
                }
            }
        }
    }
}