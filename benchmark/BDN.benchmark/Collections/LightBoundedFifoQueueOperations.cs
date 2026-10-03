// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.common;

namespace BDN.benchmark.Collections
{
    /// <summary>
    /// Measures single-threaded steady-state and saturated operations on <see cref="LightBoundedFifoQueue{T}"/>.
    /// </summary>
    [MemoryDiagnoser]
    public class LightBoundedFifoQueueOperations
    {
        const int OperationsPerBatch = 4096;
        const int PageCount = 4;
        const int MaxSpinCount = 10;

        sealed class QueueItem
        {
            internal readonly int Sequence;

            internal QueueItem(int sequence)
            {
                Sequence = sequence;
            }
        }

        LightBoundedFifoQueue<QueueItem> operationQueue;
        LightBoundedFifoQueue<QueueItem> saturatedQueue;
        QueueItem[] items;

        /// <summary>
        /// Number of queue entries in each ring page.
        /// </summary>
        [Params(8, 32, 128)]
        public int PageSize { get; set; }

        /// <summary>
        /// Creates the queues and preallocates all benchmark items.
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            operationQueue = new LightBoundedFifoQueue<QueueItem>(PageSize, PageCount, MaxSpinCount);
            saturatedQueue = new LightBoundedFifoQueue<QueueItem>(PageSize, PageCount, MaxSpinCount);
            items = new QueueItem[OperationsPerBatch];

            for (var i = 0; i < items.Length; i++)
                items[i] = new QueueItem(i + 1);

            for (var i = 0; i < saturatedQueue.Capacity; i++)
            {
                if (!saturatedQueue.TryEnqueue(items[i]))
                    throw new InvalidOperationException("Unable to prefill the saturated benchmark queue.");
            }
        }

        /// <summary>
        /// Disposes the benchmark queues.
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            operationQueue.Dispose();
            saturatedQueue.Dispose();
        }

        /// <summary>
        /// Measures an enqueue/dequeue round trip while the queue has available capacity.
        /// </summary>
        /// <returns>Checksum of the dequeued items.</returns>
        [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerBatch)]
        [BenchmarkCategory("SteadyState")]
        public long EnqueueDequeue()
        {
            long checksum = 0;

            for (var i = 0; i < OperationsPerBatch; i++)
            {
                if (!operationQueue.TryEnqueue(items[i]) ||
                    !operationQueue.TryDequeue(out var item))
                {
                    throw new InvalidOperationException("Unexpected queue backpressure in the steady-state benchmark.");
                }

                checksum += item.Sequence;
            }

            return checksum;
        }

        /// <summary>
        /// Measures a rejected enqueue after the queue has reached capacity.
        /// </summary>
        /// <returns>Number of unexpectedly accepted items.</returns>
        [Benchmark(OperationsPerInvoke = OperationsPerBatch)]
        [BenchmarkCategory("Saturated")]
        public int FullQueueEnqueueFailure()
        {
            var accepted = 0;

            for (var i = 0; i < OperationsPerBatch; i++)
                if (saturatedQueue.TryEnqueue(items[i]))
                    accepted++;

            return accepted;
        }
    }

    /// <summary>
    /// Page geometry for a queue with a fixed capacity of 128 entries.
    /// </summary>
    public enum LightBoundedFifoQueueShape
    {
        /// <summary>One page containing 128 entries.</summary>
        Page128x1,

        /// <summary>Two pages containing 64 entries each.</summary>
        Page64x2,

        /// <summary>Four pages containing 32 entries each.</summary>
        Page32x4,

        /// <summary>Eight pages containing 16 entries each.</summary>
        Page16x8,

        /// <summary>Sixteen pages containing 8 entries each.</summary>
        Page8x16,
    }

    /// <summary>
    /// Measures multi-producer publication with the benchmark thread acting as the queue's serialized consumer.
    /// </summary>
    [MemoryDiagnoser]
    [ThreadingDiagnoser]
    public class LightBoundedFifoQueueConcurrency
    {
        const int OperationsPerBatch = 65536;
        const int MaxSpinCount = 10;

        sealed class QueueItem
        {
            internal readonly int Sequence;

            internal QueueItem(int sequence)
            {
                Sequence = sequence;
            }
        }

        LightBoundedFifoQueue<QueueItem> queue;
        QueueItem[] items;
        Thread[] producers;
        ManualResetEventSlim[] startSignals;
        CountdownEvent producersDone;

        Exception producerException;
        int shutdown;
        int lifecycleErrors;

        /// <summary>
        /// Number of threads concurrently publishing into the queue.
        /// </summary>
        [Params(1, 2, 4, 8)]
        public int ProducerCount { get; set; }

        /// <summary>
        /// Page geometry used to provide 128 total queue entries.
        /// </summary>
        [Params(
            LightBoundedFifoQueueShape.Page128x1,
            LightBoundedFifoQueueShape.Page64x2,
            LightBoundedFifoQueueShape.Page32x4,
            LightBoundedFifoQueueShape.Page16x8,
            LightBoundedFifoQueueShape.Page8x16)]
        public LightBoundedFifoQueueShape Shape { get; set; }

        /// <summary>
        /// Creates persistent producer threads and preallocates all measured items.
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            if (OperationsPerBatch % ProducerCount != 0)
                throw new InvalidOperationException("The operation batch must divide evenly among producers.");

            var (pageSize, pageCount) = Shape switch
            {
                LightBoundedFifoQueueShape.Page128x1 => (128, 1),
                LightBoundedFifoQueueShape.Page64x2 => (64, 2),
                LightBoundedFifoQueueShape.Page32x4 => (32, 4),
                LightBoundedFifoQueueShape.Page16x8 => (16, 8),
                LightBoundedFifoQueueShape.Page8x16 => (8, 16),
                _ => throw new InvalidOperationException($"Unsupported queue shape: {Shape}."),
            };

            queue = new LightBoundedFifoQueue<QueueItem>(pageSize, pageCount, MaxSpinCount);
            items = new QueueItem[OperationsPerBatch];
            producers = new Thread[ProducerCount];
            startSignals = new ManualResetEventSlim[ProducerCount];
            producersDone = new CountdownEvent(ProducerCount);

            for (var i = 0; i < items.Length; i++)
                items[i] = new QueueItem(i + 1);

            for (var producerIndex = 0; producerIndex < ProducerCount; producerIndex++)
            {
                startSignals[producerIndex] = new ManualResetEventSlim(false);
                var capturedIndex = producerIndex;
                producers[producerIndex] = new Thread(() => RunProducer(capturedIndex))
                {
                    IsBackground = true,
                    Name = $"{nameof(LightBoundedFifoQueueConcurrency)}-{producerIndex}",
                };
                producers[producerIndex].Start();
            }
        }

        /// <summary>
        /// Stops the producer threads, validates the lifecycle, and releases resources.
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            Volatile.Write(ref shutdown, 1);
            foreach (var signal in startSignals)
                signal.Set();

            foreach (var producer in producers)
                producer.Join();

            foreach (var signal in startSignals)
                signal.Dispose();

            producersDone.Dispose();
            queue.Dispose();

            if (producerException != null)
                throw new InvalidOperationException("A benchmark producer failed.", producerException);
            if (lifecycleErrors != 0)
                throw new InvalidOperationException($"The queue benchmark observed {lifecycleErrors} lifecycle errors.");
        }

        /// <summary>
        /// Measures a fixed-size MPSC batch while the benchmark thread serially consumes published items.
        /// </summary>
        /// <returns>Checksum of all consumed items.</returns>
        [Benchmark(OperationsPerInvoke = OperationsPerBatch)]
        [BenchmarkCategory("Contended")]
        public long MultiProducerSingleConsumer()
        {
            producersDone.Reset(ProducerCount);
            foreach (var signal in startSignals)
                signal.Set();

            long checksum = 0;
            var consumed = 0;
            var spinner = new SpinWait();

            while (consumed < OperationsPerBatch)
            {
                if (queue.TryDequeue(out var item))
                {
                    checksum += item.Sequence;
                    consumed++;
                    spinner.Reset();
                    continue;
                }

                if (producersDone.IsSet && queue.Count == 0)
                {
                    Interlocked.Increment(ref lifecycleErrors);
                    break;
                }

                spinner.SpinOnce();
            }

            producersDone.Wait();

            if (queue.Count != 0 || checksum != ((long)OperationsPerBatch * (OperationsPerBatch + 1)) / 2)
                Interlocked.Increment(ref lifecycleErrors);

            return checksum;
        }

        void RunProducer(int producerIndex)
        {
            var operationsPerProducer = OperationsPerBatch / ProducerCount;
            var start = producerIndex * operationsPerProducer;
            var end = start + operationsPerProducer;

            while (true)
            {
                startSignals[producerIndex].Wait();
                startSignals[producerIndex].Reset();

                if (Volatile.Read(ref shutdown) != 0)
                    return;

                try
                {
                    var spinner = new SpinWait();
                    for (var i = start; i < end; i++)
                    {
                        while (!queue.TryEnqueue(items[i]))
                            spinner.SpinOnce();
                        spinner.Reset();
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.CompareExchange(ref producerException, ex, null);
                }
                finally
                {
                    producersDone.Signal();
                }
            }
        }
    }
}