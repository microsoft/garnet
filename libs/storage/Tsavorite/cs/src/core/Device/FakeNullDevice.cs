using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Tsavorite.core;

public class FakeNullDevice : StorageDeviceBase
{
    class IOContext
    {
        public DeviceIOCompletionCallback callback;
        public object context;
        public uint io_length;
    }

    private ConcurrentQueue<IOContext> io_queue = new ConcurrentQueue<IOContext>();
    private Thread io_completion_worker = null;
    private readonly CancellationTokenSource completion_cancellation_token;

    public FakeNullDevice() : base("null", 512, Devices.CAPACITY_UNSPECIFIED)
    {
        this.completion_cancellation_token = new CancellationTokenSource();
        this.io_completion_worker = new Thread(io_completion);
        this.io_completion_worker.Name = "io_completion_worker";
        this.io_completion_worker.Start();
    }

    public override void Dispose()
    {
        this.completion_cancellation_token.Cancel();
        this.io_completion_worker.Join();
        this.completion_cancellation_token.Dispose();
    }

    private void io_completion()
    {
        while (!this.completion_cancellation_token.IsCancellationRequested)
        {

            IOContext io_context;
            while (this.io_queue.TryDequeue(out io_context))
            {
                io_context.callback(0, io_context.io_length, io_context.context);
            }
        }
    }

    public override unsafe void ReadAsync(int segmentId, ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context)
    {
        IOContext io_context = new IOContext();
        io_context.callback = callback;
        io_context.io_length = aligned_read_length;
        io_context.context = context;
        this.io_queue.Enqueue(io_context);
    }

    public override unsafe void WriteAsync(IntPtr alignedSourceAddress, int segmentId, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
    {
        IOContext io_context = new IOContext();
        io_context.callback = callback;
        io_context.io_length = numBytesToWrite;
        io_context.context = context;
        this.io_queue.Enqueue(io_context);
    }

    public override void RemoveSegment(int segment)
    {
        // No-op
    }

    public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result) => callback(result);
}
