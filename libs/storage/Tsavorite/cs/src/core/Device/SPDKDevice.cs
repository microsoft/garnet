using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public class SPDKDevice : StorageDeviceBase
    {
        private const int nsid = 1;
        private const uint sector_size = 4096;
        readonly ILogger logger;

        private int num_pending = 0;
        private readonly CancellationTokenSource completion_cancellation_token;
        private readonly Thread completion_thread;

        private const string spdk_library_name = "spdk_device";
        private const string spdk_library_path =
                             "runtimes/linux-x64/native/libspdk_device.so";
        public delegate void AsyncIOCallback(IntPtr context, int result,
                                             ulong bytesTransferred);
        private ConcurrentQueue<IntPtr> spdk_device_queue =
                                          new ConcurrentQueue<IntPtr>();
        private AsyncIOCallback _callback_delegate;

        class ManagedCallback
        {
            public readonly IntPtr spdk_device;
            private DeviceIOCompletionCallback callback;
            private object context;

            public ManagedCallback(IntPtr spdk_device,
                                   DeviceIOCompletionCallback callback,
                                   object context)
            {
                this.spdk_device = spdk_device;
                this.callback = callback;
                this.context = context;
            }

            public void call(uint error_code, uint num_bytes)
            {
                this.callback(error_code, num_bytes, context);
            }
        }

        void _callback(IntPtr context, int error_code, ulong num_bytes)
        {
            Interlocked.Decrement(ref this.num_pending);
            if (error_code < 0)
            {
                error_code = -error_code;
            }
            GCHandle handle = GCHandle.FromIntPtr(context);
            ManagedCallback managed_callback =
                                (handle.Target as ManagedCallback);
            this.spdk_device_queue.Enqueue(managed_callback.spdk_device);
            managed_callback.call((uint)error_code, (uint)num_bytes);
            handle.Free();
        }

        public SPDKDevice(string filename, uint sectorSize, long capacity)
                 : base(filename, sectorSize, capacity)
        {
            NativeLibrary.SetDllImportResolver(typeof(SPDKDevice).Assembly,
                                               import_resolver);
        }

        static IntPtr import_resolver(string library_name, Assembly assembley,
                                      DllImportSearchPath? search_path)
        {
            if (library_name == spdk_library_name)
            {
                return NativeLibrary.Load(spdk_library_path);
            }
            else
            {
                return IntPtr.Zero;
            }
        }

        [DllImport(spdk_library_name, EntryPoint = "spdk_device_init",
                   CallingConvention = CallingConvention.Cdecl)]
        static extern int spdk_device_init();

        [DllImport(spdk_library_name, EntryPoint = "spdk_device_create",
                   CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr spdk_device_create(int nsid);

        [DllImport(spdk_library_name, EntryPoint = "spdk_device_read_async",
                   CallingConvention = CallingConvention.Cdecl)]
        static extern int spdk_device_read_async(IntPtr device, ulong source,
                                                 IntPtr dest, uint length,
                                                 AsyncIOCallback callback,
                                                 IntPtr context);

        [DllImport(spdk_library_name, EntryPoint = "spdk_device_write_async",
                   CallingConvention = CallingConvention.Cdecl)]
        static extern int spdk_device_write_async(IntPtr device, IntPtr source,
                                                  ulong dest, uint length,
                                                  AsyncIOCallback callback,
                                                  IntPtr context);

        [DllImport(spdk_library_name, EntryPoint = "spdk_device_poll",
                   CallingConvention = CallingConvention.Cdecl)]
        static extern int spdk_device_poll(uint timeout);

        public SPDKDevice(string filename,
                          bool delete_on_close = true,
                          bool disable_file_buffering = true,
                          long capacity = Devices.CAPACITY_UNSPECIFIED,
                          ILogger logger = null)
                : this(filename, sector_size, capacity)
        {
            this._callback_delegate = this._callback;
            this.ThrottleLimit = 1024;

            spdk_device_init();

            for (int i = 0; i < 5; i++)
            {
                this.spdk_device_queue.Enqueue(
                    spdk_device_create(SPDKDevice.nsid)
                );
            }

            this.completion_cancellation_token = new CancellationTokenSource();
            this.completion_thread = new Thread(this.completion_worker);
            this.completion_thread.Start();
        }

        public override bool Throttle() => this.num_pending > ThrottleLimit;

        private ulong get_address(int segment_id, ulong offset)
        {
            return ((ulong)segment_id << this.segmentSizeBits) | offset;
        }
        public override void ReadAsync(int segment_id, ulong source_address,
                                       IntPtr destination_address,
                                       uint read_length,
                                       DeviceIOCompletionCallback callback,
                                       object context)
        {
            if (Interlocked.Increment(ref this.num_pending) <= 0)
            {
                throw new Exception("Cannot operate on disposed device");
            }

            try
            {
                IntPtr spdk_device;
                while (!this.spdk_device_queue.TryDequeue(out spdk_device))
                {
                    Debug.WriteLine("Can't get spdk_device when reading");
                }
                GCHandle handle = GCHandle.Alloc(new ManagedCallback(spdk_device,
                                                                     callback,
                                                                     context),
                                                GCHandleType.Normal);
                int _result = spdk_device_read_async(
                    spdk_device,
                    this.get_address(segment_id, source_address),
                    destination_address,
                    read_length,
                    this._callback_delegate,
                    GCHandle.ToIntPtr(handle)
                );
                if (_result != 0)
                {
                    throw new IOException("Error reading from log file",
                                          _result);
                }
            }
            catch (IOException e)
            {
                Interlocked.Decrement(ref this.num_pending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
            }
            catch (Exception e)
            {
                Interlocked.Decrement(ref this.num_pending);
                callback((uint)e.HResult, 0, context);
            }
        }
        public override void WriteAsync(IntPtr source_address, int segment_id,
                                        ulong destination_address,
                                        uint write_length,
                                        DeviceIOCompletionCallback callback,
                                        object context)
        {
            if (Interlocked.Increment(ref this.num_pending) <= 0)
            {
                throw new Exception("Cannot operate on disposed device");
            }

            try
            {
                IntPtr spdk_device;
                while (!this.spdk_device_queue.TryDequeue(out spdk_device))
                {
                    Debug.WriteLine("Can't get spdk_device when writing");
                }
                GCHandle handle = GCHandle.Alloc(
                    new ManagedCallback(spdk_device, callback, context),
                    GCHandleType.Normal
                );
                int _result = spdk_device_write_async(
                    spdk_device,
                    source_address,
                    this.get_address(segment_id, destination_address),
                    write_length,
                    this._callback_delegate,
                    GCHandle.ToIntPtr(handle)
                );
                if (_result != 0)
                {
                    throw new IOException("Error writing to log file",
                                          _result);
                }
            }
            catch (IOException e)
            {
                Interlocked.Decrement(ref this.num_pending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
            }
            catch (Exception e)
            {
                Interlocked.Decrement(ref this.num_pending);
                callback((uint)e.HResult, 0, context);
            }
        }

        public override void RemoveSegment(int segment)
        {
            // TODO: chyin implement RemoveSegment
            return;
        }

        public override void RemoveSegmentAsync(int segment,
                                                AsyncCallback callback,
                                                IAsyncResult result)
        {
            callback(result);
            return;
        }

        // TODO: chyin add Rest()
        // TODO: chyin add TryComplete()
        // TODO: chyin add GetFileSize()

        public override void Dispose()
        {
            while (this.num_pending >= 0)
            {
                Interlocked.CompareExchange(ref this.num_pending, int.MinValue,
                                            0);
                Thread.Yield();
            }

            this.completion_cancellation_token.Cancel();
            this.completion_thread.Join();
            this.completion_cancellation_token.Dispose();

            // TODO: chyin call device destroy function.

        }

        void completion_worker()
        {
            while (true)
            {
                if (this.completion_cancellation_token.IsCancellationRequested)
                {
                    break;
                }
                spdk_device_poll(5000);
                Thread.Yield();
            }
        }
    }
}
