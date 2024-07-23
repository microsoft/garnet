using System;
using System.Collections.Concurrent;
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
        #region native_lib
        private const string spdk_library_name = "spdk_device";
        private const string spdk_library_path =
                               "runtimes/linux-x64/native/libspdk_device.so";

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

        [DllImport(spdk_library_name, EntryPoint = "begin_poller",
                   CallingConvention = CallingConvention.Cdecl)]
        static extern void begin_poller();

        [DllImport(spdk_library_name, EntryPoint = "spdk_device_poll",
                   CallingConvention = CallingConvention.Cdecl)]
        static extern int spdk_device_poll(uint timeout);

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

        static SPDKDevice()
        {
            NativeLibrary.SetDllImportResolver(typeof(SPDKDevice).Assembly,
                                               import_resolver);
            spdk_device_init();
            begin_poller();
        }
        #endregion


        private const int nsid = 1;
        private const uint sector_size = 4096;
        readonly ILogger logger;

        private int num_pending = 0;

        private ConcurrentQueue<SPDKIOContext> context_queue =
                                           new ConcurrentQueue<SPDKIOContext>();

        private delegate void AsyncIOCallback(IntPtr context, int result,
                                              ulong bytesTransferred);
        private AsyncIOCallback _callback_delegate;

        class SPDKIOContext
        {
            public readonly IntPtr native_spdk_device;
            private readonly GCHandle handle;
            public IntPtr spdk_context_ptr { get; private set; }
            public DeviceIOCompletionCallback tsavorite_callback
            {
                get; private set;
            }
            public object tsavorite_callback_context { get; private set; }

            public SPDKIOContext(IntPtr native_spdk_device)
            {
                this.native_spdk_device = native_spdk_device;
                this.handle = GCHandle.Alloc(this, GCHandleType.Normal);
                this.spdk_context_ptr = GCHandle.ToIntPtr(this.handle);
            }

            public void init_io(DeviceIOCompletionCallback tsavorite_callback,
                                object tsavorite_callback_context)
            {
                this.tsavorite_callback = tsavorite_callback;
                this.tsavorite_callback_context = tsavorite_callback_context;
            }

            public void finish_io()
            {
                this.tsavorite_callback = null;
                this.tsavorite_callback_context = null;
            }
        }

        void _callback(IntPtr context, int error_code, ulong num_bytes)
        {
            if (error_code < 0)
            {
                error_code = -error_code;
            }
            GCHandle handle = GCHandle.FromIntPtr(context);
            SPDKIOContext io_context = handle.Target as SPDKIOContext;
            DeviceIOCompletionCallback t_callback =
                                         io_context.tsavorite_callback;
            object t_context = io_context.tsavorite_callback_context;
            io_context.finish_io();
            Interlocked.Decrement(ref this.num_pending);
            this.context_queue.Enqueue(io_context);
            t_callback(
                (uint)error_code,
                (uint)num_bytes,
                t_context
            );
        }

        public SPDKDevice(string filename,
                          bool delete_on_close = true,
                          bool disable_file_buffering = true,
                          long capacity = Devices.CAPACITY_UNSPECIFIED,
                          ILogger logger = null)
                : base(filename, sector_size, capacity)
        {
            this._callback_delegate = this._callback;

            this.ThrottleLimit = 1024;

            for (int i = 0; i < 2; i++)
            {
                IntPtr spdk_device = spdk_device_create(SPDKDevice.nsid);
                this.context_queue.Enqueue(new SPDKIOContext(spdk_device));
            }
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
                SPDKIOContext spdk_io_context;
                while (!this.context_queue.TryDequeue(out spdk_io_context))
                {
                    Debug.WriteLine("Can't get spdk_device when reading");
                }
                spdk_io_context.init_io(callback, context);
                int _result = spdk_device_read_async(
                    spdk_io_context.native_spdk_device,
                    this.get_address(segment_id, source_address),
                    destination_address,
                    read_length,
                    this._callback_delegate,
                    spdk_io_context.spdk_context_ptr
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
                SPDKIOContext spdk_io_context;
                while (!this.context_queue.TryDequeue(out spdk_io_context))
                {
                    Debug.WriteLine("Can't get spdk_device when writing");
                }
                spdk_io_context.init_io(callback, context);
                int _result = spdk_device_write_async(
                    spdk_io_context.native_spdk_device,
                    source_address,
                    this.get_address(segment_id, destination_address),
                    write_length,
                    this._callback_delegate,
                    spdk_io_context.spdk_context_ptr
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

            // TODO: chyin join poller thread.

            // TODO: chyin call device destroy function.

        }
    }
}
