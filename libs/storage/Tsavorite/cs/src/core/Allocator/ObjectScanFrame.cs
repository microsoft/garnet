// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.ExceptionServices;

namespace Tsavorite.core
{
    /// <summary>Tracks the objects deserialized into one scan frame, including records the iterator never visits.</summary>
    internal sealed class ObjectScanFrame(ObjectIdMap objectIdMap)
    {
        internal readonly ObjectIdMap objectIdMap = objectIdMap;
        private long startAddress;
        internal long initializedUntilAddress;

        internal void Initialize(long pageStartPhysicalAddress)
        {
            startAddress = initializedUntilAddress = pageStartPhysicalAddress + PageHeader.Size;
        }

        /// <summary>
        /// Release a completed frame before reuse or after all reads have drained. Each frame has only one
        /// load in flight; its initialized extent excludes unread records whose fields still contain disk lengths.
        /// Visited records have already been converted to inline by DiskLogRecord.Dispose. Free only this
        /// frame's slots: other frames may concurrently deserialize into the iterator's shared object map.
        /// </summary>
        internal void Clear<TStoreFunctions>(TStoreFunctions storeFunctions)
            where TStoreFunctions : IStoreFunctions
        {
            ExceptionDispatchInfo disposalError = null;
            try
            {
                while (startAddress < initializedUntilAddress)
                {
                    var logRecord = new LogRecord(startAddress, objectIdMap);
                    startAddress += logRecord.AllocatedSize;
                    if (!logRecord.Info.Valid || !logRecord.DataHeader.RecordHasObjects)
                        continue;

                    var diskRecord = new DiskLogRecord(in logRecord);
                    try
                    {
                        // Unvisited objects also need disk-read initialization before external resources can be released.
                        if (storeFunctions.CallOnDiskRead)
                            storeFunctions.OnDiskRead(ref diskRecord.logRecord);
                        storeFunctions.OnDisposeDiskRecord(ref diskRecord, DisposeReason.DeserializedFromDisk);
                    }
                    catch (Exception ex)
                    {
                        // Finish releasing the other owned objects, then surface the first callback failure.
                        disposalError ??= ExceptionDispatchInfo.Capture(ex);
                    }
                    finally
                    {
                        diskRecord.Dispose();
                    }
                }
            }
            finally
            {
                startAddress = initializedUntilAddress = 0;
            }
            disposalError?.Throw();
        }
    }
}