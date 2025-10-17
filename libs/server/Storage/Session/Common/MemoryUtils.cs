// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Utility class for memory related operations.
    /// </summary>
    public static class MemoryUtils
    {
        /// <summary>.Net object overhead for byte arrays</summary>
        public const int ByteArrayOverhead = 24;

        /// <summary>.Net object overhead for list</summary>
        public const int ListOverhead = 40;

        /// <summary>.Net object avg. overhead for holding a list node entry</summary>
        public const int ListEntryOverhead = 48;

        /// <summary>.Net object overhead for sorted set</summary>
        public const int SortedSetOverhead = 48;

        /// <summary>.Net object avg. overhead for holding a sorted set entry</summary>
        public const int SortedSetEntryOverhead = 48;

        /// <summary>.Net object overhead for dictionary</summary>
        public const int DictionaryOverhead = 80;

        /// <summary>.Net object avg. overhead for holding a dictionary entry</summary>
        public const int DictionaryEntryOverhead = 64;

        /// <summary>.Net object overhead for hash set</summary>
        public const int HashSetOverhead = 64;

        /// <summary>.Net object avg. overhead for holding a hash set entry</summary>
        public const int HashSetEntryOverhead = 40;

        /// <summary>.Net object overhead for priority queue</summary>
        public const int PriorityQueueOverhead = 80;

        /// <summary>.Net object avg. overhead for holding a priority queue entry</summary>
        public const int PriorityQueueEntryOverhead = 48;

        internal static long CalculateHeapMemorySize(in LogRecord logRecord)
        {
            // For overflow byte[], round up key size to account for alignment during allocation and add overhead for allocating a byte array
            var result = 0L;
            if (logRecord.Info.KeyIsOverflow)
                result += Utility.RoundUp(logRecord.Key.Length, IntPtr.Size) + ByteArrayOverhead;
            if (logRecord.Info.ValueIsOverflow)
                result += Utility.RoundUp(logRecord.ValueSpan.Length, IntPtr.Size) + ByteArrayOverhead;
            else if (logRecord.Info.ValueIsObject)
                result += logRecord.ValueObject.HeapMemorySize;
            return result;
        }
    }
}