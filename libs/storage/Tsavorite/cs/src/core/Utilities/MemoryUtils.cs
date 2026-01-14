// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
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

        /// <summary>Calculate the heap memory size of this <see cref="LogRecord"/></summary>
        public static long CalculateHeapMemorySize<TSourceLogRecord>(in TSourceLogRecord logRecord)
            where TSourceLogRecord : ISourceLogRecord
            => logRecord.CalculateHeapMemorySize();
    }
}