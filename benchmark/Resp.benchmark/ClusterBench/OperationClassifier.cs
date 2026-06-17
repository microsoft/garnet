// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// Classifies operations as read or write for replica routing decisions.
    /// Read operations can be routed to replicas; write operations must go to primaries.
    /// </summary>
    public static class OperationClassifier
    {
        /// <summary>
        /// Determines if an operation is a pure read (no side effects).
        /// Read operations can be routed to read-only replicas.
        /// </summary>
        public static bool IsReadOperation(OpType op)
        {
            return op switch
            {
                // String reads
                OpType.GET => true,
                OpType.MGET => true,
                
                // Bitmap reads
                OpType.GETBIT => true,
                OpType.BITCOUNT => true,
                OpType.BITPOS => true,
                
                // HyperLogLog reads
                OpType.PFCOUNT => true,
                
                // SortedSet reads
                OpType.ZCARD => true,
                
                // Metadata/utility reads
                OpType.PING => true,
                OpType.DBSIZE => true,
                OpType.READONLY => true,
                
                // Script reads (custom read-only scripts)
                OpType.SCRIPTGET => true,
                OpType.SCRIPTRETKEY => true,
                
                // All other operations are writes or have side effects
                _ => false
            };
        }

        /// <summary>
        /// Determines if an operation is a write or has side effects.
        /// Write operations must always be routed to primaries.
        /// </summary>
        public static bool IsWriteOperation(OpType op)
        {
            return op switch
            {
                // String writes
                OpType.SET => true,
                OpType.SETEX => true,
                OpType.MSET => true,
                OpType.INCR => true,
                OpType.DEL => true,
                
                // Bitmap writes
                OpType.SETBIT => true,
                OpType.BITOP_AND => true,
                OpType.BITOP_OR => true,
                OpType.BITOP_XOR => true,
                OpType.BITOP_NOT => true,
                OpType.BITFIELD => true,
                OpType.BITFIELD_SET => true,
                OpType.BITFIELD_INCR => true,
                
                // HyperLogLog writes
                OpType.PFADD => true,
                OpType.MPFADD => true,
                OpType.PFMERGE => true,
                
                // SortedSet writes
                OpType.ZADD => true,
                OpType.ZREM => true,
                OpType.ZADDREM => true,
                OpType.ZADDCARD => true,
                
                // Geo writes
                OpType.GEOADD => true,
                OpType.GEOADDREM => true,
                
                // Transactions (all have write potential or side effects)
                OpType.READ_TXN => true,
                OpType.WRITE_TXN => true,
                OpType.READWRITETX => true,
                OpType.WATCH_TXN => true,
                OpType.SAMPLEUPDATETX => true,
                OpType.SAMPLEDELETETX => true,
                
                // Scripts (assume write unless explicitly read-only)
                OpType.SCRIPTSET => true,
                
                // Pub/Sub (side effects - message delivery)
                OpType.PUBLISH => true,
                OpType.SPUBLISH => true,
                
                // Custom operations (assume write)
                OpType.SETIFPM => true,
                OpType.MYDICTSET => true,
                
                // Auth (side effect - session state change)
                OpType.AUTH => true,
                
                // Special cases
                OpType.NONE => false,
                
                // Bitfield get is read-only (even though parent BITFIELD is write)
                OpType.BITFIELD_GET => false,
                
                // Custom dict get is read-only
                OpType.MYDICTGET => false,
                
                // Already classified as read
                _ when IsReadOperation(op) => false,
                
                // Unknown operations default to write (safe default)
                _ => true
            };
        }

        /// <summary>
        /// Get the corresponding read operation for a write operation, if one exists.
        /// Used for --allow-replica-reads mode to map write-heavy workloads to replica-compatible reads.
        /// </summary>
        public static OpType? GetCorrespondingReadOperation(OpType writeOp)
        {
            return writeOp switch
            {
                // String operations
                OpType.SET => OpType.GET,
                OpType.SETEX => OpType.GET,
                OpType.MSET => OpType.MGET,
                
                // Bitmap operations
                OpType.SETBIT => OpType.GETBIT,
                
                // HyperLogLog operations
                OpType.PFADD => OpType.PFCOUNT,
                OpType.MPFADD => OpType.PFCOUNT,
                
                // SortedSet operations
                OpType.ZADD => OpType.ZCARD,
                OpType.ZADDREM => OpType.ZCARD,
                OpType.ZADDCARD => OpType.ZCARD,
                
                // Custom operations
                OpType.MYDICTSET => OpType.MYDICTGET,
                
                // No direct read equivalent for these operations
                OpType.DEL => null,
                OpType.INCR => null,
                OpType.ZREM => null,
                OpType.GEOADD => null,
                OpType.GEOADDREM => null,
                OpType.PFMERGE => null,
                OpType.BITOP_AND => null,
                OpType.BITOP_OR => null,
                OpType.BITOP_XOR => null,
                OpType.BITOP_NOT => null,
                OpType.BITFIELD => null,
                OpType.BITFIELD_SET => null,
                OpType.BITFIELD_INCR => null,
                
                // All other operations have no read equivalent
                _ => null
            };
        }
    }
}
