using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Garnet.server.KeyspaceNotifications
{
    /// <summary>
    /// Represents different types of notifications that can be received.
    /// </summary>
    [Flags]
    public enum KeyspaceNotificationType
    {
        /// <summary>Keyspace events, published with __keyspace@&lt;db&gt;__ prefix.</summary>
        Keyspace = 1 << 0,    // K

        /// <summary>Keyevent events, published with __keyevent@&lt;db&gt;__ prefix.</summary>
        KeyEvent = 1 << 1,    // E

        /// <summary>Generic commands (non-type specific) like DEL, EXPIRE, RENAME, etc.</summary>
        Generic = 1 << 2,     // g

        /// <summary>String commands.</summary>
        String = 1 << 3,      // $

        /// <summary>List commands.</summary>
        List = 1 << 4,        // l

        /// <summary>Set commands.</summary>
        Set = 1 << 5,         // s

        /// <summary>Hash commands.</summary>
        Hash = 1 << 6,        // h

        /// <summary>Sorted set commands.</summary>
        ZSet = 1 << 7,        // z

        /// <summary>Expired events (events generated every time a key expires).</summary>
        Expired = 1 << 8,     // x

        /// <summary>Evicted events (events generated when a key is evicted for maxmemory).</summary>
        Evicted = 1 << 9,     // e

        /// <summary>Stream commands.</summary>
        Stream = 1 << 10,     // t

        /// <summary>Key miss events (events generated when a key that doesn't exist is accessed).</summary>
        KeyMiss = 1 << 11,    // m (Excluded from NOTIFY_ALL)

        /// <summary>Module-only key space notification, indicating a key loaded from RDB.</summary>
        Loaded = 1 << 12,     // module only key space notification

        /// <summary>Module key type events.</summary>
        Module = 1 << 13,     // d, module key space notification

        /// <summary>New key events (Note: not included in the 'A' class).</summary>
        New = 1 << 14,        // n, new key notification

        /// <summary>
        /// Alias for "g$lshztxed", meaning all events except "m" (KeyMiss) and "n" (New).
        /// </summary>
        All = Generic | String | List | Set | Hash | ZSet | Expired | Evicted | Stream | Module // A flag
    }
}
