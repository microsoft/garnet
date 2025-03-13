using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Garnet.server.KeyspaceNotifications
{
    /// <summary>
    /// Keyspace Notification strings
    /// </summary>
    static partial class KeyspaceNotificationStrings
    {
        public static ReadOnlySpan<byte> KeyspacePrefix => "__keyspace@"u8;
        public static ReadOnlySpan<byte> KeyeventPrefix => "__keyevent@"u8;
        public static ReadOnlySpan<byte> Suffix => "__:"u8;
    }
}
