using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.server.KeyspaceNotifications;

namespace Garnet.server
{
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        public void PublishKeyspaceNotification(KeyspaceNotificationType keyspaceNotificationType, ref ArgSlice argSlice) 
        {
            if (!storeWrapper.serverOptions.AllowedKeyspaceNotifications.HasFlag(keyspaceNotificationType))
            {
                return;
            }

            if (storeWrapper.serverOptions.AllowedKeyspaceNotifications.HasFlag(KeyspaceNotificationType.Keyspace))
            {
                PublishKeyspace(ref argSlice);
            }

            if (storeWrapper.serverOptions.AllowedKeyspaceNotifications.HasFlag(KeyspaceNotificationType.Keyevent))
            {
                PublishKeyevent(ref argSlice);
            }
        }

        public void PublishKeyevent(ref ArgSlice argSlice)
        {
            Publish(ArgSlice.FromPinnedSpan(KeyspaceNotificationStrings.KeyspacePrefix), argSlice);
        }

        public void PublishKeyspace(ref ArgSlice argSlice)
        {
            Publish(ArgSlice.FromPinnedSpan(KeyspaceNotificationStrings.KeyeventPrefix), argSlice);
        }
    }
}
