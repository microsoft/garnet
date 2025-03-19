using System;
using Garnet.server.KeyspaceNotifications;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        // TODO: use same parameter for key and keyevent; pass keyevent as ref; test performance; avoid concatSpans and remove concatenated Spans; check access modifiers
        internal void PublishKeyspaceNotification(KeyspaceNotificationType keyspaceNotificationType, ref ArgSlice key, ReadOnlySpan<byte> keyevent) 
        {
            if (!storeWrapper.serverOptions.AllowedKeyspaceNotifications.HasFlag(keyspaceNotificationType))
            {
                return;
            }

            var channel = ConcatSpans(KeyspaceNotificationStrings.KeyspacePrefix, KeyspaceNotificationStrings.Suffix);

            if (storeWrapper.serverOptions.AllowedKeyspaceNotifications.HasFlag(KeyspaceNotificationType.Keyspace))
            {
                var keyspaceChannel = ConcatSpans(channel, key.ReadOnlySpan);
                PublishKeyspace(ref keyspaceChannel, keyevent);
            }

            if (storeWrapper.serverOptions.AllowedKeyspaceNotifications.HasFlag(KeyspaceNotificationType.Keyevent))
            {
                var keyeventChannel = ConcatSpans(channel, keyevent);
                PublishKeyevent(ref keyeventChannel, ref key);
            } 
        }
        private void PublishKeyspace(ref ReadOnlySpan<byte> channel, ReadOnlySpan<byte> keyevent)
        {
            // TODO: find a better solution for the string concatenation and converting to ArgSlice
            subscribeBroker.Publish(ArgSlice.FromPinnedSpan(channel), ArgSlice.FromPinnedSpan(keyevent));
        }

        private void PublishKeyevent(ref ReadOnlySpan<byte> channel, ref ArgSlice key)
        {
            // TODO: see above
            subscribeBroker.Publish(ArgSlice.FromPinnedSpan(channel), key);
        }

        private static ReadOnlySpan<byte> ConcatSpans(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        {
            byte[] combined = new byte[first.Length + second.Length];
            first.CopyTo(combined);
            second.CopyTo(combined.AsSpan(first.Length));
            return combined;
        }
    }
}
