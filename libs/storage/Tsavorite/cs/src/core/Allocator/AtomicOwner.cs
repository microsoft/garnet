// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit)]
    struct AtomicOwner
    {
        [FieldOffset(0)]
        int owner;
        [FieldOffset(4)]
        int count;
        [FieldOffset(0)]
        long atomic;

        /// <summary>
        /// Enqueue token
        /// true: success + caller is new owner
        /// false: success + someone else is owner
        /// </summary>
        /// <returns></returns>
        public bool Enqueue()
        {
            while (true)
            {
                var older = this;
                var newer = older;
                newer.count++;
                if (older.owner == 0)
                    newer.owner = 1;

                if (Interlocked.CompareExchange(ref atomic, newer.atomic, older.atomic) == older.atomic)
                {
                    return older.owner == 0;
                }
            }
        }

        /// <summary>
        /// Dequeue token (caller is/remains owner)
        /// true: successful dequeue
        /// false: failed dequeue
        /// </summary>
        /// <returns></returns>
        public bool Dequeue()
        {
            while (true)
            {
                var older = this;
                var newer = older;
                newer.count--;

                if (Interlocked.CompareExchange(ref atomic, newer.atomic, older.atomic) == older.atomic)
                {
                    return newer.count > 0;
                }
            }
        }

        /// <summary>
        /// Release queue ownership
        /// true: successful release
        /// false: failed release
        /// </summary>
        /// <returns></returns>
        public bool Release()
        {
            while (true)
            {
                var older = this;
                var newer = older;

                if (newer.count > 0)
                    return false;

                if (newer.owner == 0)
                    throw new TsavoriteException("Invalid release by non-owner thread");
                newer.owner = 0;

                if (Interlocked.CompareExchange(ref atomic, newer.atomic, older.atomic) == older.atomic)
                {
                    return true;
                }
            }
        }
    }
}