// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace MigrateBench
{
    public static class Common
    {
        public static bool Validate(int[] slots, ILogger logger = null)
        {
            if ((slots.Length & 0x1) > 0)
            {
                logger?.LogError("Malformed SLOTSRANGES input; please provide pair of ranges");
                return false;
            }
            return true;
        }

        public static List<int> GetSingleSlots(Options opts, ILogger logger = null)
        {
            var slots = opts.Slots.ToArray();
            if (!Validate(slots, logger))
                return null;

            var _slots = new List<int>();
            for (var i = 0; i < slots.Length; i += 2)
            {
                var startSlot = slots[i];
                var endSlot = slots[i + 1];
                for (var j = startSlot; j <= endSlot; j++)
                    _slots.Add(j);
            }

            return _slots;
        }
    }
}
