// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.cluster
{
    [StructLayout(LayoutKind.Explicit, Size = 3)]
    internal struct ClusterSlotVerificationResult
    {
        [FieldOffset(0)]
        public SlotVerifiedState state;
        [FieldOffset(1)]
        public ushort slot;

        public ClusterSlotVerificationResult(SlotVerifiedState state, ushort slot)
        {
            this.state = state;
            this.slot = slot;
        }
    }
}