// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.epoch.litmus.VolatileRelease
{
    public sealed unsafe partial class LightEpoch
    {
        internal long TestHookAnnouncedEpochAt(int entry) => (*(tableAligned + entry)).localCurrentEpoch;
    }
}

namespace Tsavorite.epoch.litmus
{
    /// <summary>Production-derived epoch with a volatile-only slot clear in Release().</summary>
    internal readonly struct VolatileReleaseEpoch : IEpochUnderTest
    {
        private readonly VolatileRelease.LightEpoch epoch = new();

        public VolatileReleaseEpoch() { }

        public int EntryCount { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => VolatileRelease.LightEpoch.EntryCount; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TestHookAnnouncedEpochAt(int entry) => epoch.TestHookAnnouncedEpochAt(entry);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume() => epoch.Resume();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Suspend() => epoch.Suspend();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BumpCurrentEpoch(Action onDrain) => epoch.BumpCurrentEpoch(onDrain);

        public void Dispose() => epoch.Dispose();
    }
}