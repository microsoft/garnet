// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.epoch.litmus.ReorderedRelease
{
    public sealed unsafe partial class LightEpoch
    {
        internal long TestHookAnnouncedEpochAt(int entry) => (*(tableAligned + entry)).localCurrentEpoch;
    }
}

namespace Tsavorite.epoch.litmus
{
    /// <summary>The detector control reads drainCount before clearing the slot in Suspend().</summary>
    internal readonly struct ReorderedReleaseEpoch : IEpochUnderTest
    {
        private readonly ReorderedRelease.LightEpoch epoch = new();

        public ReorderedReleaseEpoch() { }

        public int EntryCount { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => ReorderedRelease.LightEpoch.EntryCount; }

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