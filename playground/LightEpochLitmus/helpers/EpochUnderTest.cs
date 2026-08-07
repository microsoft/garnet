// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Tsavorite.epoch.litmus
{
    /// <summary>
    /// The slice of the epoch API the litmus drives, so the same harness can be pointed at the
    /// fixed <see cref="LightEpoch"/> or at <see cref="BuggyLightEpoch"/>. Implementations are
    /// structs and the harness is generic over them so the calls stay direct: an interface
    /// indirection in the reader loop could hide the reordering being hunted.
    /// </summary>
    internal interface IEpochUnderTest : IDisposable
    {
        int EntryCount { get; }
        long TestHookAnnouncedEpochAt(int entry);
        void Resume();
        void Suspend();
        void ProtectAndDrain();
        void BumpCurrentEpoch(Action onDrain);
    }

    /// <summary>The epoch as this branch ships it: the slot claim CAS announces the epoch.</summary>
    internal readonly struct FixedEpoch : IEpochUnderTest
    {
        private readonly LightEpoch epoch = new();

        public FixedEpoch() { }

        public int EntryCount { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => epoch.EntryCount; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TestHookAnnouncedEpochAt(int entry) => epoch.TestHookAnnouncedEpochAt(entry);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume() => epoch.Resume();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Suspend() => epoch.Suspend();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProtectAndDrain() => epoch.ProtectAndDrain();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BumpCurrentEpoch(Action onDrain) => epoch.BumpCurrentEpoch(onDrain);

        public void Dispose() => epoch.Dispose();

        public override string ToString() => "fixed";
    }

    /// <summary>The epoch as it stands on main: the announce is a plain store behind the claim CAS.</summary>
    internal readonly struct BuggyEpoch : IEpochUnderTest
    {
        private readonly BuggyLightEpoch epoch = new();

        public BuggyEpoch() { }

        public int EntryCount { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => epoch.EntryCount; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TestHookAnnouncedEpochAt(int entry) => epoch.TestHookAnnouncedEpochAt(entry);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume() => epoch.Resume();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Suspend() => epoch.Suspend();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProtectAndDrain() => epoch.ProtectAndDrain();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BumpCurrentEpoch(Action onDrain) => epoch.BumpCurrentEpoch(onDrain);

        public void Dispose() => epoch.Dispose();

        public override string ToString() => "buggy";
    }
}