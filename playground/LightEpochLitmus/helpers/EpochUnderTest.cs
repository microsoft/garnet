// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Tsavorite.epoch.litmus
{
    /// <summary>
    /// Struct implementations keep the generic harness calls direct so interface dispatch
    /// does not hide the store-to-load race.
    /// </summary>
    internal interface IEpochUnderTest : IDisposable
    {
        int EntryCount { get; }
        long TestHookAnnouncedEpochAt(int entry);
        void Resume();
        void Suspend();
        void BumpCurrentEpoch(Action onDrain);
    }

    /// <summary>The actual epoch in the checked-out Tsavorite.core source.</summary>
    internal readonly struct ProductionEpoch : IEpochUnderTest
    {
        private readonly LightEpoch epoch = new();

        public ProductionEpoch() { }

        public int EntryCount { [MethodImpl(MethodImplOptions.AggressiveInlining)] get => LightEpoch.EntryCount; }

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