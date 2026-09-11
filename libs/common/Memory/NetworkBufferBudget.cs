// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Process-wide budget for the network buffers held by live connections.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The buffer pool's own <c>maxPooledBytes</c> ceiling bounds only the idle free list. Buffers
    /// checked out by live connections are bounded by nothing, so their footprint is
    /// <c>connections x per-connection size</c>. This type supplies the missing ceiling by publishing a
    /// <see cref="TargetBufferSize"/> that allocation sites use as the <em>base</em> size for a new buffer:
    /// <code>
    ///     target = clamp(floor, ceiling, PreviousPowerOf2(budget / liveBufferCount))
    /// </code>
    /// </para>
    /// <para>
    /// It is a division rather than a feedback control loop, which is deliberate. Buffer sizes move in
    /// factor-of-two steps, so any controller whose deadband is narrower than 2x cannot converge and will
    /// limit-cycle between two size classes. Sizing directly from the live buffer count has no equilibrium
    /// to hunt for, and it is agnostic to TLS, throttle depth and payload mix because those all show up
    /// directly in the count.
    /// </para>
    /// <para>
    /// Three properties keep the common case free of change. The target can only ever <em>lower</em> the
    /// base size, because it is clamped to the configured buffer size as a ceiling. Demand-driven growth is
    /// never clamped, so a connection that needs a large buffer still gets one. And adaptation only engages
    /// once <c>budget / liveBufferCount</c> falls below the configured size, which at the default settings
    /// is several thousand connections; below that every comparison is arithmetically inert.
    /// </para>
    /// </remarks>
    public sealed class NetworkBufferBudget
    {
        /// <summary>
        /// Total bytes the live buffers should collectively stay within. Zero disables adaptation.
        /// </summary>
        readonly long budgetBytes;

        /// <summary>
        /// The configured base buffer size. The target is clamped to this, so adaptation can only shrink.
        /// </summary>
        readonly int ceiling;

        /// <summary>
        /// Smallest base size a receive buffer may be clamped to.
        /// </summary>
        readonly int receiveFloor;

        /// <summary>
        /// Smallest base size a send buffer may be clamped to. Higher than <see cref="receiveFloor"/>
        /// because an undersized send buffer pushes oversized responses onto the pooled-rental path.
        /// </summary>
        readonly int sendFloor;

        /// <summary>
        /// Outstanding buffers checked out of the budgeted pools. This rides exactly the two paths that
        /// maintain the pool's live byte accounting, and nothing else.
        /// </summary>
        long liveBufferCount;

        /// <summary>
        /// Published base size, read by allocation sites. Advisory: a stale read costs at most one
        /// buffer allocated at the previous size.
        /// </summary>
        int targetBufferSize;

        long pressureShrinks;
        long idleShrinks;

        /// <summary>
        /// Whether adaptation is enabled. When false the budget is inert and behaviour is exactly as it
        /// was before the budget existed.
        /// </summary>
        public bool IsEnabled => budgetBytes > 0;

        /// <summary>
        /// Whether the budget is actually binding, i.e. the published target has been driven below the
        /// configured size by the number of live buffers. This is the pressure signal the shrink policy
        /// gates on: it reads an already-published value rather than a contended byte counter, so it costs
        /// one predictable branch on the receive path.
        /// </summary>
        public bool IsUnderPressure => IsEnabled && TargetBufferSize < ceiling;

        /// <summary>
        /// Configured budget in bytes. Zero when disabled.
        /// </summary>
        public long BudgetBytes => budgetBytes;

        /// <summary>
        /// Current published base size for a new buffer, before the per-site floor is applied.
        /// </summary>
        public int TargetBufferSize => Volatile.Read(ref targetBufferSize);

        /// <summary>
        /// Base size for a new receive buffer.
        /// </summary>
        public int TargetReceiveBufferSize => Math.Max(receiveFloor, TargetBufferSize);

        /// <summary>
        /// Base size for a new send buffer.
        /// </summary>
        public int TargetSendBufferSize => Math.Max(sendFloor, TargetBufferSize);

        /// <summary>
        /// Outstanding buffers checked out of the budgeted pools.
        /// </summary>
        public long LiveBufferCount => Interlocked.Read(ref liveBufferCount);

        /// <summary>
        /// Number of times a buffer was shrunk because the budget was under pressure.
        /// </summary>
        public long PressureShrinks => Interlocked.Read(ref pressureShrinks);

        /// <summary>
        /// Number of times a buffer was shrunk after a quiet stretch rather than under pressure.
        /// </summary>
        public long IdleShrinks => Interlocked.Read(ref idleShrinks);

        /// <summary>
        /// A disabled budget, for pools that are not connection-scaled.
        /// </summary>
        public static NetworkBufferBudget Disabled { get; } = new NetworkBufferBudget(0, 1 << 17, 1 << 14, 1 << 16);

        /// <summary>
        /// Create a budget.
        /// </summary>
        /// <param name="budgetBytes">Total bytes live buffers should stay within. Zero disables adaptation.</param>
        /// <param name="ceiling">Configured base buffer size; the target never exceeds this.</param>
        /// <param name="receiveFloor">Smallest base size for a receive buffer.</param>
        /// <param name="sendFloor">Smallest base size for a send buffer.</param>
        public NetworkBufferBudget(long budgetBytes, int ceiling, int receiveFloor, int sendFloor)
        {
            Debug.Assert(BitOperations.IsPow2(ceiling));
            Debug.Assert(BitOperations.IsPow2(receiveFloor));
            Debug.Assert(BitOperations.IsPow2(sendFloor));

            this.budgetBytes = budgetBytes > 0 ? budgetBytes : 0;
            this.ceiling = ceiling;
            // A floor above the configured size would raise the base size, which adaptation must never do.
            this.receiveFloor = Math.Min(receiveFloor, ceiling);
            this.sendFloor = Math.Min(sendFloor, ceiling);
            this.targetBufferSize = ceiling;
        }

        /// <summary>
        /// Account for a buffer being checked out. Inert when the budget is disabled, so pools that do not
        /// participate cannot accumulate a meaningless count on the shared <see cref="Disabled"/> instance.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnBufferAcquired()
        {
            if (budgetBytes == 0)
                return;
            Interlocked.Increment(ref liveBufferCount);
        }

        /// <summary>
        /// Account for a buffer being handed back, whether it was pooled or dropped.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnBufferReleased()
        {
            if (budgetBytes == 0)
                return;
            Interlocked.Decrement(ref liveBufferCount);
        }

        /// <summary>
        /// Record that a buffer was shrunk because the budget was under pressure.
        /// </summary>
        public void RecordPressureShrink() => Interlocked.Increment(ref pressureShrinks);

        /// <summary>
        /// Record that a buffer was shrunk after a quiet stretch.
        /// </summary>
        public void RecordIdleShrink() => Interlocked.Increment(ref idleShrinks);

        /// <summary>
        /// Recompute and publish <see cref="TargetBufferSize"/>. Called from slow paths only: connection
        /// admission and release, and the pool's allocate-miss path.
        /// </summary>
        public void Recompute()
        {
            if (budgetBytes == 0)
                return;

            var count = Interlocked.Read(ref liveBufferCount);
            var raw = budgetBytes / Math.Max(1, count);

            var current = Volatile.Read(ref targetBufferSize);

            // Hysteresis must be at least as wide as the actuation step, which is 2x. Shrink as soon as the
            // quotient falls below the published target; grow only once it reaches twice that. A narrower
            // band would oscillate between adjacent size classes indefinitely.
            if (raw >= current && raw < 2L * current)
                return;

            var next = ComputeTarget(raw);
            if (next == current)
                return;

            _ = Interlocked.CompareExchange(ref targetBufferSize, next, current);
        }

        /// <summary>
        /// Largest permitted base size for the given per-buffer byte quotient.
        /// </summary>
        int ComputeTarget(long raw)
        {
            if (raw >= ceiling)
                return ceiling;
            if (raw <= receiveFloor)
                return receiveFloor;

            // Round down to a size class the pool can actually recycle.
            var rounded = 1 << (BitOperations.Log2((ulong)raw));
            return Math.Clamp(rounded, receiveFloor, ceiling);
        }

        /// <summary>
        /// Target that would be published for a given live buffer count, without touching shared state or
        /// applying hysteresis. Pure function of the configuration, so the sizing policy can be examined
        /// and tested without sockets.
        /// </summary>
        /// <param name="count">Hypothetical live buffer count.</param>
        public int TargetForCount(long count)
            => budgetBytes == 0 ? ceiling : ComputeTarget(budgetBytes / Math.Max(1, count));

        /// <summary>
        /// Stats fragment for INFO.
        /// </summary>
        public string GetStats()
            => $"budgetBytes={Format.MemoryBytes(budgetBytes)}," +
               $"targetBufferSize={Format.MemoryBytes(TargetBufferSize)}," +
               $"liveBufferCount={LiveBufferCount}," +
               $"pressureShrinks={PressureShrinks}," +
               $"idleShrinks={IdleShrinks}";
    }
}