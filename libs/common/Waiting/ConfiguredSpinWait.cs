// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Spin-wait policy selected according to the cost of entering the caller's slow path.
    /// </summary>
    public enum SpinWaitPolicy : byte
    {
        /// <summary>
        /// Perform only CPU-local spins and stop before <see cref="SpinWait.SpinOnce()"/> would yield.
        /// </summary>
        CpuLocal = 1,

        /// <summary>
        /// Progress through <see cref="SpinWait"/>'s scheduler-yield behavior for a bounded number of iterations.
        /// </summary>
        BoundedProgressive = 2
    }

    /// <summary>
    /// Applies a declaratively selected <see cref="SpinWaitPolicy"/> before a caller enters its domain-specific slow path.
    /// </summary>
    public struct ConfiguredSpinWait
    {
        const int UseDefaultSleep1Threshold = int.MinValue;

        SpinWait spinner;
        readonly SpinWaitPolicy policy;
        readonly int maxIterations;
        readonly int sleep1Threshold;
        int iteration;

        ConfiguredSpinWait(SpinWaitPolicy policy, int maxIterations, int sleep1Threshold)
        {
            spinner = default;
            this.policy = policy;
            this.maxIterations = maxIterations;
            this.sleep1Threshold = sleep1Threshold;
            iteration = 0;
        }

        /// <summary>
        /// Gets the selected spin-wait policy.
        /// </summary>
        public readonly SpinWaitPolicy Policy => policy;

        /// <summary>
        /// Creates a wait that stops before <see cref="SpinWait.SpinOnce()"/> would yield.
        /// </summary>
        public static ConfiguredSpinWait CpuLocal()
            => new(SpinWaitPolicy.CpuLocal, 0, UseDefaultSleep1Threshold);

        /// <summary>
        /// Creates a bounded progressive wait using <see cref="SpinWait.SpinOnce()"/>'s default sleep threshold.
        /// </summary>
        /// <param name="maxIterations">Maximum number of progressive wait iterations.</param>
        public static ConfiguredSpinWait BoundedProgressive(int maxIterations)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxIterations);
            return new(SpinWaitPolicy.BoundedProgressive, maxIterations, UseDefaultSleep1Threshold);
        }

        /// <summary>
        /// Creates a bounded progressive wait using a caller-selected <see cref="SpinWait.SpinOnce(int)"/> sleep threshold.
        /// </summary>
        /// <param name="maxIterations">Maximum number of progressive wait iterations.</param>
        /// <param name="sleep1Threshold">
        /// Number of yielding iterations before <see cref="Thread.Sleep(int)"/> with a value of one is permitted,
        /// or -1 to disable it.
        /// </param>
        public static ConfiguredSpinWait BoundedProgressive(int maxIterations, int sleep1Threshold)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxIterations);
            ArgumentOutOfRangeException.ThrowIfLessThan(sleep1Threshold, -1);
            return new(SpinWaitPolicy.BoundedProgressive, maxIterations, sleep1Threshold);
        }

        /// <summary>
        /// Performs one policy-approved wait step.
        /// </summary>
        /// <returns>True if a wait step was performed; false when the caller should enter its slow path.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWait()
        {
            switch (policy)
            {
                case SpinWaitPolicy.CpuLocal:
                    if (spinner.NextSpinWillYield)
                        return false;
                    spinner.SpinOnce();
                    return true;

                case SpinWaitPolicy.BoundedProgressive:
                    if (iteration++ >= maxIterations)
                        return false;
                    if (sleep1Threshold == UseDefaultSleep1Threshold)
                        spinner.SpinOnce();
                    else
                        spinner.SpinOnce(sleep1Threshold);
                    return true;

                default:
                    throw new InvalidOperationException($"{nameof(ConfiguredSpinWait)} must be created using a policy factory.");
            }
        }
    }
}