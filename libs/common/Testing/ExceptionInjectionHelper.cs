// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Exception injection helper - used only in debug mode for testing
    /// </summary>
    public static class ExceptionInjectionHelper
    {
        static object @lock = new();
        static TaskCompletionSource<bool> update = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Non-zero while at least one <see cref="SuspendParking"/> scope is open. While it is non-zero
        /// <see cref="ResetAndWaitAsync"/> refuses to park, because nothing is left to signal it.
        /// </summary>
        static int parkingSuspensions;

        /// <summary>
        /// Array of exception injection types
        /// </summary>
        static readonly bool[] ExceptionInjectionTypes =
            Enum.GetValues<ExceptionInjectionType>().Select(_ => false).ToArray();

        /// <summary>
        /// Check if exception is enabled
        /// </summary>
        /// <param name="exceptionType"></param>
        /// <returns></returns>
        public static bool IsEnabled(ExceptionInjectionType exceptionType) => ExceptionInjectionTypes[(int)exceptionType];

        /// <summary>
        /// Enable exception scenario (NOTE: enable at beginning of test to trigger the exception at runtime)
        /// </summary>
        /// <param name="exceptionType"></param>
        [Conditional("DEBUG")]
        public static void EnableException(ExceptionInjectionType exceptionType)
        {
            if (exceptionType == ExceptionInjectionType.None)
            {
                return;
            }

            ExceptionInjectionTypes[(int)exceptionType] = true;

            TaskCompletionSource<bool> release;

            lock (@lock)
            {
                release = update;
                update = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            _ = release.TrySetResult(true);
        }

        /// <summary>
        /// Disable exception scenario (NOTE: for tests you need to always call disable at the end of the test to avoid breaking other tests in the line)
        /// </summary>
        /// <param name="exceptionType"></param>
        [Conditional("DEBUG")]
        public static void DisableException(ExceptionInjectionType exceptionType)
        {
            ExceptionInjectionTypes[(int)exceptionType] = false;
            TaskCompletionSource<bool> release;

            lock (@lock)
            {
                release = update;
                update = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            _ = release.TrySetResult(true);
        }

        /// <summary>
        /// Enables an exception scenario until the returned scope is disposed.
        /// </summary>
        /// <param name="exceptionType"></param>
        public static Scope EnabledScope(ExceptionInjectionType exceptionType)
        {
            EnableException(exceptionType);
            return new Scope(exceptionType);
        }

        /// <summary>
        /// Disables the exception scenario it was created for when disposed.
        /// </summary>
        /// <param name="exceptionType"></param>
        public readonly struct Scope(ExceptionInjectionType exceptionType) : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose() => DisableException(exceptionType);
        }

        /// <summary>
        /// Trigger exception scenario (NOTE: add this to the location where the exception should be emulated/triggered)
        /// </summary>
        /// <param name="exceptionType"></param>
        /// <exception cref="GarnetException"></exception>
        [Conditional("DEBUG")]
        public static void TriggerException(ExceptionInjectionType exceptionType)
        {
            if (exceptionType == ExceptionInjectionType.None)
            {
                return;
            }

            if (ExceptionInjectionTypes[(int)exceptionType])
                throw new GarnetException($"Exception injection triggered {exceptionType}");
        }

        /// <summary>
        /// Trigger condition and reset it
        /// </summary>
        /// <param name="exceptionType"></param>
        /// <returns></returns>
        public static bool TriggerCondition(ExceptionInjectionType exceptionType)
        {
#if DEBUG
            if (IsEnabled(exceptionType))
            {
                DisableException(exceptionType);
                return true;
            }
            return false;
#else
            return false;
#endif
        }

        /// <summary>
        /// Stops <see cref="ResetAndWaitAsync"/> from parking, and releases anyone already parked.
        ///
        /// A parked waiter is only released by <see cref="EnableException"/>, but the cleanup a test runs on
        /// its way out is <see cref="DisableException"/>. A test that leaves between a waiter arriving and
        /// being re-enabled - an assertion failing, or a wait for the arrival timing out - therefore strands
        /// that waiter permanently. The waiter is a server thread holding a pooled network buffer, so
        /// <c>LimitedFixedBufferPool.Dispose</c> then spins forever waiting for a reference that is never
        /// returned and the whole test process hangs rather than one test failing.
        ///
        /// Suspension covers arrivals as well as waiters already parked, so a request that reaches an
        /// injection point after its owner has begun shutting down cannot re-create the same hang.
        ///
        /// Every call must be paired with <see cref="ResumeParking"/>. The count keeps concurrent shutdowns
        /// independent, and ending every scope is what stops a suspension leaking into a later test sharing
        /// this process-wide state.
        /// </summary>
        [Conditional("DEBUG")]
        public static void SuspendParking()
        {
            TaskCompletionSource<bool> release;

            lock (@lock)
            {
                parkingSuspensions++;
                release = update;
                update = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _ = release.TrySetResult(true);
        }

        /// <summary>
        /// Ends a <see cref="SuspendParking"/> scope. Parking resumes once every scope has ended.
        /// </summary>
        [Conditional("DEBUG")]
        public static void ResumeParking()
        {
            lock (@lock)
            {
                Debug.Assert(parkingSuspensions > 0, "ResumeParking without a matching SuspendParking");
                parkingSuspensions--;
            }
        }

        /// <summary>
        /// Wait on set condition
        /// </summary>
        /// <param name="exceptionType"></param>
        /// <returns></returns>
        public static async Task ResetAndWaitAsync(ExceptionInjectionType exceptionType)
        {
            if (exceptionType == ExceptionInjectionType.None)
            {
                return;
            }

            if (IsEnabled(exceptionType))
            {
                // Reset and wait to be signaled to go forward
                DisableException(exceptionType);
                while (!IsEnabled(exceptionType))
                {
                    Task task;
                    lock (@lock)
                    {
                        // Parking is suspended while a server is shutting down, because whoever armed this
                        // injection point is gone and will never re-enable it. Reading it under the lock is
                        // what makes a suspension raised at any point before here take effect.
                        if (IsEnabled(exceptionType) || parkingSuspensions > 0)
                            break;
                        task = update.Task;
                    }
                    await task.ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Synchronous, event-driven counterpart to <see cref="ResetAndWaitAsync"/>: signals arrival by
        /// clearing the condition, then parks on the shared <see cref="TaskCompletionSource{TResult}"/>
        /// until it is re-enabled.
        /// </summary>
        /// <param name="exceptionType"></param>
        public static void ResetAndWait(ExceptionInjectionType exceptionType)
        {
            AsyncUtils.BlockingWait(ResetAndWaitAsync(exceptionType));
        }

        /// <summary>
        /// Wait on clear condition
        /// </summary>
        /// <param name="exceptionType"></param>
        /// <returns></returns>
        public static void WaitOnClear(ExceptionInjectionType exceptionType)
        {
            while (ExceptionInjectionTypes[(int)exceptionType])
                Thread.Yield();
        }

        /// <summary>
        /// Wait on clear condition
        /// </summary>
        /// <param name="exceptionType"></param>
        /// <returns></returns>
        public static async Task WaitOnClearAsync(ExceptionInjectionType exceptionType)
        {
            while (IsEnabled(exceptionType))
            {
                Task task;
                lock (@lock)
                {
                    if (!IsEnabled(exceptionType))
                        break;
                    task = update.Task;
                }
                await task.ConfigureAwait(false);
            }
        }
    }
}