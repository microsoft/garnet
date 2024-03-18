// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// The current state of a state-machine operation such as a checkpoint.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct VersionSchemeState
    {
        /// <summary>
        /// Special value denoting that the version state machine is at rest in stable state
        /// </summary>
        public const byte REST = 0;
        const int kTotalSizeInBytes = 8;
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Phase
        const int kPhaseBits = 8;
        const int kPhaseShiftInWord = kTotalBits - kPhaseBits;
        const long kPhaseMaskInWord = ((1L << kPhaseBits) - 1) << kPhaseShiftInWord;
        const long kPhaseMaskInInteger = (1L << kPhaseBits) - 1;

        // Version
        const int kVersionBits = kPhaseShiftInWord;
        const long kVersionMaskInWord = (1L << kVersionBits) - 1;

        /// <summary>Internal intermediate state of state machine</summary>
        private const byte kIntermediateMask = 128;

        [FieldOffset(0)] internal long Word;

        /// <summary>
        /// Custom phase marker denoting where in a state machine EPVS is in right now
        /// </summary>
        public byte Phase
        {
            get { return (byte)((Word >> kPhaseShiftInWord) & kPhaseMaskInInteger); }
            set
            {
                Word &= ~kPhaseMaskInWord;
                Word |= (((long)value) & kPhaseMaskInInteger) << kPhaseShiftInWord;
            }
        }

        /// <summary></summary>
        /// <returns>whether EPVS is in intermediate state now (transitioning between two states)</returns>
        public bool IsIntermediate() => (Phase & kIntermediateMask) != 0;

        /// <summary>
        /// Version number of the current state
        /// </summary>
        public long Version
        {
            get { return Word & kVersionMaskInWord; }
            set
            {
                Word &= ~kVersionMaskInWord;
                Word |= value & kVersionMaskInWord;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VersionSchemeState Copy(ref VersionSchemeState other)
        {
            var info = default(VersionSchemeState);
            info.Word = other.Word;
            return info;
        }

        /// <summary>
        /// Make a state with the given phase and version
        /// </summary>
        /// <param name="phase"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static VersionSchemeState Make(byte phase, long version)
        {
            var info = default(VersionSchemeState);
            info.Phase = phase;
            info.Version = version;
            return info;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VersionSchemeState MakeIntermediate(VersionSchemeState state)
            => Make((byte)(state.Phase | kIntermediateMask), state.Version);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void RemoveIntermediate(ref VersionSchemeState state)
        {
            state.Phase = (byte)(state.Phase & ~kIntermediateMask);
        }

        internal static bool Equal(VersionSchemeState s1, VersionSchemeState s2)
        {
            return s1.Word == s2.Word;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"[{Phase},{Version}]";
        }

        /// <summary>
        /// Compare the current <see cref="SystemState"/> to <paramref name="obj"/> for equality if obj is also a <see cref="SystemState"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            return obj is VersionSchemeState other && Equals(other);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return Word.GetHashCode();
        }

        /// <summary>
        /// Compare the current <see cref="SystemState"/> to <paramref name="other"/> for equality
        /// </summary>
        private bool Equals(VersionSchemeState other)
        {
            return Word == other.Word;
        }

        /// <summary>
        /// Equals
        /// </summary>
        public static bool operator ==(VersionSchemeState left, VersionSchemeState right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Not Equals
        /// </summary>
        public static bool operator !=(VersionSchemeState left, VersionSchemeState right)
        {
            return !(left == right);
        }
    }

    /// <summary>
    /// A version state machine specifies a sequence of transitions to a new version
    /// </summary>
    public abstract class VersionSchemeStateMachine
    {
        private long toVersion;
        /// <summary>
        /// The actual version this state machine is advancing to, or -1 if not yet determined
        /// </summary>
        protected internal long actualToVersion = -1;

        /// <summary>
        /// Constructs a new version state machine for transition to the given version
        /// </summary>
        /// <param name="toVersion"> version to transition to, or -1 if unconditionally transitioning to an unspecified next version</param>
        protected VersionSchemeStateMachine(long toVersion = -1)
        {
            this.toVersion = toVersion;
            actualToVersion = toVersion;
        }

        /// <summary></summary>
        /// <returns> version to transition to, or -1 if unconditionally transitioning to an unspecified next version</returns>
        public long ToVersion() => toVersion;

        /// <summary>
        /// Given the current state, compute the next state the version scheme should enter, if any.
        /// </summary>
        /// <param name="currentState"> the current state</param>
        /// <param name="nextState"> the next state, if any</param>
        /// <returns>whether a state transition is possible at this moment</returns>
        public abstract bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState);

        /// <summary>
        /// Code block to execute before entering a state. Guaranteed to execute in a critical section with mutual
        /// exclusion with other transitions or EPVS-protected code regions 
        /// </summary>
        /// <param name="fromState"> the current state </param>
        /// <param name="toState"> the state transitioning to </param>
        public abstract void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState);

        /// <summary>
        /// Code block to execute after entering the state. Execution here may interleave with other EPVS-protected
        /// code regions. This can be used to collaborative perform heavyweight transition work without blocking
        /// progress of other threads.
        /// </summary>
        /// <param name="state"> the current state </param>
        public abstract void AfterEnteringState(VersionSchemeState state);
    }

    internal class SimpleVersionSchemeStateMachine : VersionSchemeStateMachine
    {
        private Action<long, long> criticalSection;

        public SimpleVersionSchemeStateMachine(Action<long, long> criticalSection, long toVersion = -1) : base(toVersion)
        {
            this.criticalSection = criticalSection;
        }

        public override bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            Debug.Assert(currentState.Phase == VersionSchemeState.REST);
            nextState = VersionSchemeState.Make(VersionSchemeState.REST, ToVersion() == -1 ? currentState.Version + 1 : ToVersion());
            return true;
        }

        public override void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            Debug.Assert(fromState.Phase == VersionSchemeState.REST && toState.Phase == VersionSchemeState.REST);
            criticalSection(fromState.Version, toState.Version);
        }

        public override void AfterEnteringState(VersionSchemeState state) { }
    }

    /// <summary>
    /// Status for state machine execution
    /// </summary>
    public enum StateMachineExecutionStatus
    {
        /// <summary>
        /// execution successful
        /// </summary>
        OK,
        /// <summary>
        /// execution unsuccessful but may be retried
        /// </summary>
        RETRY,
        /// <summary>
        /// execution failed and should not be retried
        /// </summary>
        FAIL
    }

    /// <summary>
    /// Epoch Protected Version Scheme
    /// </summary>
    public class EpochProtectedVersionScheme
    {
        private LightEpoch epoch;
        private VersionSchemeState state;
        private VersionSchemeStateMachine currentMachine;

        /// <summary>
        /// Construct a new EPVS backed by the given epoch framework. Multiple EPVS instances can share an underlying
        /// epoch framework (WARNING: re-entrance is not yet supported, so nested protection of these shared instances
        /// likely leads to error)
        /// </summary>
        /// <param name="epoch"> The backing epoch protection framework </param>
        public EpochProtectedVersionScheme(LightEpoch epoch)
        {
            this.epoch = epoch;
            state = VersionSchemeState.Make(VersionSchemeState.REST, 1);
            currentMachine = null;
        }

        /// <summary></summary>
        /// <returns> the current state</returns>
        public VersionSchemeState CurrentState() => state;

        // Atomic transition from expectedState -> nextState
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MakeTransition(VersionSchemeState expectedState, VersionSchemeState nextState)
        {
            if (Interlocked.CompareExchange(ref state.Word, nextState.Word, expectedState.Word) != expectedState.Word)
                return false;
            Debug.WriteLine("Moved to {0}, {1}", nextState.Phase, nextState.Version);
            return true;
        }

        /// <summary>
        /// Enter protection on the current thread. During protection, no version transition is possible. For the system
        /// to make progress, protection must be later relinquished on the same thread using Leave() or Refresh()
        /// </summary>
        /// <returns> the state of the EPVS as of protection, which extends until the end of protection </returns>
        public VersionSchemeState Enter()
        {
            epoch.Resume();
            TryStepStateMachine();

            VersionSchemeState result;
            while (true)
            {
                result = state;
                if (!result.IsIntermediate()) break;
                epoch.Suspend();
                Thread.Yield();
                epoch.Resume();
            }

            return result;
        }

        /// <summary>
        /// Refreshes protection --- equivalent to dropping and immediately reacquiring protection, but more performant.
        /// </summary>
        /// <returns> the state of the EPVS as of protection, which extends until the end of protection</returns>
        public VersionSchemeState Refresh()
        {
            epoch.ProtectAndDrain();
            VersionSchemeState result = default;
            TryStepStateMachine();

            while (true)
            {
                result = state;
                if (!result.IsIntermediate()) break;
                epoch.Suspend();
                Thread.Yield();
                epoch.Resume();
            }
            return result;
        }

        /// <summary>
        /// Drop protection of the current thread
        /// </summary>
        public void Leave()
        {
            epoch.Suspend();
        }

        internal void TryStepStateMachine(VersionSchemeStateMachine expectedMachine = null)
        {
            var machineLocal = currentMachine;
            var oldState = state;

            // Nothing to step
            if (machineLocal == null) return;

            // Should exit to avoid stepping infinitely (until stack overflow)
            if (expectedMachine != null && machineLocal != expectedMachine) return;

            // Still computing actual to version
            if (machineLocal.actualToVersion == -1) return;

            // Machine finished, but not reset yet. Should reset and avoid starting another cycle
            if (oldState.Phase == VersionSchemeState.REST && oldState.Version == machineLocal.actualToVersion)
            {
                Interlocked.CompareExchange(ref currentMachine, null, machineLocal);
                return;
            }

            // Step is in progress or no step is available
            if (oldState.IsIntermediate() || !machineLocal.GetNextStep(oldState, out var nextState)) return;

            var intermediate = VersionSchemeState.MakeIntermediate(oldState);
            if (!MakeTransition(oldState, intermediate)) return;

            // Avoid upfront memory allocation by going to a function
            StepMachineHeavy(machineLocal, oldState, nextState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StepMachineHeavy(VersionSchemeStateMachine machineLocal, VersionSchemeState old, VersionSchemeState next)
        {
            // // Resume epoch to ensure that state machine is able to make progress
            // if this thread is the only active thread. Also, StepMachineHeavy calls BumpCurrentEpoch, which requires a protected thread.
            bool isProtected = epoch.ThisInstanceProtected();
            if (!isProtected)
                epoch.Resume();
            try
            {
                epoch.BumpCurrentEpoch(() =>
                {
                    machineLocal.OnEnteringState(old, next);
                    var success = MakeTransition(VersionSchemeState.MakeIntermediate(old), next);
                    machineLocal.AfterEnteringState(next);
                    Debug.Assert(success);
                    TryStepStateMachine(machineLocal);
                });
            }
            finally
            {
                if (!isProtected)
                    epoch.Suspend();
            }
        }

        /// <summary>
        /// Signals to EPVS that a new step is available in the state machine. This is useful when the state machine
        /// delays a step (e.g., while waiting on IO to complete) and invoked after the step is available, so the
        /// state machine can make progress even without active threads entering and leaving the system. There is no
        /// need to invoke this method if steps are always available. 
        /// </summary>
        public void SignalStepAvailable()
        {
            TryStepStateMachine();
        }

        /// <summary>
        /// Attempt to start executing the given state machine.
        /// </summary>
        /// <param name="stateMachine"> state machine to execute </param>
        /// <returns>
        /// whether the state machine is successfully started (OK),
        /// cannot be started due to an active state machine (RETRY),
        /// or cannot be started because the version has advanced past the target version specified (FAIL)
        /// </returns>
        public StateMachineExecutionStatus TryExecuteStateMachine(VersionSchemeStateMachine stateMachine)
        {
            if (stateMachine.ToVersion() != -1 && stateMachine.ToVersion() <= state.Version) return StateMachineExecutionStatus.FAIL;
            var actualStateMachine = Interlocked.CompareExchange(ref currentMachine, stateMachine, null);
            if (actualStateMachine == null)
            {
                // Compute the actual ToVersion of state machine
                stateMachine.actualToVersion =
                    stateMachine.ToVersion() == -1 ? state.Version + 1 : stateMachine.ToVersion();
                // Trigger one initial step to begin the process
                TryStepStateMachine(stateMachine);
                return StateMachineExecutionStatus.OK;
            }

            // Otherwise, need to check that we are not a duplicate attempt to increment version
            if (stateMachine.ToVersion() != -1 && actualStateMachine.actualToVersion >= stateMachine.ToVersion())
                return StateMachineExecutionStatus.FAIL;

            return StateMachineExecutionStatus.RETRY;
        }


        /// <summary>
        /// Start executing the given state machine
        /// </summary>
        /// <param name="stateMachine"> state machine to start </param>
        /// <param name="spin">whether to spin wait until version transition is complete</param>
        /// <returns> whether the state machine can be executed. If false, EPVS has advanced version past the target version specified </returns>
        public bool ExecuteStateMachine(VersionSchemeStateMachine stateMachine, bool spin = false)
        {
            if (epoch.ThisInstanceProtected())
                throw new InvalidOperationException("unsafe to execute a state machine blockingly when under protection");
            StateMachineExecutionStatus status;
            do
            {
                status = TryExecuteStateMachine(stateMachine);
            } while (status == StateMachineExecutionStatus.RETRY);

            if (status != StateMachineExecutionStatus.OK) return false;

            if (spin)
            {
                while (state.Version != stateMachine.actualToVersion || state.Phase != VersionSchemeState.REST)
                {
                    TryStepStateMachine();
                    Thread.Yield();
                }
            }

            return true;
        }

        /// <summary>
        /// Advance the version with a single critical section to the requested version. 
        /// </summary>
        /// <param name="criticalSection"> critical section to execute, with old version and new (target) version as arguments </param>
        /// <param name="targetVersion">version to transition to, or -1 if unconditionally transitioning to an unspecified next version</param>
        /// <returns>
        /// whether the state machine is successfully started (OK),
        /// cannot be started due to an active state machine (RETRY),
        /// or cannot be started because the version has advanced past the target version specified (FAIL)
        /// </returns>
        public StateMachineExecutionStatus TryAdvanceVersionWithCriticalSection(Action<long, long> criticalSection, long targetVersion = -1)
        {
            return TryExecuteStateMachine(new SimpleVersionSchemeStateMachine(criticalSection, targetVersion));
        }

        /// <summary>
        /// Advance the version with a single critical section to the requested version. 
        /// </summary>
        /// <param name="criticalSection"> critical section to execute, with old version and new (target) version as arguments </param>
        /// <param name="targetVersion">version to transition to, or -1 if unconditionally transitioning to an unspecified next version</param>
        /// <param name="spin">whether to spin wait until version transition is complete</param>
        /// <returns> whether the state machine can be executed. If false, EPVS has advanced version past the target version specified </returns>
        public bool AdvanceVersionWithCriticalSection(Action<long, long> criticalSection, long targetVersion = -1, bool spin = false)
        {
            return ExecuteStateMachine(new SimpleVersionSchemeStateMachine(criticalSection, targetVersion), spin);
        }

    }
}