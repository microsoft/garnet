// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    internal enum ResizeOperationStatus : int { IN_PROGRESS, DONE };

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct ResizeInfo
    {
        [FieldOffset(0)]
        public ResizeOperationStatus status;

        [FieldOffset(4)]
        public int version; // either 0 or 1

        [FieldOffset(0)]
        public long word;
    }

    /// <summary>
    /// The current phase of a state-machine operation such as a checkpoint
    /// </summary>
    public enum Phase : int
    {
        /// <summary>In-progress phase, entering (v+1) version</summary>
        IN_PROGRESS,

        /// <summary>Wait for an index checkpoint to finish</summary>
        WAIT_INDEX_CHECKPOINT,

        /// <summary>Wait for data flush to complete</summary>
        WAIT_FLUSH,

        /// <summary>After flush has completed, write metadata to persistent storage and issue user callbacks</summary>
        PERSISTENCE_CALLBACK,

        /// <summary>The default phase; no state-machine operation is operating</summary>
        REST,

        /// <summary>Prepare for an index checkpoint</summary>
        PREP_INDEX_CHECKPOINT,

        /// <summary>Wait for an index-only checkpoint to complete</summary>
        WAIT_INDEX_ONLY_CHECKPOINT,

        /// <summary>Prepare for a checkpoint, still in (v) version</summary>
        PREPARE,

        /// <summary>Prepare to resize the index</summary>
        PREPARE_GROW,

        /// <summary>Index resizing is in progress</summary>
        IN_PROGRESS_GROW,

        /// <summary>Internal intermediate state of state machine</summary>
        INTERMEDIATE = 16,
    };

    /// <summary>
    /// The current state of a state-machine operation such as a checkpoint.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct SystemState
    {
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

        /// <summary>
        /// The word containing information in bitfields
        /// </summary>
        [FieldOffset(0)]
        internal long Word;


        /// <summary>
        /// The current <see cref="Phase"/> of the operation
        /// </summary>
        public Phase Phase
        {
            get
            {
                return (Phase)((Word >> kPhaseShiftInWord) & kPhaseMaskInInteger);
            }
            set
            {
                Word &= ~kPhaseMaskInWord;
                Word |= (((long)value) & kPhaseMaskInInteger) << kPhaseShiftInWord;
            }
        }

        /// <summary>
        /// The version of the database when this operation is complete
        /// </summary>
        public long Version
        {
            get
            {
                return Word & kVersionMaskInWord;
            }
            set
            {
                Word &= ~kVersionMaskInWord;
                Word |= value & kVersionMaskInWord;
            }
        }

        /// <summary>
        /// Copy the <paramref name="other"/> <see cref="SystemState"/> into this <see cref="SystemState"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SystemState Copy(ref SystemState other)
        {
            var info = default(SystemState);
            info.Word = other.Word;
            return info;
        }

        /// <summary>
        /// Create a <see cref="SystemState"/> with the specified values
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SystemState Make(Phase status, long version)
        {
            var info = default(SystemState);
            info.Phase = status;
            info.Version = version;
            return info;
        }

        /// <summary>
        /// Create a copy of the passed <see cref="SystemState"/> that is marked with the <see cref="Phase.INTERMEDIATE"/> phase
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SystemState MakeIntermediate(SystemState state)
            => Make(state.Phase | Phase.INTERMEDIATE, state.Version);

        /// <summary>
        /// Create a copy of the passed <see cref="SystemState"/> that is not marked with the <see cref="Phase.INTERMEDIATE"/> phase
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void RemoveIntermediate(ref SystemState state)
        {
            state.Phase &= ~Phase.INTERMEDIATE;
        }

        /// <summary>
        /// Compare two <see cref="SystemState"/>s for equality
        /// </summary>
        internal static bool Equal(SystemState s1, SystemState s2)
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
            return obj is SystemState other && Equals(other);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return Word.GetHashCode();
        }

        /// <summary>
        /// Compare the current <see cref="SystemState"/> to <paramref name="other"/> for equality
        /// </summary>
        private bool Equals(SystemState other)
        {
            return Word == other.Word;
        }

        /// <summary>
        /// Equals
        /// </summary>
        public static bool operator ==(SystemState left, SystemState right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Not Equals
        /// </summary>
        public static bool operator !=(SystemState left, SystemState right)
        {
            return !(left == right);
        }
    }
}