// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// "Constants" for code gen purposes of proprites of arbitrary TInputs.
    /// </summary>
    internal static class InputExtraOptions<TInput>
    {
        /// <summary>
        /// Returns true if TInput implements <see cref="IInputExtraOptions"/>.
        /// </summary>
        internal static readonly bool Implements = typeof(TInput).IsValueType ? default(TInput) is IInputExtraOptions : typeof(TInput).IsAssignableTo(typeof(IInputExtraOptions));
    }


    /// <summary>
    /// Utility methods for dealing with TInputs that _may_ implement <see cref="IInputExtraOptions"/>.
    /// </summary>
    internal static class InputExtraOptions
    {
        /// <summary>
        /// Get a hashcode for a key, incorporating the namespace in the input if any.
        /// </summary>
        /// <typeparam name="TStoreFunctions">Hashcode calculation provider type</typeparam>
        /// <typeparam name="TInput">Input type for the current operation</typeparam>
        /// <param name="functions">Hashcode calculator</param>
        /// <param name="key">Key bytes</param>
        /// <param name="input">Input which may specify a namespace</param>
        /// <returns>Calculated hash</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        internal static long GetKeyHashCode64<TStoreFunctions, TInput>(in TStoreFunctions functions, ReadOnlySpan<byte> key, ref TInput input)
            where TStoreFunctions : IStoreFunctions
        {
            // TODO: Look at code gen and very expected optimizations happen

            // If TInput is a struct and doesn't implement IInputExtraOptions, this should all get erased and turn into a GetKeyHashCode64 call

            if (input is IInputExtraOptions extra)
            {
                // Even if TInput implements IInputExtraOptions, if TryGetNamespace always returns false we should erase down to just GetKeyHashCode64

                Span<byte> namespaceBytes = stackalloc byte[8];
                if (extra.TryGetNamespace(ref namespaceBytes))
                {
                    return functions.GetKeyHashCode64(key, namespaceBytes);
                }
            }

            return functions.GetKeyHashCode64(key);
        }

        /// <summary>
        /// Get the namespace, if any, associated with the given TInput.
        /// </summary>
        /// <typeparam name="TInput">Input type for the current operation</typeparam>
        /// <param name="input">Input which may specify a namespace</param>
        /// <param name="namespaceBytes">Where to store namespace bytes if any.  Should be resized to exact namespace length if true is returned.</param>
        /// <returns>True if there is a namespace</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool TryGetNamespace<TInput>(ref TInput input, ref Span<byte> namespaceBytes)
        {
            // TODO: Look at code gen and very expected optimizations happen

            // If TInput is a struct and doesn't implement IInputExtraOptions, this should all get erased to false

            if (input is IInputExtraOptions extra)
            {
                // Even if TInput implements IInputExtraOptions, if TryGetNamespace always returns false we should erase down to false
                if (extra.TryGetNamespace(ref namespaceBytes))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
