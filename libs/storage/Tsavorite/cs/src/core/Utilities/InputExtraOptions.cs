// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
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
    }
}
