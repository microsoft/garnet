// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Functions that make RMW behave as an atomic TryAdd operation, where Input is the value being added.
    /// Return Status.NotFound => TryAdd succeededed (item added).
    /// Return Status.Found => TryAdd failed (item not added, key was already present).
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public class TryAddFunctions<TKey, TValue, TContext> : SimpleSessionFunctions<TKey, TValue, TContext>
    {
        /// <inheritdoc />
        public override bool InPlaceUpdater(ref TKey key, ref TValue input, ref TValue value, ref TValue output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        /// <inheritdoc />
        public override bool NeedCopyUpdate(ref TKey key, ref TValue input, ref TValue oldValue, ref TValue output, ref RMWInfo rmwInfo) => false;
    }

    /// <summary>
    /// Functions that make RMW behave as an atomic TryAdd operation, where Input is the value being added.
    /// Return Status.NotFound => TryAdd succeededed (item added)
    /// Return Status.Found => TryAdd failed (item not added, key was already present)
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class TryAddFunctions<TKey, TValue> : TryAddFunctions<TKey, TValue, Empty> { }
}