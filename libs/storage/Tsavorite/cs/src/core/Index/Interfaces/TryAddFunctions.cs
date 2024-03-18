// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Functions that make RMW behave as an atomic TryAdd operation, where Input is the value being added.
    /// Return Status.NotFound => TryAdd succeededed (item added).
    /// Return Status.Found => TryAdd failed (item not added, key was already present).
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class TryAddFunctions<Key, Value, Context> : SimpleFunctions<Key, Value, Context>
    {
        /// <inheritdoc />
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        /// <inheritdoc />
        public override bool NeedCopyUpdate(ref Key key, ref Value input, ref Value oldValue, ref Value output, ref RMWInfo rmwInfo) => false;
    }

    /// <summary>
    /// Functions that make RMW behave as an atomic TryAdd operation, where Input is the value being added.
    /// Return Status.NotFound => TryAdd succeededed (item added)
    /// Return Status.Found => TryAdd failed (item not added, key was already present)
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class TryAddFunctions<Key, Value> : TryAddFunctions<Key, Value, Empty> { }
}