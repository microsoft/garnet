// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Functions that make RMW behave as an atomic TryAdd operation, where Input is the value being added.
    /// Return Status.NotFound => TryAdd succeededed (item added).
    /// Return Status.Found => TryAdd failed (item not added, key was already present).
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public class TryAddFunctions<TValue, TContext> : SimpleSessionFunctions<TValue, TContext>
    {
        /// <inheritdoc />
        public override bool InPlaceUpdater(ref LogRecord<TValue> logRecord, ref TValue input, ref TValue output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc />
        public override bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TValue input, ref TValue output, ref RMWInfo rmwInfo) => false;
    }

    /// <summary>
    /// Functions that make RMW behave as an atomic TryAdd operation, where Input is the value being added.
    /// Return Status.NotFound => TryAdd succeededed (item added)
    /// Return Status.Found => TryAdd failed (item not added, key was already present)
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    public class TryAddFunctions<TValue> : TryAddFunctions<TValue, Empty> { }
}