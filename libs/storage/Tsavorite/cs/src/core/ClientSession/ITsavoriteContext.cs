﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for Key-only Tsavorite operations
    /// </summary>
    public interface ITsavoriteContext<Key>
    {
        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// </summary>
        /// <returns>The hashcode of the key; created and returned by <see cref="ITsavoriteEqualityComparer{Key}.GetHashCode64(ref Key)"/></returns>
        long GetKeyHash(Key key);

        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// </summary>
        /// <returns>The hashcode of the key; created and returned by <see cref="ITsavoriteEqualityComparer{Key}.GetHashCode64(ref Key)"/></returns>
        long GetKeyHash(ref Key key);
    }

    /// <summary>
    /// Interface for Tsavorite operations
    /// </summary>
    public interface ITsavoriteContext<Key, Value, Input, Output, Context, Functions> : ITsavoriteContext<Key>
        where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Indicates whether this context has been initialized.
        /// </summary>
        public bool IsNull { get; }

        /// <summary>
        /// Obtain the underlying <see cref="ClientSession{Key, Value, Input, Output, Context, Functions}"/>
        /// </summary>
        ClientSession<Key, Value, Input, Output, Context, Functions> Session { get; }

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        bool CompletePending(bool wait = false, bool spinWaitForCommit = false);

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="completedOutputs">Outputs completed by this operation</param>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        /// <summary>
        /// Complete all pending synchronous Tsavorite operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <returns></returns>
        ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// Complete all pending synchronous Tsavorite operations, returning outputs for the completed operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <returns>Outputs completed by this operation</returns>
        ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(Key key, Input input, out Output output, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(Key key, Input input, out Output output, ref ReadOptions readOptions, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Output output, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Output output, ref ReadOptions readOptions, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Read(Key key, out Output output, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Read(Key key, out Output output, ref ReadOptions readOptions, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        public (Status status, Output output) Read(Key key, Context userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        public (Status status, Output output) Read(Key key, ref ReadOptions readOptions, Context userContext = default);

        /// <summary>
        /// Read operation that accepts a <paramref name="recordMetadata"/> ref argument to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and is updated with the address and record header for the found record.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="recordMetadata">On output, receives:
        ///         <list type="bullet">
        ///             <li>The address of the found record. This may be different from the <paramref name="recordMetadata.RecordInfo.PreviousAddress"/> passed on the call, due to
        ///                 tracing back over hash collisions until we arrive at the key match</li>
        ///             <li>A copy of the record's header in <paramref name="recordMetadata.RecordInfo"/>; <paramref name="recordMetadata.RecordInfo.PreviousAddress"/> can be passed
        ///                 in a subsequent call, thereby enumerating all records in a key's hash chain.</li>
        ///         </list>
        /// </param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default);

        /// <summary>
        /// Read operation that accepts an address to lookup at, instead of a key.
        /// </summary>
        /// <param name="address">The logical address to read</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation, including the address to read at in StartAddress</param>
        /// <param name="recordMetadata">On output, receives metadata about the record</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation; this should store the key if it needs it</returns>
        Status ReadAtAddress(long address, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default);

        /// <summary>
        /// Read operation that accepts an address to lookup at, and a key to optimize locking.
        /// </summary>
        /// <param name="address">The logical address to read</param>
        /// <param name="key">The key of the record to read, to optimize locking</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation, including the address to read at in StartAddress</param>
        /// <param name="recordMetadata">On output, receives metadata about the record</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation; this should store the key if it needs it</returns>
        Status ReadAtAddress(long address, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="upsertOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="upsertOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="recordMetadata"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="upsertOptions"></param>
        /// <param name="recordMetadata"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(Key key, Value desiredValue, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="upsertOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="upsertOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(Key key, Input input, Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="recordMetadata"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="recordMetadata"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(Key key, Input input, out Output output, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(Key key, Input input, out Output output, ref RMWOptions rmwOptions, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(Key key, Input input, Context userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(Key key, Input input, ref RMWOptions rmwOptions, Context userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(ref Key key, Context userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="deleteOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(Key key, Context userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="deleteOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(Key key, ref DeleteOptions deleteOptions, Context userContext = default);

        /// <summary>
        /// Reset the modified bit of a record (for in memory records)
        /// </summary>
        /// <param name="key"></param>
        void ResetModified(ref Key key);

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh();
    }
}