// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for Key-only Tsavorite operations
    /// </summary>
    public interface ITsavoriteContext<TKey>
    {
        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// </summary>
        /// <returns>The hashcode of the key; created and returned by <see cref="IKeyComparer{Key}.GetHashCode64(ref Key)"/></returns>
        long GetKeyHash(TKey key);

        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// </summary>
        /// <returns>The hashcode of the key; created and returned by <see cref="IKeyComparer{Key}.GetHashCode64(ref Key)"/></returns>
        long GetKeyHash(ref TKey key);
    }

    /// <summary>
    /// Interface for Tsavorite operations
    /// </summary>
    public interface ITsavoriteContext<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> : ITsavoriteContext<TKey>
        where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Indicates whether this context has been initialized.
        /// </summary>
        public bool IsNull { get; }

        /// <summary>
        /// Obtain the underlying <see cref="ClientSession{Key, Value, Input, Output, Context, Functions, TStoreFunctions, TAllocator}"/>
        /// </summary>
        ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session { get; }

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
        bool CompletePendingWithOutputs(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

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
        ValueTask<CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref TKey key, ref TInput input, ref TOutput output, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(TKey key, TInput input, out TOutput output, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(TKey key, TInput input, out TOutput output, ref ReadOptions readOptions, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref TKey key, ref TOutput output, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="ISessionFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref TKey key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Read(TKey key, out TOutput output, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Read(TKey key, out TOutput output, ref ReadOptions readOptions, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        public (Status status, TOutput output) Read(TKey key, TContext userContext = default);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        public (Status status, TOutput output) Read(TKey key, ref ReadOptions readOptions, TContext userContext = default);

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
        Status Read(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default);

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
        Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default);

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
        Status ReadAtAddress(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default);

        /// <summary>
        /// Read batch operation, which attempts to prefetch as an optimization.
        /// </summary>
        void ReadWithPrefetch<TBatch>(ref TBatch batch, TContext userContext = default)
            where TBatch : IReadArgBatch<TKey, TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            ;

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref TKey key, ref TValue desiredValue, TContext userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="upsertOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref TKey key, ref TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, TContext userContext = default);

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
        Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default);

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
        Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default);

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
        Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(TKey key, TValue desiredValue, TContext userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="upsertOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(TKey key, TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Upsert(TKey key, TInput input, TValue desiredValue, ref TOutput output, TContext userContext = default);

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
        Status Upsert(TKey key, TInput input, TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref TKey key, ref TInput input, ref TOutput output, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="recordMetadata"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default);

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
        Status RMW(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(TKey key, TInput input, out TOutput output, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(TKey key, TInput input, out TOutput output, ref RMWOptions rmwOptions, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref TKey key, ref TInput input, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(ref TKey key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(TKey key, TInput input, TContext userContext = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="rmwOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status RMW(TKey key, TInput input, ref RMWOptions rmwOptions, TContext userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(ref TKey key, TContext userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="deleteOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(ref TKey key, ref DeleteOptions deleteOptions, TContext userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(TKey key, TContext userContext = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="deleteOptions"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        Status Delete(TKey key, ref DeleteOptions deleteOptions, TContext userContext = default);

        /// <summary>
        /// Reset the modified bit of a record (for in memory records)
        /// </summary>
        /// <param name="key"></param>
        void ResetModified(ref TKey key);

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh();
    }
}