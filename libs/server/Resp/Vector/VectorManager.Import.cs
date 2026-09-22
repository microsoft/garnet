// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using ImportResult = Garnet.server.NativeDiskANNMethods.DiskANNImportResult;

namespace Garnet.server
{
    public partial class VectorManager
    {
        internal readonly record struct ImportPartition(ImportJob Job, int TaskIndex);

        internal sealed class ImportJob
        {
            internal readonly ulong Context;
            internal readonly nint IndexPtr;
            private readonly ImportResult[] results;
            private TaskCompletionSource<ImportResult> completion;
            private ImportResult outcome;
            private int pending;
            private int activeImports;
            private bool finalizationStarted;

            internal int TaskCount => results.Length;

            internal ImportJob(ulong context, nint indexPtr, int taskCount)
            {
                Context = context;
                IndexPtr = indexPtr;
                results = new ImportResult[taskCount];
                Array.Fill(results, ImportResult.TaskFailed);
            }

            internal bool TryBeginImport()
            {
                lock (this)
                {
                    if (finalizationStarted)
                    {
                        return false;
                    }
                    activeImports++;
                    return true;
                }
            }

            internal void EndImport()
            {
                lock (this)
                {
                    if (--activeImports == 0)
                    {
                        System.Threading.Monitor.PulseAll(this);
                    }
                }
            }

            internal Task<ImportResult> StartOrJoinAsync(ChannelWriter<ImportPartition> writer)
            {
                lock (this)
                {
                    finalizationStarted = true;
                    while (activeImports != 0)
                    {
                        System.Threading.Monitor.Wait(this);
                    }
                    if (completion != null && (!completion.Task.IsCompleted || outcome != ImportResult.TaskFailed))
                    {
                        return completion.Task;
                    }

                    completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    pending = 0;
                    foreach (var result in results)
                    {
                        if (result == ImportResult.TaskFailed)
                        {
                            pending++;
                        }
                    }
                    for (var taskIndex = 0; taskIndex < results.Length; taskIndex++)
                    {
                        if (results[taskIndex] == ImportResult.TaskFailed && !writer.TryWrite(new(this, taskIndex)))
                        {
                            Complete(taskIndex, ImportResult.TaskFailed);
                        }
                    }
                    return completion.Task;
                }
            }

            internal void Complete(int taskIndex, ImportResult result)
            {
                lock (this)
                {
                    results[taskIndex] = result;
                    if (--pending != 0)
                    {
                        return;
                    }

                    outcome = Array.IndexOf(results, ImportResult.FinishFailed) >= 0
                        ? ImportResult.FinishFailed
                        : Array.IndexOf(results, ImportResult.TaskFailed) >= 0 ? ImportResult.TaskFailed : ImportResult.Success;
                    completion.SetResult(outcome);
                }
            }
        }

        private readonly ConcurrentDictionary<(ulong Context, nint IndexPtr), ImportJob> importJobs = new();
        private readonly Channel<ImportPartition> importChannel = Channel.CreateUnbounded<ImportPartition>(
            new() { SingleWriter = false, SingleReader = false, AllowSynchronousContinuations = false });
        private readonly Task[] importTasks;

        private static bool IsImportPending(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var flags, out _);
            return (flags & VectorSetFlags.ImportPending) != 0;
        }

        private static bool IsImportCompleted(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var flags, out _);
            return (flags & VectorSetFlags.ImportCompleted) != 0;
        }

        private static bool IsImportFailed(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var flags, out _);
            return (flags & VectorSetFlags.ImportFailed) != 0;
        }

        internal bool ImportTerm(ReadOnlySpan<byte> key, ReadOnlySpan<byte> indexConfig, uint termType, ReadOnlySpan<byte> id, ReadOnlySpan<byte> value)
        {
            if (IsImportCompleted(indexConfig) || IsImportFailed(indexConfig))
            {
                return false;
            }
            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);
            var job = GetImportJob(context, indexPtr);
            if (!job.TryBeginImport())
            {
                return false;
            }
            try
            {
                if (!Service.ImportTerm(context, indexPtr, termType, id, value))
                {
                    return false;
                }
                if (!IsImportPending(indexConfig))
                {
                    SetFlags(key, VectorSetFlags.ImportPending, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
                }
                ReplicateVectorSetImport(key, termType, id, value);
                return true;
            }
            finally
            {
                job.EndImport();
            }
        }

        private ImportJob GetImportJob(ulong context, nint indexPtr)
            => importJobs.GetOrAdd((context, indexPtr),
                static (identity, taskCount) => new(identity.Context, identity.IndexPtr, taskCount), importTasks.Length);

        /// <summary>
        /// Joins or retries finalization while the caller retains the index lifetime lock.
        /// Partition progress is handle-local; completion and terminal failure are persisted in the index stub.
        /// </summary>
        internal ImportResult FinishImport(ReadOnlySpan<byte> key, ReadOnlySpan<byte> indexConfig)
        {
            if (IsImportFailed(indexConfig))
            {
                return ImportResult.FinishFailed;
            }
            if (IsImportCompleted(indexConfig))
            {
                ReplicateVectorSetFinishImport(key);
                return ImportResult.Success;
            }
            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);
            if (!IsImportPending(indexConfig))
            {
                if (!Service.CanImport(context, indexPtr))
                {
                    return ImportResult.TaskFailed;
                }
                SetFlags(key, VectorSetFlags.ImportPending, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
            }
            var job = GetImportJob(context, indexPtr);
            var result = AsyncUtils.BlockingWait(job.StartOrJoinAsync(importChannel.Writer));
            if (result == ImportResult.Success)
            {
                ReplicateVectorSetFinishImport(key);
                SetFlags(key, VectorSetFlags.ImportCompleted, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
            }
            else if (result == ImportResult.FinishFailed)
            {
                ReplicateVectorSetFinishImport(key, failed: true);
                SetFlags(key, VectorSetFlags.ImportPending | VectorSetFlags.ImportFailed, ref ActiveThreadSession.stringBasicContext, SetImportStateArg);
            }
            return result;
        }

        private void StartImportTasks()
        {
            for (var taskIndex = 0; taskIndex < importTasks.Length; taskIndex++)
            {
                importTasks[taskIndex] = RunImportTaskAsync();
            }
        }

        private async Task RunImportTaskAsync()
        {
            await Task.Yield();

            var reader = importChannel.Reader;
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                RespServerSession session = null;
                try
                {
                    session = (RespServerSession)getTempSession();
                    if (session.activeDbId != dbId && !session.TrySwitchActiveDatabaseSession(dbId))
                    {
                        throw new GarnetException($"Could not switch import finalization session to {dbId}");
                    }

                    while (reader.TryRead(out var partition))
                    {
                        ProcessImportPartition(session, partition);
                    }
                }
                catch (Exception exception)
                {
                    logger?.LogError(exception, "Could not initialize import finalization worker for database {dbId}", dbId);
                    while (reader.TryRead(out var partition))
                    {
                        partition.Job.Complete(partition.TaskIndex, ImportResult.TaskFailed);
                    }
                }
                finally
                {
                    try
                    {
                        session?.Dispose();
                    }
                    catch (Exception exception)
                    {
                        logger?.LogError(exception, "Could not dispose import finalization session for database {dbId}", dbId);
                    }
                }
            }
        }

        private void ProcessImportPartition(RespServerSession session, ImportPartition partition)
        {
            var previousSession = ActiveThreadSession;
            var result = ImportResult.FinishFailed;
            try
            {
                ActiveThreadSession = session.storageSession;
                if (partition.TaskIndex == 0)
                {
                    ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.VectorSet_Pause_Before_Import_Finalization);
                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Fail_Before_Import_Finalization);
                }
                result = Service.FinishImport(partition.Job.Context, partition.Job.IndexPtr, partition.TaskIndex, partition.Job.TaskCount);
            }
            catch (Exception exception)
            {
                logger?.LogError(exception, "Import finalization partition {taskIndex}/{taskCount} failed for context {context}",
                    partition.TaskIndex, partition.Job.TaskCount, partition.Job.Context);
            }
            finally
            {
                ActiveThreadSession = previousSession;
                partition.Job.Complete(partition.TaskIndex, result);
            }
        }

        private void StopImportTasks()
        {
            _ = importChannel.Writer.TryComplete();
            while (importChannel.Reader.TryRead(out var partition))
            {
                partition.Job.Complete(partition.TaskIndex, ImportResult.TaskFailed);
            }
            AsyncUtils.BlockingWait(Task.WhenAll(importTasks));
            importJobs.Clear();
        }
    }
}