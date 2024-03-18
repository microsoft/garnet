// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading;


    /// <summary>
    /// Functionality for tracking threads.
    /// </summary>
    class TrackedThreads
    {
        static readonly ConcurrentDictionary<int, Thread> threads = new ConcurrentDictionary<int, Thread>();

        public static Thread MakeTrackedThread(Action action, string name)
        {
            Thread thread = null;
            thread = new Thread(ThreadStart) { Name = name };
            void ThreadStart()
            {
                threads.TryAdd(thread.ManagedThreadId, thread);
                try
                {
                    action();
                }
                finally
                {
                    threads.TryRemove(thread.ManagedThreadId, out _);
                }
            }
            return thread;
        }

        public static int NumberThreads => threads.Count;

        public static string GetThreadNames()
        {
            return string.Join(",", threads
                .Values
                .GroupBy((thread) => thread.Name)
                .OrderByDescending(group => group.Count())
                .Select(group => $"{group.Key}(x{group.Count()})"));
        }
    }
}