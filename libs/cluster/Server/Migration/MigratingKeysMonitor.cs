// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// MigrateSession
    /// </summary>
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        readonly HashSet<ArgSlice> keysPendingMigration = new(ArgSliceComparer.Instance);

        /// <summary>
        /// Add key to the set of migrating keys
        /// </summary>
        /// <param name="key"></param>
        public void SetAsMigrating(ArgSlice key) => keysPendingMigration.Add(key);

        /// <summary>
        /// Remove key from migrating set (
        /// </summary>
        /// <param name="key"></param>
        public void RemoveAsMigrating(ArgSlice key) => keysPendingMigration.Remove(key);

        /// <summary>
        /// If key is not part of those that are currently migrating then can safely operate on it
        /// </summary>
        /// <param name="key"></param>
        /// <returns>False if key is being migrated, true otherwise</returns>
        public bool CanOperateOnKey(ArgSlice key) => !keysPendingMigration.Contains(key);
    }
}
