// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Outcome of <see cref="RangeIndexManager.PublishMigratedIndex"/> for a single migrated RI key.
    /// </summary>
    public enum PublishMigratedIndexResult
    {
        /// <summary>The migrated RangeIndex was published successfully.</summary>
        Success,

        /// <summary>A RangeIndex already existed at this key and MIGRATE REPLACE was not specified;
        /// no destructive action was taken.</summary>
        SkippedAlreadyExists,

        /// <summary>Publish failed due to an exception or store-level error (logged).</summary>
        Failed,
    }
}