// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Result codes for RangeIndex operations.
    /// </summary>
    public enum RangeIndexResult
    {
        /// <summary>Operation succeeded.</summary>
        OK,
        /// <summary>Key or field was not found.</summary>
        NotFound,
        /// <summary>Key or field was deleted.</summary>
        Deleted,
        /// <summary>Invalid key (e.g. too long).</summary>
        InvalidKey,
        /// <summary>Operation failed with an error.</summary>
        Error,
    }
}