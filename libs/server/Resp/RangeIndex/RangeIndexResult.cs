// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Result codes returned by RangeIndex storage-layer operations.
    /// These codes are mapped to RESP responses by the network handler
    /// (<see cref="RespServerSession"/>).
    /// </summary>
    public enum RangeIndexResult
    {
        /// <summary>Operation succeeded.</summary>
        OK,
        /// <summary>Key or field was not found in the BfTree.</summary>
        NotFound,
        /// <summary>Key or field was found but has been logically deleted.</summary>
        Deleted,
        /// <summary>Invalid key (e.g. exceeds the configured MAXKEYLEN).</summary>
        InvalidKey,
        /// <summary>Operation failed with an error (see accompanying error message).</summary>
        Error,
        /// <summary>Operation not supported for MEMORY-mode indexes.</summary>
        MemoryModeNotSupported,
    }
}