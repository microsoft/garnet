// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// If implemented by a TInput, used to specify extra options - RecordType, Namespace, Alignment, etc.
    /// </summary>
    public interface IInputExtraOptions
    {
        /// <summary>
        /// Obtain namespace bytes, if any.
        /// </summary>
        /// <param name="namespaceBytes">At least 8 bytes of space for namespace.</param>
        /// <returns>True if namespace is present.</returns>
        bool TryGetNamespace(ref Span<byte> namespaceBytes);
    }
}
