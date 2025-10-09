// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public enum SerializationPhase : int
    {
        /// <summary>
        /// Serialization has not been started.
        /// </summary>
        REST,

        /// <summary>
        /// Serialization is in progress.
        /// </summary>
        SERIALIZING,

        /// <summary>
        /// Serialization has been completed.
        /// </summary>
        SERIALIZED
    }
}