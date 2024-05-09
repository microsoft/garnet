// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Interface to define classes that are serializable to RESP format
    /// </summary>
    public interface IRespSerializable
    {
        /// <summary>
        /// Serializes the current object to RESP format
        /// </summary>
        /// <returns>Serialized value in RESP format</returns>
        string ToRespFormat();
    }
}