// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking
{
    /// <summary>
    /// Wire format for a session, you can add custom session types on the server and client side
    /// (e.g., one per distinct store and/or function types).
    /// </summary>
    public enum WireFormat : byte
    {
        /// <summary>
        /// ASCII wire format, e.g. for RESP
        /// </summary>
        ASCII = 255
    }
}