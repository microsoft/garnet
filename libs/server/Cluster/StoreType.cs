// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Store type to operate on. Garnet keeps data in two stores, main store
    /// for raw strings and object store for data structures such as sorted set, 
    /// hash, list.
    /// </summary>
    public enum StoreType : byte
    {
        /// <summary>
        /// No store specified
        /// </summary>
        None = 0,

        /// <summary>
        /// Main (raw string) store
        /// </summary>
        Main = 1,

        /// <summary>
        /// Object store
        /// </summary>
        Object = 2,

        /// <summary>
        /// All stores
        /// </summary>
        All = 3,
    }
}