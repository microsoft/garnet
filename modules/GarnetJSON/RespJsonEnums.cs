// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace GarnetJSON
{
    /// <summary>
    /// Represents the result of a JSON.SET operation.
    /// </summary>
    public enum SetResult : byte
    {
        /// <summary>
        /// The operation was successful.
        /// </summary>
        Success,

        /// <summary>
        /// The condition for the operation was not met.
        /// </summary>
        ConditionNotMet,

        /// <summary>
        /// An error occurred during the operation.
        /// </summary>
        Error
    }
}