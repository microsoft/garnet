// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Return status code for Tsavorite operations
    /// These are the basic codes that correspond to the old Status values, but *do not* compare to these directly; use the IsXxx functions.
    /// </summary>
    [Flags]
    internal enum StatusCode : byte
    {
        #region Basic status codes
        /// <summary>
        /// General success indicator. By itself it means they key for the operation was found; it may have been updated in place or copied to the log tail.
        /// </summary>
        /// <remarks>
        /// <list type="bullet">
        /// <item>Upsert ConcurrentWriter: <see cref="Found"/> | <see cref="InPlaceUpdatedRecord"/></item>
        /// <item>RMW InPlaceUpdater: <see cref="Found"/> | <see cref="InPlaceUpdatedRecord"/></item>
        /// <item>RMW CopyUpdater: <see cref="Found"/> | <see cref="CopyUpdatedRecord"/></item>
        /// <list type="bullet">
        ///   <item>If NeedCopyUpdate returns false: <see cref="Found"/></item>
        /// </list>
        /// <item>Delete ConcurrentDeleter: <see cref="Found"/> | <see cref="InPlaceUpdatedRecord"/></item>
        /// <item>Read ConcurrentReader: <see cref="Found"/></item>
        /// <list type="bullet">
        ///   <item>If in immutable region and copying to tail: <see cref="Found"/> | <see cref="CopiedRecord"/></item>
        /// </list>
        /// <item>Read Pending to SingleReader: <see cref="Found"/></item>
        /// <list type="bullet">
        ///   <item>If copying to tail: <see cref="Found"/> | <see cref="CopiedRecord"/></item>
        ///   <item>If copying to readCache: <see cref="Found"/> | <see cref="CopiedRecordToReadCache"/></item>
        /// </list>
        /// </list>
        /// </remarks>
        Found = 0x00,

        /// <summary>
        /// The key for the operation was not found. For Read, that is all that is returned for an unfound key. For other operations, see
        /// the advanced enum values for more detailed information.
        /// </summary>
        /// <remarks>
        /// <list type="bullet">
        /// <item>Upsert SingleWriter (not found in mutable region): <see cref="NotFound"/> | <see cref="CreatedRecord"/></item>
        /// <item>RMW InitialUpdater (not found in mutable, immutable, or on-disk regions): <see cref="NotFound"/> | <see cref="CreatedRecord"/></item>
        /// <list type="bullet">
        ///   <item>If NeedInitialUpdate returns false: <see cref="NotFound"/></item>
        /// </list>
        /// <item>Delete SingleDeleter (not found in mutable region): <see cref="NotFound"/> | <see cref="CreatedRecord"/></item>
        /// </list>
        /// </remarks>
        NotFound = 0x01,

        /// <summary>
        /// The operation was canceled (e.g. by an IFunctions method setting info.CancelOperation). This is not combined with advanced enum values.
        /// </summary>
        Canceled = 0x02,

        /// <summary>
        /// The Read or RMW operation went pending for I/O. This is not combined with advanced enum values; however, the application should
        /// use this to issue CompletePending operations, and then can apply knowledge of this to the advanced enum values to know whether,
        /// for example, a <see cref="CopyUpdatedRecord"/> was a copy of a record from the ReadOnly in-memory region or from Storage.
        /// </summary>
        Pending = 0x03,

        /// <summary>
        /// An error occurred. This is not combined with advanced enum values.
        /// </summary>
        Error = 0x04,

        // Values 0x03-0x0F are reserved for future use

        // Masking to extract the basic values
        BasicMask = 0x0F,
        #endregion

        // These are the advanced codes for additional info such as "did we CopyToTail?" or detailed info such as "how exactly did this operation achieve its Found status?"
        #region Advanced status codes
        /// <summary>
        /// Indicates that a new record for a previously non-existent key was appended to the log.
        /// </summary>
        /// <remarks>
        /// See basic codes for details of usage.
        /// </remarks>
        CreatedRecord = 0x10,

        /// <summary>
        /// Indicates that an existing record was updated in place.
        /// </summary>
        /// <remarks>
        /// See basic codes for details of usage.
        /// </remarks>
        InPlaceUpdatedRecord = 0x20,

        /// <summary>
        /// Indicates that an existing record key was copied, updated, and appended to the log.
        /// </summary>
        /// <remarks>
        /// See basic codes for details of usage.
        /// </remarks>
        CopyUpdatedRecord = 0x30,

        /// <summary>
        /// Indicates that an existing record key was copied and appended to the log.
        /// </summary>
        /// <remarks>
        /// See basic codes for details of usage.
        /// </remarks>
        CopiedRecord = 0x40,

        /// <summary>
        /// Indicates that an existing record key was copied, updated, and added to the readcache.
        /// </summary>
        /// <remarks>
        /// See basic codes for details of usage.
        /// </remarks>
        CopiedRecordToReadCache = 0x50,

        // unused 0x60,
        // unused 0x70,

        /// <summary>
        /// Indicates that an existing record key was auto-expired. This is a flag that is combined with lower Advanced values.
        /// </summary>
        /// <remarks>
        /// See basic codes for details of usage.
        /// </remarks>
        Expired = 0x80,

        // !! Do not enter more values here unless we expand the size of StatusCode !!

        // Mask to extract the advanced values
        AdvancedMask = 0xF0
        #endregion
    }
}