// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// The phase of a <see cref="HeapObjectBase"/>: who holds it, who last published its record to an object log, and whether a CopyUpdate
    /// has superseded it.
    /// </summary>
    /// <remarks>The object's phase word holds this value in its low bits. <see cref="SNAPSHOT_STAMPED"/> and <see cref="CAPTURED"/> also hold
    /// the tag of the Snapshot checkpoint they belong to in the bits above.</remarks>
    public enum SerializationPhase : int
    {
        /// <summary>
        /// No flush or capture has published the object's record.
        /// </summary>
        REST,

        /// <summary>
        /// One thread holds the object: a flush or a capture writing it and stamping its record, or a serializer writing it.
        /// </summary>
        SERIALIZING,

        /// <summary>
        /// A CopyUpdate superseded the object without a Snapshot checkpoint capturing it. It serializes as the bytes cached for the
        /// checkpoint that needed them, or as a null value.
        /// </summary>
        SERIALIZED,

        /// <summary>
        /// A Snapshot checkpoint's flush published the object's record with the object's bytes in that checkpoint's object log.
        /// </summary>
        SNAPSHOT_STAMPED,

        /// <summary>
        /// A ReadOnly flush published the object's record with the object's bytes in the main object log.
        /// </summary>
        READONLY_STAMPED,

        /// <summary>
        /// A Snapshot checkpoint captured the object's bytes into its object log when a CopyUpdate superseded it. It serializes as a null value.
        /// </summary>
        CAPTURED
    }
}