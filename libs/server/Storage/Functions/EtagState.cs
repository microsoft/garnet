// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Indirection wrapper to provide a way to set offsets related to Etags and use the getters opaquely from outside.
    /// </summary>
    public readonly struct EtagState
    {
        public EtagState()
        {
        }

        /// <summary>
        /// Offset used accounting space for an etag during allocation
        /// </summary>
        public byte etagOffsetForVarlen { get; init; } = 0;

        /// <summary>
        /// Gives an offset used to opaquely work with Etag in a payload. By calling this you can skip past the etag if it is present.
        /// </summary>
        public byte etagSkippedStart { get; init; } = 0;

        /// <summary>
        /// Resp response methods depend on the value for end being -1 or length of the payload. This field lets you work with providing the end opaquely.
        /// </summary>
        public int etagAccountedLength { get; init; } = -1;

        /// <summary>
        /// Field provides access to getting an Etag from a record, hiding whether it is actually present or not.
        /// </summary>
        public long etag { get; init; } = Constants.BaseEtag;

        /// <summary>
        /// Sets the values to indicate the presence of an Etag as a part of the payload value
        /// </summary>
        /// <param name="value">The SpanByte for the record</param>
        public static EtagState SetValsForRecordWithEtag(ref SpanByte value) => new EtagState
        {
            etagOffsetForVarlen = Constants.EtagSize,
            etagSkippedStart = Constants.EtagSize,
            etagAccountedLength = value.LengthWithoutMetadata,
            etag = value.GetEtagInPayload()
        };

        public static EtagState ResetState() => new EtagState();
    }
}