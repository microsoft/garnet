// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    internal static class EtagConstants
    {
        public const byte EtagSize = sizeof(long);

        public const long BaseEtag = 0;
    }

    /// <summary>
    /// Indirection wrapper to provide a way to set offsets related to Etags and use the getters opaquely from outside.
    /// </summary>
    public struct EtagState
    {
        public EtagState()
        {
        }

        /// <summary>
        /// Offset used accounting space for an etag during allocation
        /// </summary>
        public byte etagOffsetForVarlen { get; set; } = 0;

        /// <summary>
        /// Gives an offset used to opaquely work with Etag in a payload. By calling this you can skip past the etag if it is present.
        /// </summary>
        public byte etagSkippedStart { get; private set; } = 0;

        /// <summary>
        /// Resp response methods depend on the value for end being -1 or length of the payload. This field lets you work with providing the end opaquely.
        /// </summary>
        public int etagAccountedLength { get; private set; } = -1;

        /// <summary>
        /// Field provides access to getting an Etag from a record, hiding whether it is actually present or not.
        /// </summary>
        public long etag { get; private set; } = EtagConstants.BaseEtag;

        /// <summary>
        /// Sets the values to indicate the presence of an Etag as a part of the payload value
        /// </summary>
        public static void SetValsForRecordWithEtag(ref EtagState curr, ref SpanByte value)
        {
            curr.etagOffsetForVarlen = EtagConstants.EtagSize;
            curr.etagSkippedStart = EtagConstants.EtagSize;
            curr.etagAccountedLength = value.LengthWithoutMetadata;
            curr.etag = value.GetEtagInPayload();
        }

        public static void ResetState(ref EtagState curr)
        {
            curr.etagOffsetForVarlen = 0;
            curr.etagSkippedStart = 0;
            curr.etag = EtagConstants.BaseEtag;
            curr.etagAccountedLength = -1;
        }
    }
}