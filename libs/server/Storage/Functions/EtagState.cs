// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Indirection wrapper to provide a way to set offsets related to Etags and use the getters opaquely from outside.
    /// </summary>
    public class EtagState
    {
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
        public long etag { get; private set; } = Constants.BaseEtag;

        /// <summary>
        /// Sets the values to indicate the presence of an Etag as a part of the payload value
        /// </summary>
        /// <param name="value">The SpanByte for the record</param>
        public void SetValsForRecordWithEtag(ref SpanByte value)
        {
            etagOffsetForVarlen = Constants.EtagSize;
            etagSkippedStart = Constants.EtagSize;
            etagAccountedLength = value.LengthWithoutMetadata;
            etag = value.GetEtagInPayload();
        }

        /// <summary>
        /// Resets the values back to default values so that state between operations does not leak
        /// </summary>
        public void ResetToDefaultVals()
        {
            etagOffsetForVarlen = 0;
            etagSkippedStart = 0;
            etagAccountedLength = -1;
            etag = Constants.BaseEtag;
        }
    }
}