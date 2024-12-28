using System.Threading;
using Tsavorite.core;

namespace Garnet.server
{
    public struct EtagOffsetManagementContext
    {
        // default values for when no Etag exists on a record
        public int EtagIgnoredOffset { get; private set; }

        public int EtagIgnoredEnd { get; private set; }

        public long ExistingEtag { get; private set; }

        public int EtagOffsetBasedOnInputHeaderOrRecordInfo { get; private set; }

        public EtagOffsetManagementContext SetEtagOffsetBasedOnInputHeader()
        {
            EtagOffsetBasedOnInputHeaderOrRecordInfo = Constants.EtagSize;
            return this;
        }

        public unsafe EtagOffsetManagementContext CalculateOffsets(bool hasEtag, ref SpanByte value)
        {
            if (hasEtag)
            {
                EtagOffsetBasedOnInputHeaderOrRecordInfo = EtagIgnoredOffset = Constants.EtagSize;
                EtagIgnoredEnd = value.LengthWithoutMetadata;
                ExistingEtag = *(long*)value.ToPointer();
            }
            else
            {
                EtagIgnoredOffset = 0;
                EtagIgnoredEnd = -1;
                ExistingEtag = Constants.BaseEtag;
            }
 
            return this;
        }
    }
}