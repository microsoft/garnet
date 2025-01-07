using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Offset acounting done to prevent the need for recalculation at different methods.
    /// </summary>
    internal sealed class EtagOffsetManagementContext
    {
        public byte EtagIgnoredOffset { get; private set; }

        public byte EtagOffsetForVarlen { get; private set; }

        public int EtagIgnoredEnd { get; private set; }

        public long ExistingEtag { get; private set; }

        public void SetEtagOffsetBasedOnInputHeader(bool withEtag)
        {
            this.EtagOffsetForVarlen = !withEtag ? (byte)0 : Constants.EtagSize;
        }

        public unsafe void CalculateOffsets(bool hasEtag, ref SpanByte value)
        {
            if (!hasEtag)
            {
                this.EtagIgnoredOffset = 0;
                this.EtagIgnoredEnd = -1;
                this.ExistingEtag = Constants.BaseEtag;
                return;
            }

            this.EtagOffsetForVarlen = this.EtagIgnoredOffset = Constants.EtagSize;
            this.EtagIgnoredEnd = value.LengthWithoutMetadata;
            this.ExistingEtag = *(long*)value.ToPointer();
        }
    }
}