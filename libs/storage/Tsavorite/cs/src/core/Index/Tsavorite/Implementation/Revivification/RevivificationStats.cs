// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;

namespace Tsavorite.core
{
    /// <summary>
    /// Revivification statistics
    /// </summary>
    public struct RevivificationStats
    {
        // The number of times an Add() succeeded or failed (e.g. bin was full)
        internal long successfulAdds, failedAdds;

        // The number of times a Take() from the FreeRecordPool succeeded or failed.
        internal long successfulTakes, failedTakes;

        // The number of times a Take() from the FreeRecordPool encountered an empty bin.
        internal long takeEmptyBins;

        // The number of times a Take() from the FreeRecordPool did not find an address above minAddress in the bin (may also have had a too-small recordSize).
        internal long takeAddressFailures;

        // The number of times a Take() from the FreeRecordPool did not find a recordSize >= the requested size in the bin (may also have had a too-low address).
        internal long takeRecordSizeFailures;

        // The number of times an in-chain revivification succeeded or failed (e.g. competing CAS).
        internal long inChainSuccesses, inChainFailures;

        internal void MergeTo(ref RevivificationStats to, bool reset)
        {
            to.successfulAdds += successfulAdds;
            to.failedAdds += failedAdds;
            to.successfulTakes += successfulTakes;
            to.failedTakes += failedTakes;
            to.takeEmptyBins += takeEmptyBins;
            to.takeAddressFailures += takeAddressFailures;
            to.takeRecordSizeFailures += takeRecordSizeFailures;
            to.inChainSuccesses += inChainSuccesses;
            to.inChainFailures += inChainFailures;
            if (reset)
                Reset();
        }

        internal void Reset()
        {
            successfulAdds = 0;
            failedAdds = 0;
            successfulTakes = 0;
            failedTakes = 0;
            takeEmptyBins = 0;
            takeAddressFailures = 0;
            takeRecordSizeFailures = 0;
            inChainSuccesses = 0;
            inChainFailures = 0;
        }

        internal string Dump()
        {
            StringBuilder sb = new();
            sb.Append($"Successful Adds: {successfulAdds}\n");
            sb.Append($"Failed Adds: {failedAdds}\n");
            sb.Append($"Successful Takes: {successfulTakes}\n");
            sb.Append($"Failed Takes: {failedTakes}\n");
            sb.Append($"\t Empty bins: {takeEmptyBins}\n");
            sb.Append($"\t Address limit: {takeAddressFailures}\n");
            sb.Append($"\t Record size limit: {takeRecordSizeFailures}\n"); ;
            sb.Append($"Successful In-Chain: {inChainSuccesses}\n");
            sb.Append($"Failed In-Chain: {inChainFailures}\n");
            return sb.ToString();
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"adds s={successfulAdds}, f={failedAdds}; takes s={successfulTakes}, f={failedTakes} (empty={takeEmptyBins}, addrLimit={takeAddressFailures},"
                 + $" recSizeLimit={takeRecordSizeFailures}, in-chain s={inChainSuccesses}, f={inChainFailures}";
        }
    }
}