// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;

namespace Garnet.networking
{
    /// <summary>
    /// Debug-build counters for the session parking handoff, kept off the generic handler so that a reader
    /// does not have to name the closed type whose statics it wants.
    /// </summary>
    internal static class ParkDiagnostics
    {
        /// <summary>
        /// Counts parks whose receive state -- the accept socket and the pinned receive buffer -- reached the
        /// path that disposes it, rather than being left on a handler for the garbage collector to find.
        /// </summary>
        /// <remarks>
        /// A park that is abandoned before its operation starts has no release coming, so without something
        /// standing in for one the rendezvous never completes, the resume is never scheduled, and the receive
        /// state is never deterministically released. The connection still goes away, which is why counting
        /// live connections cannot see this; the handoff is what has to be observed, and this counts it.
        /// </remarks>
        internal static int ReceiveStateReclaimed;

        /// <summary>
        /// Records one deterministic release of a park's receive state.
        /// </summary>
        [Conditional("DEBUG")]
        internal static void NoteReceiveStateReclaimed() => Interlocked.Increment(ref ReceiveStateReclaimed);
    }
}