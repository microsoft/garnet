// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.cluster
{
    internal struct GossipStats
    {
        /// <summary>
        /// number of requests for received for processing
        /// </summary>
        public long meet_requests_recv;

        /// <summary>
        /// number of succeeded meet requests
        /// </summary>
        public long meet_requests_succeed;
        /// <summary>
        /// number of failed meet requests
        /// </summary>
        public long meet_requests_failed;

        /// <summary>
        /// number of gossip requests send successfully
        /// </summary>
        public long gossip_success_count;

        /// <summary>
        /// number of gossip requests failed to send
        /// </summary>
        public long gossip_failed_count;

        /// <summary>
        /// number of gossip requests that timed out
        /// </summary>
        public long gossip_timeout_count;

        /// <summary>
        /// number of gossip requests that contained full config array
        /// </summary>
        public long gossip_full_send;

        /// <summary>
        /// number of gossip requests send with empty array (i.e. ping)
        /// </summary>
        public long gossip_empty_send;

        /// <summary>
        /// Aggregate bytes gossip has send
        /// </summary>
        public long gossip_bytes_send;

        /// <summary>
        /// Aggregate bytes gossip has received
        /// </summary>
        public long gossip_bytes_recv;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateMeetRequestsRecv()
            => Interlocked.Increment(ref meet_requests_recv);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateMeetRequestsSucceed()
            => Interlocked.Increment(ref meet_requests_succeed);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateMeetRequestsFailed()
            => Interlocked.Increment(ref meet_requests_failed);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateGossipBytesSend(int byteCount)
            => Interlocked.Add(ref gossip_bytes_send, byteCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateGossipBytesRecv(int byteCount)
            => Interlocked.Add(ref gossip_bytes_recv, byteCount);

        public void Reset()
        {
            meet_requests_recv = 0;
            meet_requests_succeed = 0;
            meet_requests_failed = 0;
            gossip_success_count = 0;
            gossip_failed_count = 0;
            gossip_timeout_count = 0;
            gossip_full_send = 0;
            gossip_empty_send = 0;
            gossip_bytes_send = 0;
            gossip_bytes_recv = 0;
        }
    }
}