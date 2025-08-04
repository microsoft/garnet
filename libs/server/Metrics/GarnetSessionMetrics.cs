// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Garnet.server
{
    /// <summary>
    /// Performance Metrics Emitted from ServerSessionBase
    /// </summary>
    public class GarnetSessionMetrics
    {
        /// <summary>
        /// Aggregate number of incoming bytes from the network
        /// </summary>
        public ulong total_net_input_bytes;

        /// <summary>
        /// Aggregate number of outgoing bytes to the network
        /// </summary>
        public ulong total_net_output_bytes;

        /// <summary>
        /// Aggregate number of commands processed
        /// </summary>
        public ulong total_commands_processed;

        /// <summary>
        /// Aggregate number pending.
        /// </summary>
        public ulong total_pending;

        /// <summary>
        /// Aggregate number of found.
        /// </summary>
        public ulong total_found;

        /// <summary>
        /// Aggregate number of notfound.
        /// </summary>
        public ulong total_notfound;

        /// <summary>
        /// Aggregate number of cluster commands processed
        /// </summary>
        public ulong total_cluster_commands_processed;

        /// <summary>
        /// Keep track of write commands executed
        /// </summary>
        public ulong total_write_commands_processed;

        /// <summary>
        /// Keep track of read commands executed
        /// </summary>
        public ulong total_read_commands_processed;

        /// <summary>
        /// Keep track of transaction commands received.
        /// </summary>
        public ulong total_transactions_commands_received;

        /// <summary>
        /// Keep track of total number of exceptions triggered in try consume for all resp server sessions
        /// </summary>
        public ulong total_number_resp_server_session_exceptions;

        /// <summary>
        /// Keep track of total number of transactions that were executed successfully.
        /// </summary>
        public ulong total_transaction_commands_execution_failed;

        /// <summary>
        /// GarnetSessionMetrics constructor
        /// </summary>
        public GarnetSessionMetrics() { Reset(); }

        /// <summary>
        /// Add to this session metrics tracker
        /// </summary>
        /// <param name="add"></param>
        internal void Add(GarnetSessionMetrics add)
        {
            incr_total_net_input_bytes(add.get_total_net_input_bytes());
            incr_total_net_output_bytes(add.get_total_net_output_bytes());
            incr_total_commands_processed(add.get_total_commands_processed());
            incr_total_pending(add.get_total_pending());
            incr_total_found(add.get_total_found());
            incr_total_notfound(add.get_total_notfound());

            incr_total_cluster_commands_processed(add.get_total_cluster_commands_processed());

            add_total_write_commands_processed(add.get_total_write_commands_processed());
            add_total_read_commands_processed(add.get_total_read_commands_processed());
            incr_total_transaction_commands_received(add.get_total_transaction_commands_received());
            incr_total_transaction_execution_failed(add.get_total_transaction_commands_execution_failed());

            incr_total_number_resp_server_session_exceptions(add.get_total_number_resp_server_session_exceptions());
        }

        /// <summary>
        /// Reset current session metrics tracker
        /// </summary>
        internal void Reset()
        {
            total_net_input_bytes = 0;
            total_net_output_bytes = 0;
            total_commands_processed = 0;
            total_pending = 0;
            total_found = 0;
            total_notfound = 0;
            total_cluster_commands_processed = 0;
            total_write_commands_processed = 0;
            total_read_commands_processed = 0;
            total_transactions_commands_received = 0;
            total_transaction_commands_execution_failed = 0;
            total_number_resp_server_session_exceptions = 0;
        }

        /// <summary>
        /// Accumulate incoming bytes from network
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_net_input_bytes(ulong bytes) => total_net_input_bytes += bytes;

        /// <summary>
        /// Get total_net_input_bytes
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_net_input_bytes() => total_net_input_bytes;

        /// <summary>
        /// Accumulate output bytes from network
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_net_output_bytes(ulong bytes) => total_net_output_bytes += bytes;

        /// <summary>
        /// Get total_net_output_bytes
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_net_output_bytes() => total_net_output_bytes;

        /// <summary>
        /// Accumulate commands processed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_commands_processed(ulong cmds) => total_commands_processed += cmds;

        /// <summary>
        /// Get total_commands_processed
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_commands_processed() => total_commands_processed;

        /// <summary>
        /// Increment pending operations.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_pending(ulong count = 1) => total_pending += count;

        /// <summary>
        /// Get total_pending
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_pending() => total_pending;

        /// <summary>
        /// Increment found operations.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_found(ulong count = 1) => total_found += count;

        /// <summary>
        /// Get total_found
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_found() => total_found;

        /// <summary>
        /// Increment found operations.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_notfound(ulong count = 1) => total_notfound += count;

        /// <summary>
        /// Get total_notfound
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_notfound() => total_notfound;

        /// <summary>
        /// Increment total cluster commands processed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_cluster_commands_processed(ulong count = 1) => total_cluster_commands_processed += count;

        /// <summary>
        /// Get total_cluster_commands_processed
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_cluster_commands_processed() => total_cluster_commands_processed;

        /// <summary>
        /// Add to total_write_commands_processed
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void add_total_write_commands_processed(ulong count) => total_write_commands_processed += count;

        /// <summary>
        /// Get total_write_commands_processed
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_write_commands_processed() => total_write_commands_processed;

        /// <summary>
        /// Add to total_read_commands_processed
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void add_total_read_commands_processed(ulong count) => total_read_commands_processed += count;

        /// <summary>
        /// Increment total_transactions_commands_received
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_transaction_commands_received(ulong count = 1) => total_transactions_commands_received += count;

        /// <summary>
        /// Increment total_transaction_commands_execution_failed
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_transaction_execution_failed(ulong count = 1) => total_transaction_commands_execution_failed += count;

        /// <summary>
        /// Get total_read_commands_processed
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_read_commands_processed() => total_read_commands_processed;

        /// <summary>
        /// Get total_transactions_commands_received
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_transaction_commands_received() => total_transactions_commands_received;

        /// <summary>
        /// Get total_transaction_commands_execution_failed
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_transaction_commands_execution_failed() => total_transaction_commands_execution_failed;

        /// <summary>
        /// Increment total_number_resp_server_session_exceptions
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void incr_total_number_resp_server_session_exceptions(ulong count) => total_number_resp_server_session_exceptions += count;

        /// <summary>
        /// Get total_number_resp_server_session_exceptions
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong get_total_number_resp_server_session_exceptions() => total_number_resp_server_session_exceptions;
    }
}