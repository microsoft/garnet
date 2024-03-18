// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    internal interface IMetrics
    {
        public void Add(IMetrics metrics);

        public void Reset();

        public void incr_total_net_input_bytes(ulong bytes);

        public ulong get_total_net_input_bytes();

        public void incr_total_net_output_bytes(ulong bytes);

        public ulong get_total_net_output_bytes();

        public void incr_total_commands_processed(ulong cmds);

        public ulong get_total_commands_processed();

        public void incr_total_pending(ulong count = 1);

        public ulong get_total_pending();

        public void incr_total_found(ulong count = 1);

        public ulong get_total_found();

        public void incr_total_notfound(ulong count = 1);

        public ulong get_total_notfound();

        public void incr_total_cluster_commands_processed(ulong count = 1);

        public ulong get_total_cluster_commands_processed();

        public void add_total_write_commands_processed(ulong count);

        public void incr_total_write_commands_processed(byte cmd);

        public ulong get_total_write_commands_processed();

        public void add_total_read_commands_processed(ulong count);

        public void incr_total_read_commands_processed(byte cmd);

        public ulong get_total_read_commands_processed();
    }
}