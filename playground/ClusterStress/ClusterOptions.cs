// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace Resp.benchmark
{
    public partial class Options
    {
        [Option("cluster", Required = false, Default = false, HelpText = "Cluster mode benchmark enable")]
        public bool Cluster { get; set; }

        [Option("shard", Required = false, Default = -1, HelpText = "Restrict benchmark to specific shard")]
        public int Shard { get; set; }

        [Option("replica-reads", Required = false, Default = false, HelpText = "Allow replica reads for cluster mode.")]
        public bool ReplicaReads { get; set; }

        [Option("migrate-freq", Required = false, Default = 0, HelpText = "Used to control frequency of a task that issues migrate command (Only for cluster option).")]
        public int MigrateSlotsFreq { get; set; }

        [Option("migrate-batch", Required = false, Default = 100, HelpText = "Max number of slots picked to migrate from one node to another from background task that executes migrate (Only for cluster option).")]
        public int MigrateBatch { get; set; }

    }
}
