# Experimental Setup

## Overview

The experiment compared Garnet and Valkey as replicated, sharded clusters at the  
same server-compute scale. The deployment used separate Azure Virtual Machine  
Scale Sets (VMSS) for servers and load-generating clients. Server and client VMs  
shared the accelerated-networking data-plane subnet, while management traffic  
used a separate subnet. The VMSS deployment used cloud-init to install the  
required build and diagnostic tools, clone the system and benchmark  
repositories, build Garnet, Valkey, and `Resp.benchmark`, configure the data  
network, and create the cluster directories.

## Server Infrastructure

The server allocation consisted of 74 `Standard_DS5_v2` VMs from the older
DSv2 family, each with 16 cores and 56 GB of memory. This provided 1,184 server
cores and approximately 4.2 TB of aggregate memory. The use of an older VM SKU
demonstrates that Garnet does not require top-tier hardware to achieve high
throughput, creating an opportunity for substantial infrastructure cost
reduction. Both systems used all 74 VMs, but their process layouts differed to
match their threading models while keeping the total server allocation fixed.

| System | Primary shards | Replicas | Processes per VM | CPU use per process |
| --- | ---: | ---: | ---: | --- |
| Garnet | 37 | 37 | 1 | All 16 VM cores through Garnet’s native multithreading |
| Valkey | 74 | 74 | 2 | 2 cores for interrupt handling and 6 I/O threads |

Garnet therefore placed one process on each VM, with 37 primary-replica pairs  
across the 74 machines. Valkey placed two processes on each VM: one primary and  
one replica, producing 74 primary-replica pairs. This layout gave each Valkey  
process eight effective cores and fully allocated the 16 cores available on  
each server VM.

Cluster instances listened on the accelerated-networking interface (`eth1`),  
starting at port 7000. The cluster deployment divided all 16,384 hash slots  
evenly among the primary shards and configured one replica per primary.

## Persistence and System Configuration

The experiment focused on raw in-memory throughput rather than storage-device  
performance. Garnet ran without its storage tier and directed AOF writes to the  
null device. Valkey used AOF persistence on a `tmpfs` ramdisk mounted at  
`/mnt/ramdisk`; VM initialization sized this ramdisk to 50% of the VM’s memory.  
This removed physical storage from the critical path for both systems while  
retaining their AOF execution paths.

### Garnet AOF

The baseline Garnet configuration used a single physical AOF sublog:

```jsonc
{
  "Address": "$eth1",
  "Port": $port,
  "EnableCluster": true,
  "EnableStorageTier": false,
  "IndexSize": "1g",
  "LogMemorySize": "16g",
  "GossipSamplePercent": 50,
  "MetricsSamplingFrequency": 5,

  "EnableAOF": true,
  "UseAofNullDevice": true,
  "FastAofTruncate": true,
  "CommitFrequencyMs": -1,
  "AofMemorySize": "16g",
  "AofPageSize": "128m"
}
```

### Garnet AOFx8

The AOFx8 variant retained the same settings but striped the AOF across eight  
physical sublogs:

```jsonc
{
  "Address": "$eth1",
  "Port": $port,
  "EnableCluster": true,
  "EnableStorageTier": false,
  "IndexSize": "1g",
  "LogMemorySize": "16g",
  "GossipSamplePercent": 50,
  "MetricsSamplingFrequency": 5,

  "EnableAOF": true,
  "UseAofNullDevice": true,
  "FastAofTruncate": true,
  "CommitFrequencyMs": -1,
  "AofPhysicalSublogCount": 8,
  "AofMemorySize": "16g",
  "AofPageSize": "128m"
}
```

### Valkey AOF on Ramdisk

Valkey disabled snapshotting, enabled AOF without per-write `fsync`, and placed  
its working and AOF files on the ramdisk:

```conf
bind $eth1
port $port
protected-mode no
daemonize yes
logfile /home/guser/valkey-cluster/$port/valkey.log
dir $ramdisk/valkey-cluster/$port

cluster-enabled yes
cluster-config-file nodes-$port.conf
cluster-node-timeout 5000
cluster-announce-ip $eth1

save ""
appendonly yes
appendfsync no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
aof-timestamp-enabled no

maxmemory-policy allkeys-lru

io-threads 6
io-threads-do-reads yes
tcp-backlog 16384
hz 100
```

## Client Infrastructure

The load-generator allocation consisted of 250 VMs distributed across multiple  
Azure SKUs. Every client VM had four cores and between 8 GB and 32 GB of memory,  
for a total of 1,000 client cores and a reported 5,144 GB of aggregate memory.  
Client memory capacity did not play a significant role in the benchmark:
clients were generally lightweight and required little memory to pre-generate
the workload being tested. The large client fleet instead provided enough
independent workers and network connections to scale the workload and saturate
the server cluster.

The benchmark configuration distributed the 250 clients as follows:

| Client VMSS group | VM count |
| --- | ---: |
| `Standard_F4s_v2` (100-instance group) | 100 |
| `Standard_F4s_v2` (25-instance group) | 25 |
| `Standard_E4s_v6` | 25 |
| `Standard_D4s_v6` | 25 |
| `Standard_E4s_v5` | 25 |
| `Standard_D4s_v5` | 25 |
| `Standard_D4lds_v6` | 25 |
| **Total** | **250** |

## Workload Configuration

The workload was generated with `Resp.benchmark` in cluster mode. One benchmark  
process ran on each client VM, and each process used 64 workers. The complete  
client fleet therefore provided 16,000 concurrent workers.

| Parameter | Value |
| --- | ---: |
| Client VMs / benchmark processes | 250 |
| Workers per benchmark process | 64 |
| Total workers | 16,000 |
| Connections per worker per shard | 2 |
| Total logical keys | Approximately 1,000,000 |
| Key length | 8 bytes |
| Value length (payload) | 8 or 100 bytes |
| Batch size | 64, 256, 1,024, or 4,096 operations |
| Replica operations | SET: 0%; GET: primary and replicas |

Keys were sharded equally across the primary instances. The per-shard database  
size was adjusted for each topology:

| System topology | Primary shards | Keys per shard | Approximate total keys |
| --- | ---: | ---: | ---: |
| Garnet | 37 | 27,028 | 1,000,036 |
| Valkey | 74 | 13,514 | 1,000,036 |

Both products therefore operated on the same approximately one-million-key  
logical dataset despite using different shard counts. The benchmark used a  
connection pool for every worker; in this mode each worker maintained two  
connections to every shard. Broadcast mode was not enabled.

### SET and GET Experiments

Both the SET and GET workloads executed against a pre-loaded database. The
workload was pre-generated using batch sizes of 64, 256, 1,024, and 4,096
operations and payloads of 8 and 100 bytes. These variations measured the
systems across different request-grouping and data-transfer demands. Batching
emulated adaptive pipelining, in which multiple requests issued by different
applications are grouped and sent efficiently over established connections.

The SET experiments sent all write operations to primary shards. Replicas were
not targeted because they are read-only and apply writes through replication
from their corresponding primaries.

The GET experiments targeted both primary shards and replicas. This allowed the
read workload to use the full replicated cluster rather than limiting requests
to primary shards.