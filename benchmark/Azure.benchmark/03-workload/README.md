# 03 - Benchmark / Cluster Management (Workload)

Workstation-side assets that drive the running platform: forming/tearing down the
storage cluster and executing client benchmarks over SSH. Run this layer **after**
`02-platform/` has provisioned and bootstrapped the VMSS instances.

## Contents

| Path | Purpose |
|------|---------|
| `cluster.ps1` | Cluster lifecycle: start/stop/restart server instances and form the cluster on remote VMs via SSH (invokes VM-side `cluster-deploy.ps1`) |
| `bench/resp-bench.ps1` | Launches [Resp.benchmark](https://github.com/microsoft/garnet) across client VMs via SSH |
| `bench/memtier-bench.ps1` | Drives `memtier_benchmark` across client VMs via SSH |
| `bench/utils.ps1` | Shared helper functions for the benchmark drivers |
| `bench/*.conf` | Key-value configs for SSH targets, benchmark parameters, and workload tuning (`bench.conf`, `memtier.conf`, `read.conf`, `write.conf`) |
| `bench/EXPERIMENTAL_SETUP.md` | Notes on the benchmarking methodology |
| `analysis/*.py` | Python post-processing / plotting of collected results |
| `results/` | Auto-generated per-run output (git-ignored) |

## Usage

```powershell
# Form/refresh the cluster on the server VMSS
.\03-workload\cluster.ps1 -Action start -System valkey -Conf .\02-platform\tools\config\valkey\valkey-cache.conf -InstancePerVm 2 -VmCount <server-vm-count> -Clean

# Run a benchmark from the client VMs
.\03-workload\bench\resp-bench.ps1 -Detail
.\03-workload\bench\memtier-bench.ps1 -Background
```

## Manage Cluster (`cluster.ps1`)

Start or stop the storage cluster on server VMs. Reads connection info from `bench/bench.conf`.

```powershell
# Start a 2-instance-per-node valkey cache cluster (clean deploy)
.\03-workload\cluster.ps1 -Action start -System valkey -Conf .\02-platform\tools\config\valkey\valkey-cache.conf -InstancePerVm 2 -VmCount <server-vm-count> -Clean

# Start garnet with replication (no cluster mode)
.\03-workload\cluster.ps1 -Action start -System garnet -Conf .\02-platform\tools\config\garnet\garnet-cache-replication.conf -InstancePerVm 1 -VmCount <server-vm-count> -NoCluster

# Stop the cluster
.\03-workload\cluster.ps1 -Action stop -System valkey -InstancePerVm 2 -VmCount <server-vm-count>
```

The `start` action automatically ships the local `-Conf` file to every server,
starts the instances, and forms the cluster in one step. Use `-Clean` to wipe data
directories before starting.

`-InstancePerVm` is the number of Garnet/Valkey processes and ports started on each
machine. `-VmCount` is the expected number of VMSS machines. The legacy names
`-ICount` and `-NodeCount` remain aliases.

Before running a cluster action, `cluster.ps1` loads a workstation-local Azure peer
inventory from the repository-root `.peer-cache/` directory (git-ignored). If no matching cache exists,
the cache is older than `-PeerCacheTtlMinutes` (30 minutes by default), or
`-ForcePeerRefresh` is supplied, it uses Azure CLI to enumerate the VMSS instances and
their non-primary (`eth1`) addresses, then saves the result locally. Set
`-PeerCacheTtlMinutes 0` to disable age-based regeneration. A fresh inventory is always
pushed to the coordinator, which validates VMSS identity, subnet membership, count,
and SSH connectivity before caching and using it. If the coordinator rejects a local
cache, `cluster.ps1` regenerates it through Azure once and retries.

Use `-VmssName` and `-ResourceGroup` to select the VMSS explicitly. `-VmssName` can
normally be inferred from a host such as `vm0.ds8server.<region>.cloudapp.azure.com`;
the resource group can be inferred through Azure CLI when the local cache is first
created.

> **🔑 SSH key resolution:** `cluster.ps1` (like `manage-vmss.ps1`) SSHs from your **local machine into the VMs** using your personal keys. It resolves the private key from `01-resources/security/manifest.json` — expanding `basePath` and picking the first existing entry in `userKeys` — falling back to `~/.ssh/id_ed25519` if none is found. Pass `-SshKey <path>` to override. The `vmKeys` entry / Key Vault key is **not** used here; that key is only for **VM-to-VM** (intra-cluster) SSH. `manifest.json` is authored/edited by you; `01-resources/deploy-common-resources.ps1` copies the listed public keys into `security/` and uploads the `vmKeys` private key to Key Vault.

> **⚠️ Azure VPN required:** These commands run locally and SSH into the remote server VMs by their public Azure hostnames. You must be connected to the **Azure VPN** for the local machine to reach and SSH into the remote machines. Example invocations:
>
> ```powershell
> pwsh .\03-workload\cluster.ps1 --serverhost vm1.d128ldsv6server.canadaeast.cloudapp.azure.com --clean --instancespervm 1 --vmcount 4 --system garnet --action start --conf .\02-platform\tools\config\garnet\garnet-aofx16m.conf --replicas 1 --createmanual
> pwsh .\03-workload\cluster.ps1 --serverhost vm1.d128ldsv6server.canadaeast.cloudapp.azure.com --instancespervm 1 --vmcount 4 --system garnet --action stop
> ```

## Run Benchmarks

### Configuration (`bench/bench.conf`)

```ini
# SSH connection
SshUser=guser
ClientMachineHostnames=[vm0.myclient.southcentralus.cloudapp.azure.com]
ClientMachineCount=12   # number of VM instances (vm0..vm11)
Multiplier=1            # benchmark instances per VM

# Benchmark parameters
Server=10.5.1.4         # target server IP
Port=7000
Threads=16
Runtime=60
ClusterBench=true

# Optional workload tuning
# Op=SET                # operation type (GET, SET, MGET, MSET)
# Pool=true             # connection pool per worker
# Broadcast=false       # broadcast requests across pool
# DbSize=1000000
# KeyLength=16
# ValueLength=128
# BatchSize=100
# ExtraArgs=--db-size 1000000
```

SSH keys are resolved automatically from `01-resources/security/manifest.json` (falls back to `~/.ssh/id_ed25519`).

### Running

```powershell
.\03-workload\bench\resp-bench.ps1                                    # inline parallel, totals only
.\03-workload\bench\resp-bench.ps1 -Detail                            # show per-instance results
.\03-workload\bench\resp-bench.ps1 -Background                        # spawn Windows Terminal panes
.\03-workload\bench\resp-bench.ps1 -ConfigFile .\custom.conf -Detail  # custom config
```

| Flag | Behavior |
|------|----------|
| *(none)* | Inline parallel execution, prints TOTAL only |
| `-Detail` | Adds per-instance breakdown to aggregation output |
| `-Background` | Spawns Windows Terminal tabs/panes (2 per tab) for visual inspection |

Before running benchmarks, the script probes the server and displays system info (name, version, OS, CPU count, port, uptime).

The script:
1. SSHs into each client VM and runs `Resp.benchmark` with the configured parameters
2. Output is tee'd to timestamped log files under `03-workload/results/<yyyyMMdd-HHmmss>/`
3. Polls log files until all instances report `Total throughput:` (or a timeout of `runtime + 120s`)
4. Aggregates results across all instances:

```
=== Aggregate Results (20260616-164500) ===
  vm0-myclient-0   1,234.56 Kops/sec |  0.450 GB/s data |  0.520 GB/s wire
  vm1-myclient-1     987.65 Kops/sec |  0.380 GB/s data |  0.440 GB/s wire
  ----------------------------------------------------------------------
  TOTAL              2,222.21 Kops/sec |  0.830 GB/s data |  0.960 GB/s wire
```

`bench/memtier-bench.ps1` drives `memtier_benchmark` with an analogous config/flag surface; `analysis/*.py` post-process the collected `results/` output.

## Notes on paths after the refactor

- `cluster.ps1` and the `bench/*` drivers resolve SSH keys from `../01-resources/security/manifest.json` (relative to this folder / `bench/` respectively).
- `cluster.ps1` defaults its connection config to `bench/bench.conf`; the benchmark drivers default to their sibling `.conf` files.
- Benchmark output is written to `03-workload/results/<run>/` by both drivers and is git-ignored.
- `02-platform/manage-vmss.ps1` dot-sources `bench/utils.ps1` from this layer for shared helpers.
- The VM-side counterparts (`02-platform/tools/cluster/`, `02-platform/tools/bench/`) live under `02-platform/tools/` because they are packaged into `tools.tar.gz` and deployed into the VM guest tree; this layer is the **workstation-side** driver for them.
