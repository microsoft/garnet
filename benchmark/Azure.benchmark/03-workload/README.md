# 03 - Benchmark / Cluster Management (Workload)

Workstation-side assets that drive the running platform: forming/tearing down the
storage cluster and executing client benchmarks over SSH. Run this layer **after**
`02-platform/` has provisioned and bootstrapped the VMSS instances.

## Contents

| Path | Purpose |
|------|---------|
| `cluster.ps1` | Cluster lifecycle and declarative create/stop on remote VMs |
| `benchmark.ps1` | Runs a declarative benchmark through the existing RESP or memtier driver |
| `bench/resp-bench.ps1` | Launches [Resp.benchmark](https://github.com/microsoft/garnet) across client VMs via SSH |
| `bench/memtier-bench.ps1` | Drives `memtier_benchmark` across client VMs via SSH |
| `bench/utils.ps1` | Shared helper functions for the benchmark drivers |
| `bench/*-cluster.conf`, `bench/*-standalone.conf` | Key-value configs for SSH targets, benchmark parameters, and workload tuning; `bench-tls-cluster.conf` enables TLS |
| `bench/EXPERIMENTAL_SETUP.md` | Notes on the benchmarking methodology |
| `analysis/*.py` | Python post-processing / plotting of collected results |
| `results/` | Auto-generated per-run output (git-ignored) |

## Usage

```powershell
# Form/refresh the cluster on the server VMSS
.\03-workload\cluster.ps1 -Action start -System valkey -Conf .\02-platform\tools\config\valkey\valkey-cache.conf -InstancePerVm 2 -VmCount <server-vm-count> -Clean

# Run a benchmark from the client VMs
.\03-workload\bench\resp-bench.ps1 -ClientHosts <client-host> -Detail
.\03-workload\bench\memtier-bench.ps1 -Background
```

## Manage Cluster (`cluster.ps1`)

Start or stop the storage cluster on server VMs. Declarative runs resolve SSH
placement through VMSS selection. Legacy runs require `-ServerHost` (or `Host`
in an optional key=value `-ConfigFile`); RESP workload files contain no SSH settings.

```powershell
# Start a 2-instance-per-node valkey cache cluster (clean deploy)
.\03-workload\cluster.ps1 -Action start -System valkey -Conf .\02-platform\tools\config\valkey\valkey-cache.conf -InstancePerVm 2 -VmCount <server-vm-count> -Clean

# Start garnet with replication (no cluster mode)
.\03-workload\cluster.ps1 -Action start -System garnet -Conf .\02-platform\tools\config\garnet\garnet-cache-replication.conf -InstancePerVm 1 -VmCount <server-vm-count> -NoCluster

# Start Garnet with TLS. Server-role VMs retrieve the server PFX/password and CA
# certificate from Key Vault; certificate secrets are not stored in this config.
.\03-workload\cluster.ps1 -Action start -System garnet -Tls -Conf .\02-platform\tools\config\garnet\garnet-cache-tls.conf -InstancePerVm 1 -VmCount <server-vm-count> -Clean

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

For `-System garnet -Tls`, each server refreshes role-specific certificate material
from Key Vault into `/opt/azurebench/tls`, generates a mode-`0600` per-instance
Garnet configuration, and validates peer certificates against the generated CA using
the stable `azurebench-server` target name. Client certificates are not required by
this server-start mode.

Use `-VmssName` and `-ResourceGroup` to select the VMSS explicitly. `-VmssName` can
normally be inferred from a host such as `vm0.ds8server.<region>.cloudapp.azure.com`;
the resource group can be inferred through Azure CLI when the local cache is first
created.

> **🔑 SSH key resolution:** `cluster.ps1` SSHs from your **local machine into the VMs** using your personal keys. Both declarative and legacy operations resolve the private key from `01-resources/security/manifest.json`, expanding `basePath` and picking the first existing entry in `userKeys`. A missing/unconfigured manifest or missing declared keys produces a configuration error; there is no fallback to an undeclared key. Pass `-SshKey <path>` to override. The `vmKeys` entry / Key Vault key is **not** used here; that key is only for **VM-to-VM** (intra-cluster) SSH. Initialize the git-ignored manifest once per checkout using `01-resources/security/initialize-manifest.ps1 -SshUserKey <personal-key-names> -SshVmKey <existing-vm-key-name>`. `01-resources/deploy-common-resources.ps1` copies the listed public keys into `security/` and uploads the `vmKeys` private key to Key Vault.

> **⚠️ Azure VPN required:** These commands run locally and SSH into the remote server VMs by their public Azure hostnames. You must be connected to the **Azure VPN** for the local machine to reach and SSH into the remote machines. Example invocations:
>
> ```powershell
> pwsh .\03-workload\cluster.ps1 --serverhost vm1.d128ldsv6server.canadaeast.cloudapp.azure.com --clean --instancespervm 1 --vmcount 4 --system garnet --action start --conf .\02-platform\tools\config\garnet\garnet-aofx16m.conf --replicas 1 --createmanual
> pwsh .\03-workload\cluster.ps1 --serverhost vm1.d128ldsv6server.canadaeast.cloudapp.azure.com --instancespervm 1 --vmcount 4 --system garnet --action stop
> ```

## Declarative cluster definitions

`cluster.ps1` accepts the cluster JSON files in `../templates/` through `-Definition`
(alias `-ClusterDefinition`). Commands below run from the `Azure.benchmark` root.

```powershell
# Destructive: stop and clean the declared instances, start them, then form the cluster.
.\03-workload\cluster.ps1 --action create --definition .\templates\cluster-explicit.template.json --rg <resource-group> --vmssname d128ldsv6

# Kill only the declared server instances; do not clean data or touch client vm2.
.\03-workload\cluster.ps1 --action stop --definition .\templates\cluster-explicit.template.json --rg <resource-group> --vmssname d128ldsv6

# Interactive: list VMSS in the resource group, then select one by index or name.
.\03-workload\cluster.ps1 --action create --definition .\templates\cluster-auto.template.json --rg <resource-group>
```

Update the VM tools bundle before using these actions; it includes the shared
`cluster-utils.ps1` helper and scoped instance management. `create` preflights SSH,
tool support, and each declared VM's `eth1` address before any cleanup. Azure
inventory resolves the environment; peer caches do not add nodes to the
declaration. Only the listed VMs participate. Instance
configuration content is shipped from the workstation. Setup verifies all slots,
each declared role, and replica links/full-sync completion before reporting success.
The final setup summary lists each VM's endpoint, role, primary, slots, health, and
replica sync status. It reports `Cluster is UP` only after these checks and a
successful `PING` on every node.

`create` always cleans the selected system's per-port instance directories under
the managed home, ramdisk, NVMe, and `/dev/shm` roots. Other ports are left alone.
`stop` confirms termination without requiring the referenced instance config file
to remain available. A failed operation is reported as an error; already started
instances are not automatically rolled back and can be stopped with the same
definition.

`-SshKey`, `-SshUser`, and Garnet `-Tls` remain available. Do not combine a definition
with legacy topology/discovery flags such as `-System`, `-Conf`, `-VmCount`,
`-Replicas`, or `-Clean`. Existing `start`, `stop`, and `restart` usage without
`-Definition` is unchanged.

- `cluster-explicit.template.json`: primary `vm0` owns all slots (`0..16383`);
  `vm1` is its replica.
- `cluster-auto.template.json`: the same VM roles, with `auto` assigning all slots
  to the sole primary, `vm0`.
- `benchmark-cluster.json`: one cluster benchmark process on client `vm2`, targeting
  primary `vm0`, using `bench-cluster.conf`.
- `benchmark-standalone.json`: one standalone benchmark process on client `vm2`,
  targeting `vm0`, using `bench-standalone.conf`.

Templates contain no `VmDomain` or `SshUser`. Provide `-ResourceGroup` (alias `-rg`)
and optionally `-VmssName`; omitting the name lists available VMSS and prompts.
Exactly one VMSS must be selected. `-NonInteractive` requires an explicit name.
Azure login and read access to the VMSS and its instance public IPs are required.
The shared `02-platform/vmss-utils.ps1` helper is also used by `manage-vmss.ps1`.

Declared VM names match the first DNS label of Azure-reported instance FQDNs.
The tool does not construct domains from the VMSS name or assume VM instance IDs
match DNS labels. Missing or ambiguous FQDNs fail before any cleanup. SSH uses the
VMSS admin username unless `-SshUser` overrides it, and workstation keys still
come from `01-resources/security/manifest.json`.

Older declarations containing `VmDomain` and `SshUser` still work without Azure
selection parameters. Supplying `-ResourceGroup`/`-VmssName` switches them to
VMSS resolution, with Azure supplying the username unless explicitly overridden.

`InstanceConfig` paths are relative to the declaration file on the workstation. Server instances
use the declared port and their VM's internal data-plane address; benchmark
traffic likewise uses the seed VM's internal data-plane address, not its public
SSH address. SSH keys follow the existing key-resolution convention.

Cluster templates use `Primaries` and `Replicas` groups. Each group has a nonempty
`VMs` array. A primary group's `Slots` is `"auto"` or an array of inclusive ranges;
explicit ranges require exactly one VM in the group. All VMs in a replica group
replicate its scalar `Primary`. Replicas can be omitted or an empty array.
Groups expand into the existing per-node representation. Existing `Nodes`
definitions remain supported, but cannot be combined with grouped definitions.

Cluster examples run one server instance per VM. Primary slot assignments must
cover `0..16383` without overlaps or gaps; replicas reference a declared primary
and do not own slots. The examples demonstrate all-explicit or all-`auto` primary
assignments; mixing explicit and `auto` assignments is rejected.
Automatic division spans all primary VMs, preserving group order and VM order
within each group. Earlier primaries receive one extra slot if there is a
remainder. The single-primary auto example assigns `0..16383` to `vm0`.

For benchmarks, the declaration owns VM placement and target endpoint.
Those settings override the corresponding values in the referenced workload
configuration, including its machine counts and multiplier. Repeated client VM
entries are invalid: each declared client VM runs one benchmark instance.
For cluster benchmarks, the target is a seed for discovery, not a restriction to
that shard. Standalone benchmarks connect directly to the target without discovery.
Benchmark templates likewise leave environment selection outside JSON.
The runner reuses the shared resolver separately for client and target VMSS when
they differ, using the same resource group.

## Declarative benchmarks

```powershell
.\03-workload\benchmark.ps1 --definition .\templates\benchmark-cluster.json --rg <resource-group> --vmssname d128ldsv6

# Standalone RESP workload, without cluster discovery.
.\03-workload\benchmark.ps1 --definition .\templates\benchmark-standalone.json --rg <resource-group> --vmssname d128ldsv6

# Resolve and inspect the plan without loading keys or launching a benchmark.
.\03-workload\benchmark.ps1 --definition .\templates\benchmark-cluster.json --rg <resource-group> --vmssname d128ldsv6 --dryrun
```

`System` selects `Resp.benchmark` or `memtier_benchmark`. `InstanceConfig` points to
the native RESP JSON workload configuration (or memtier's key=value configuration),
resolved relative to the JSON file.
Both RESP templates use `System: "Resp.benchmark"`: `"ClusterBench": true` in
`*-cluster.conf` selects its `ClusterBench` implementation, while
`"ClusterBench": false` in `bench-standalone.conf` selects the standalone RESP benchmark.
The standalone profile disables the cluster worker pool and fan-out and contains
no cluster-only options. It uses `"LSet": true` for single-key preload, which also
avoids cross-slot MSET when targeting one cluster primary.
`memtier-cluster.conf` retains `Cluster=true`
for the separate memtier driver.
Standalone offline results report Kops/sec; data/wire bandwidth is omitted because
that benchmark implementation does not emit those metrics.
`Target` declares the seed VM and port; each entry in `Instances` contains one
unique `Vm`. The runner resolves SSH endpoints and usernames through VMSS selection
and obtains the target's `eth1` address through the existing cluster helper.
`-TargetVmssName` selects a separate server VMSS; by default it uses the client VMSS.
`-SshUser`, `-TargetSshUser`, and `-SshKey` override the discovered connection settings.
`-NonInteractive` requires an explicit client VMSS, just as for cluster execution.

The runner invokes `bench/resp-bench.ps1` or `bench/memtier-bench.ps1` inline.
Their workload options, TLS checks, logs, progress, and aggregation remain in use.
The declaration overrides client host/count/multiplier and target host/port settings.
Each client runs exactly one workload instance; RESP validates the native config
on every client without launching a preliminary workload. The file is forwarded
byte-for-byte, with CLI overrides only for target host/port and optional TLS.
Temporary remote config files are removed on exit, including client errors.
New native benchmark options require no Azure forwarding-map changes.
All clients are checked for the selected
benchmark executable before workloads start. Failed instances return an error
instead of reporting declarative completion; logs remain under `03-workload/results`.
Declarative `-Verbose` prints captured benchmark-client stdout/stderr, grouped by VM,
after each phase completes, without opening terminal panes. Failed client output
is printed even without `-Verbose`. This is not a stream of Garnet server logs.
`-Detail`/`-Monitor` apply to RESP, `-SkipLoad` to memtier, and `-Tls` to either.
Memtier still performs its normal load phase unless skipped by the flag or profile.
Dry runs resolve Azure inventory and the target address, but do not launch workloads.

## Run Benchmarks

### Configuration (`bench/bench-cluster.conf`)

```json
{
  "Address": "10.5.1.4",
  "Port": 7000,
  "NumThreads": [16],
  "RunTime": 60,
  "ClusterBench": true,
  "UseFixedWorkerPool": true,
  "UseParallelFanOut": false,
  "Op": "SET",
  "DbSize": 1000000,
  "KeyLength": 16,
  "ValueLength": 128,
  "BatchSize": [100]
}
```

RESP configs use native `Options.cs` property names, including arrays and enum
strings. Defaults are provided by the benchmark; explicit CLI overrides take
precedence over the file. `ExtraArgs` is removed: set its native options directly.
Old RESP key=value files must be converted (`Threads` becomes `NumThreads`,
`Runtime` becomes `RunTime`, `Server` becomes `Address`, `Tls` becomes `EnableTLS`).
Memtier retains its existing key=value format.

Client placement and SSH settings belong in the declaration/VMSS selection or
driver parameters, never in the native workload file. For direct driver use,
pass `-ClientHosts`, or `-ClientMachineHostnames` with `-ClientMachineCount` and
optional `-Multiplier`. SSH keys are resolved automatically from
`01-resources/security/manifest.json` (falls back to `~/.ssh/id_ed25519`).

### Running

```powershell
# Direct driver examples require explicit client placement:
$PSDefaultParameterValues['resp-bench.ps1:ClientHosts'] = @('vm0.myclient.example.com')
.\03-workload\bench\resp-bench.ps1                                    # inline parallel, totals only
.\03-workload\bench\resp-bench.ps1 -Detail                            # show per-instance results
.\03-workload\bench\resp-bench.ps1 -Background                        # spawn Windows Terminal panes
.\03-workload\bench\resp-bench.ps1 -ConfigFile .\custom.conf -Detail  # custom config
.\03-workload\bench\resp-bench.ps1 -ConfigFile .\03-workload\bench\bench-tls-cluster.conf -Detail
.\03-workload\bench\resp-bench.ps1 -ConfigFile .\03-workload\bench\bench-standalone.conf -Detail
.\03-workload\bench\resp-bench.ps1 -Tls -Detail                       # force TLS for this run
```

| Flag | Behavior |
|------|----------|
| *(none)* | Inline parallel execution, prints TOTAL only |
| `-Detail` | Adds per-instance breakdown to aggregation output |
| `-Background` | Spawns Windows Terminal tabs/panes (2 per tab) for visual inspection |
| `-Tls` | Enables AzureBench TLS, overriding `EnableTLS=false` or an omitted key |

Before running benchmarks, the script probes the server and displays system info (name, version, OS, CPU count, port, uptime).

TLS runs require client VMs deployed with `deploymentRole=client`. The driver
validates `/opt/azurebench/tls/client.pfx`, `client.password`, `ca.crt`, and
`metadata.json` before starting. Resp.benchmark reads the PFX password from the
protected password file, validates the server certificate against `ca.crt`, and
requires its identity to match `TlsHost`. Auxiliary `INFO`, `CLUSTER NODES`, and
`DBSIZE` probes use `redis-cli-tls` with the same CA and SNI. The default Garnet
TLS configuration performs server-auth TLS; it does not require the client
certificate at the server.

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

For a TLS-enabled Garnet cluster, use the dedicated memtier configuration:

```powershell
.\03-workload\bench\memtier-bench.ps1 -ConfigFile .\03-workload\bench\memtier-tls.conf
```

The client VMs must use `deploymentRole=client` and contain
`/opt/azurebench/tls/ca.crt` plus matching `metadata.json`. The same
`memtier_benchmark` binary handles plaintext and TLS; it is built with OpenSSL
support and uses `--tls`, `--cacert`, and `--sni`. Because memtier sends SNI
but does not itself enforce certificate-name matching, the drivers first use
`openssl s_client -verify_hostname` to validate the endpoint against both the CA and
`TlsHost`. The current Garnet configuration uses server-auth TLS, so memtier
does not send a client certificate and never uses `--tls-skip-verify`.

## Notes on paths after the refactor

- `cluster.ps1` and the `bench/*` drivers resolve SSH keys from `../01-resources/security/manifest.json` (relative to this folder / `bench/` respectively).
- `cluster.ps1` accepts an optional legacy SSH connection file; it does not read RESP workload JSON. The benchmark drivers default to `bench-cluster.conf` and `memtier-cluster.conf`.
- Benchmark output is written to `03-workload/results/<run>/` by both drivers and is git-ignored.
- `02-platform/manage-vmss.ps1` dot-sources `bench/utils.ps1` from this layer for shared helpers.
- The VM-side counterparts (`02-platform/tools/cluster/`, `02-platform/tools/bench/`) live under `02-platform/tools/` because they are packaged into `tools.tar.gz` and deployed into the VM guest tree; this layer is the **workstation-side** driver for them.
