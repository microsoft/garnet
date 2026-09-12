# AzureBench

Automated deployment of benchmarking environments on Azure using VMSS (Virtual Machine Scale Sets).

The repo is organized into three layers, each with its own README containing the detailed reference:

| Layer | Folder | Responsibility |
| --- | --- | --- |
| 1. Resources | [`01-resources/`](01-resources/README.md) | One-time infrastructure: VNet/NSG/subnets, proximity placement, Key Vault + SSH keys |
| 2. Platform | [`02-platform/`](02-platform/README.md) | VMSS deployment (bicep + cloud-init) and lifecycle management |
| 3. Workload | [`03-workload/`](03-workload/README.md) | Cluster start/stop and benchmark execution over SSH |
| VM assets | [`02-platform/tools/`](02-platform/tools/) | Guest-side scripts/config packaged into `tools.tar.gz` and delivered into the VMs (`bootstrap/`, `cluster/`, `bench/`, `config/`) |

## Prerequisites

- Azure CLI (`az`) logged in
- PowerShell 7+
- A resource group (default: `vazois-garnet`)
- SSH key pair for **intra-VMSS** access (VM-to-VM within a scale set, e.g. `id_ed25519_vmss`)
- SSH key pair(s) for **inter-VMSS** access (your desktop → VMs, e.g. `id_ed12182024_desktop`)

## Quick Start

```powershell
# 1. Deploy infrastructure (network + storage + Key Vault + keys)
# See 01-resources/README.md.
pwsh .\01-resources\deploy-common-resources.ps1 `
  --rg <resource-group> `
  --location <region>

# Example:
pwsh .\01-resources\deploy-common-resources.ps1 `
  --rg testrg `
  --location southcentralus

# 2. Publish the tools bundle before creating any VMSS
# This requires the storage account from step 1. VM provisioning cannot fetch
# /home/guser/tools when tools.tar.gz has not been uploaded.
pwsh .\02-platform\manage-vmss.ps1 `
  --rg testrg `
  --action publish-tools

# 3. Deploy server and client VMSS
# See 02-platform/README.md. DeploymentName identifies the Azure deployment
# record; the VMSS resource name comes from the vmssName Bicep parameter.
pwsh .\02-platform\manage-vmss.ps1 `
  --rg testrg `
  --action create `
  --deploymentname server-deployment

pwsh .\02-platform\manage-vmss.ps1 `
  --rg testrg `
  --action create `
  --deploymentname client-deployment

# Alternatively, deploy the VMSS directly through Azure CLI:
az deployment group create --resource-group <rg> --template-file 02-platform\vmss.bicep `
  --parameters @02-platform\vmss-parameters.json --parameters vmssName=<server-name> instanceCount=<n>
az deployment group create --resource-group <rg> --template-file 02-platform\vmss.bicep `
  --parameters @02-platform\vmss-parameters.json --parameters vmssName=<client-name> instanceCount=<n>

# 4. Set up and start Garnet on the server VMs
# serverhost identifies the coordinator VM. This example starts one Garnet
# instance per VM with a clean deployment and the Garnet cache configuration.
# See 03-workload/README.md for additional systems and cluster options.
pwsh .\03-workload\cluster.ps1 `
  --serverhost vm0.server16core.southcentralus.cloudapp.azure.com `
  --clean `
  --icount 1 `
  --system garnet `
  --action start `
  --conf 02-platform/tools/config/garnet/garnet-cache.conf

# 5. Run benchmark from client VMs -> see 03-workload/README.md
pwsh .\03-workload\bench\resp-bench.ps1 `
  --configfile .\03-workload\bench\bench.conf
```

> **VM provisioning takes time:** A newly created VMSS instance may accept SSH before
> cloud-init has finished downloading and installing the tools bundle. Wait for
> provisioning to complete before starting cluster or benchmark operations. On an
> instance, use `cloud-init status --wait` and confirm `/home/guser/tools` exists.

**What you need:**

- 2 VMSS groups: servers (run the storage system) and clients (run the benchmark workload)
- All VMs share an accelerated networking subnet for low-latency data traffic
- A management subnet with public IPs for SSH access from your desktop

**Workflow:** Deploy infra → publish tools → provision VMs → start cluster → run benchmark → collect results.

> **⚠️ Azure VPN:** Steps 4–5 (and VMSS management) run locally and SSH into VMs by their public Azure hostnames — you must be connected to the Azure VPN for those to reach the VMs.

For detailed options, actions, and configuration of each step, follow the per-folder README linked in the table above.
