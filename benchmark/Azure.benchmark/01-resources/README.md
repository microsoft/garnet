# 01 - Resources Declaration

One-time deployment of the shared Azure resources the benchmarking environment depends on. Run this layer **first**; the VMSS platform and workload layers consume its outputs.

## Scope

| Concern | Resource |
|---------|----------|
| Networking | VNet, subnets (management + accelerated), NSG |
| Placement | Proximity Placement Group |
| Storage | Storage account + blob container (`node` tarball delivery) |
| Secrets | Key Vault + SSH key material + tools tarball SAS URL |

## Contents

| Path | Purpose |
|------|---------|
| `deploy-common-resources.ps1` | Deploys network (NSG, VNet, proximity group), storage (storage account), and security (Key Vault + SSH keys) resources, and generates the root `vmss-parameters.json` |
| `network/network.bicep` | Network infrastructure template |
| `network/network-parameters.json` | Network deployment parameters |
| `storage/storage.bicep` | Storage account + blob container template (deterministic name, `app=azurebench` tag for discovery) |
| `security/keyvault.bicep` | Key Vault deployment template |
| `security/keyvault.json` | Key Vault deployment parameters |
| `security/manifest.json` | SSH key declarations (`basePath`, `userKeys`, `vmKeys`) |
| `security/*.pub` | Personal + VMSS public keys (git-ignored) |

## Usage

```powershell
# Deploy all shared resources (network + storage + Key Vault + keys; writes ../vmss-parameters.json)
.\01-resources\deploy-common-resources.ps1
```

## Deploy Common Resources

```powershell
.\01-resources\deploy-common-resources.ps1
```

Creates NSG, VNet, a proximity placement group, a storage account, and a Key Vault (copying the manifest public keys into `security/` and uploading the VMSS private key as secret `vmss-ssh-private`). It also publishes a read-only, policy-bound **SAS URL** for the tools tarball blob into the Key Vault (secret `tools-sas-url`) so the VMSS can pull `tools.tar.gz` at boot **without a Storage Blob Data Reader role assignment** (no RBAC required — see [Tools tarball delivery](#tools-tarball-delivery-sas)). Auto-generates `vmss-parameters.json` (written to `02-platform/`) with resource IDs. The storage account and Key Vault names are discovered later by their `app=azurebench` tag / resource-group lookup, so they are **not** written to `vmss-parameters.json`.

### Actions

| Action | Command | Description |
|--------|---------|-------------|
| `deploy` (default) | `.\01-resources\deploy-common-resources.ps1` | Deploys shared resources (network, storage, Key Vault + keys, tools SAS) and generates `vmss-parameters.json`. **Idempotent** — checks the resource group and skips any resource (network, storage, Key Vault, or existing SAS secret) that already exists, with an informational message |
| `stage` | `.\01-resources\deploy-common-resources.ps1 -Action stage -rg <rg>` | Queries existing network resources, copies the manifest-declared public keys from `basePath` (normally `%USERPROFILE%\.ssh`) into the git-ignored `security/` cache, and generates `vmss-parameters.json` (no Azure deployment) |
| `refresh-sas` | `.\01-resources\deploy-common-resources.ps1 -Action refresh-sas -rg <rg>` | Regenerates the tools tarball SAS and refreshes the `tools-sas-url` Key Vault secret (renews the stored access policy expiry). Run before expiry or after rotating the storage account key |

> **`-Region` (optional):** By default the deploy action uses the resource group's own location (`az group show`). Pass `-Region <region>` to deploy the shared resources (NSG, VNet, proximity group, storage account) into a specific region, e.g. `.\01-resources\deploy-common-resources.ps1 -rg garnet-bench-eastcan -Region canadaeast`. The chosen region is written to `vmss-parameters.json` so the VMSS inherits it. Azure resource locations are **immutable** — if the NSG/VNet/PPG already exist in a different region, delete them first, then redeploy. The region must also be listed in the `@allowed` set in `network/network.bicep` and `storage/storage.bicep`.

The VNet contains these subnets:

| Subnet | Prefix | Purpose |
|--------|--------|---------|
| `garnet-subnet` | 10.5.0.0/24 | Management — public IPs, SSH access from corpnet |
| `garnet-acc-subnet` | 10.5.1.0/24 | Data plane — accelerated networking for all VMSS |

Both server and client VMSS share the accelerated networking subnet. Peer discovery uses hostname prefixes to distinguish VMSS membership.

## Configure SSH Keys and Key Vault

Edit `security/manifest.json` to declare your SSH key names and base path:

```json
{
    "basePath": "%USERPROFILE%\\.ssh",
    "userKeys": ["id_ed12182024_desktop", "id_ed121824_notebook"],
    "vmKeys": "id_ed25519_vmss"
}
```

- **`userKeys`** — your personal keys for SSH access into VMs (public keys deployed to `authorized_keys`)
- **`vmKeys`** — the VMSS inter-node key (public key deployed to VMs, private key uploaded to Key Vault for VM-to-VM SSH)

The Key Vault and SSH key material are provisioned as part of `deploy-common-resources.ps1` (see [Deploy Common Resources](#deploy-common-resources)): it copies the manifest `.pub` files into `security/`, deploys the Key Vault, and uploads the `vmKeys` private key as secret `vmss-ssh-private`. All steps are idempotent.

Downstream deployments discover the Key Vault dynamically (by resource group / `app=azurebench` tag), so the vault name is **not** persisted to `manifest.json` or `vmss-parameters.json`:

- **VMSS creation** — `02-platform/manage-vmss.ps1 -Action create` inline-discovers the Key Vault and resolves public keys from the manifest `basePath` (normally `%USERPROFILE%\.ssh`), using `security/*.pub` only as a fallback cache.
- **Pushing keys to live instances** — `02-platform/manage-vmss.ps1 -Action push-keys -VmssName <name>` writes the `security/*.pub` keys to running instances' `authorized_keys` via `az vmss run-command` (no redeployment). *(This replaces the former `deploy-keys.ps1 -Action update`.)*

### Key Vault Naming

Vault names are auto-generated as `kv-{yyyyMMddHHmmss}` to avoid global name collisions and tagged `app=azurebench` for discovery. If a soft-deleted vault with the same name exists, the script attempts to purge it. An existing tagged vault in the resource group is reused instead of creating a new one.

### Tools tarball delivery (SAS)

VMSS instances need the `tools.tar.gz` bundle (the guest-side scripts/config in `02-platform/tools/`), which lives in the storage account's `tools` blob container. Instead of granting the VMSS managed identity a **Storage Blob Data Reader** role assignment — which the deployer's custom role (`Microsoft.Authorization/*/Write` in its NotActions) cannot create — the deploy action publishes a **read-only, policy-bound SAS URL** into the Key Vault:

1. A container **stored access policy** (`vmss-tools-read`, permissions `rl`, expiry `+SasExpiryDays`) is created/updated on the `tools` container. The policy centralizes expiry and revocation.
2. A policy-bound **SAS** is generated against the account key and combined with the blob endpoint into `https://<account>.blob.core.windows.net/tools/tools.tar.gz?<sas>`.
3. The URL is stored as Key Vault secret **`tools-sas-url`**. VMSS instances already have Key Vault access, so they read this secret at boot and `curl` the tarball — no RBAC role assignment required.

Upload the bundle with `02-platform/manage-vmss.ps1 -Action publish-tools` (run it before `-Action create`). Because SAS tokens always expire, run `-Action refresh-sas` to renew the policy expiry and regenerate the secret before the SAS lapses (or after rotating the storage account key). `deploy` skips regeneration if the secret already exists; `refresh-sas` forces it. Tune the lifetime with `-SasExpiryDays` (default 365).

## Region / Location Handling

No script requires a standalone `-Location` parameter. The region is determined once and propagated:

| Script | Behavior |
|--------|----------|
| `deploy-common-resources.ps1` | Prompts for region only when creating a **new** resource group; otherwise reads it from the existing RG via `az group show`. Accepts an optional `-Region <region>` override (see [Deploy Common Resources](#deploy-common-resources)). The Key Vault inherits the same region |
| `02-platform/vmss.bicep` | Receives `location` from `vmss-parameters.json` (written by `deploy-common-resources.ps1`) |

In short: set the region when you create the resource group and everything else inherits it. Use `-Region` on `deploy-common-resources.ps1` only when you need the shared resources in a region different from the resource group's own location.

## Notes on paths after the refactor

- `vmss-parameters.json` is intentionally written to **`02-platform/`** (next to `vmss.bicep`, which consumes it), not to this folder. It is the hand-off artifact from this resources layer to the VMSS platform layer.
- `security/manifest.json` is resolved by scripts in other layers (`03-workload/cluster.ps1`, `02-platform/manage-vmss.ps1`, `03-workload/bench/*`) via the `01-resources/security/manifest.json` path.
