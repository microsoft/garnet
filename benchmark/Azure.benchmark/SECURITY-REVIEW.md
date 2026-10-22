# Security & Open-Source Review — `benchmark/Azure.benchmark`

This document records the pre-publication security review of the Azure benchmark
suite and the actions taken so that nothing personal or account-specific is
committed to the public `microsoft/garnet` repository.

## Summary

No hardcoded secrets, credentials, connection strings, SAS tokens, storage
account keys, private keys, or real subscription/tenant/object IDs are committed.
Runtime authentication uses an Azure managed identity plus Key Vault; subscription
and tenant IDs are resolved dynamically (`az` / IMDS), never baked into source.
The GUIDs that appear in the Bicep are **public Azure built-in role-definition IDs**
(Key Vault Secrets User `4633458b-…`, Storage Blob Data Reader `2a2b9908-…`), not
tenant identifiers. All IP addresses are private RFC1918 benchmark ranges.

The findings below concern **information appropriateness for a public repo** and
copy-paste-risky configuration defaults, not exploitable vulnerabilities.

## Actions taken

1. **Removed the compiled Key Vault ARM template** `01-resources/security/keyvault.json`.
   It was a generated `az bicep build` artifact (note the `_generator` / `templateHash`
   metadata) that had already drifted from its `keyvault.bicep` source. Compiled
   Bicep outputs are now git-ignored (`keyvault.json`, `storage.json`, `network.json`,
   `vmss.json`); the `.bicep` files are the single source of truth. The branch's
   single commit was rewritten so the file never appears in history.
   *Note:* the removed file contained **no** literal subscription/tenant GUID — its
   `tenantId` was the runtime expression `[subscription().tenantId]` — but generated
   artifacts should not be tracked regardless.

2. **Removed the personal alias.** The author's alias no longer appears anywhere.
   The resource group name is supplied by the user (prompted if omitted), the Key
   Vault is timestamp-named (`kv-<timestamp>`) and discovered by its `app=azurebench`
   tag, and the `Owner` resource tag was dropped — so no owner-derived names or tags
   remain to genericize.

3. **Removed personal SSH key names from source control.** The SSH key manifest
   `security/manifest.json` is now **git-ignored** (per-user). A tracked
   `security/manifest.template.json` holds neutral `__SSH_USER_KEY__` /
   `__SSH_VM_KEY__` placeholders; `Get-SshKeyManifest` seeds `manifest.json` from it
   on first run and refuses to proceed while placeholders remain, so no personal key
   names are ever committed.

4. **Removed private-repo / GitHub-PAT support.** The `clone-repos.ps1` PAT-fetch
   and `x-access-token:<PAT>@` URL-rewrite paths existed only to clone Garnet while
   it was private. All benchmarked repos are now public, so this dead path was
   removed along with its docs (`New-GitHubPat.ps1` helper, `ghclone`, `private`
   visibility). This also eliminates the credential-in-URL / credential-in-`.git/config`
   exposure flagged in review. The Key Vault still stores the VMSS SSH private key
   (`vmss-ssh-private`) and the tools SAS URL (`tools-sas-url`); those are unrelated
   and unchanged.

## Placeholders that MUST be filled before deploying

Run `security/initialize-manifest.ps1` to create `security/manifest.json` from the
template and fill in your SSH key names (or copy the template by hand, or let
`deploy-common-resources.ps1` seed it on first run). `manifest.json` is git-ignored,
so filled-in personal values are never committed.

| Placeholder        | Meaning                                                        |
|--------------------|----------------------------------------------------------------|
| `__SSH_USER_KEY__` | Your personal SSH public key name in the manifest `basePath`.   |
| `__SSH_VM_KEY__`   | The VMSS inter-node SSH key name (default `id_ed25519_vmss`).    |

## Residual low-severity items (documented, not exploitable)

These are acceptable for an **isolated, firewalled benchmark harness** but are
copy-paste hazards if reused on an internet-exposed host. Left as-is by design;
noted here for reviewers.

| # | Severity | Location | Note |
|---|----------|----------|------|
| 1 | LOW | `02-platform/tools/bootstrap/setup-ssh-keys.ps1`, most bench/cluster scripts | `StrictHostKeyChecking no` / `UserKnownHostsFile=/dev/null` disables SSH host-key verification. Scoped to the private `10.5.x` benchmark VNet (NSG permits SSH only from corpnet). Consider `accept-new` outside benchmarks. |
| 2 | LOW | `02-platform/tools/config/valkey/*.conf` | `protected-mode no` with no `requirepass`, bound to the private data-plane interface. These are **illustrative benchmark configs, not production-endorsed settings** — they intentionally trade auth for measurement simplicity on an isolated, firewalled harness and must not be used with a public bind. |

## Guidance for `*-parameters.json`

Hand-authored parameter files (e.g. `network-parameters.json`, `vmss-parameters.json`)
are tracked and must contain **only placeholders or non-sensitive defaults** — never
real subscription IDs, object IDs, secrets, or public IPs. Compiled Bicep outputs
(`*.json` produced from a sibling `*.bicep`) remain git-ignored.
