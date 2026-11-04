# RBAC Roles and Manual PIM Elevation

Tools storage uses Microsoft Entra ID authentication. Shared Key authorization and
local users are disabled; account keys and account-key-signed SAS cannot be used.
The deployment workflow separates resource creation, publisher access, and VMSS
access. No script activates PIM or signs you in automatically.

## Migrating an existing deployment

**Breaking change:** tools delivery uses Entra ID instead of account keys or
account-key-signed SAS. Shared-resource deployment disables Shared Key and local
users on existing storage accounts as well as new ones. The `refresh-sas` action
and tools-SAS provisioning are removed; existing SAS URLs no longer authorize
downloads once Shared Key is disabled.

Before hardening an existing account, grant the publisher a blob data role and
grant each existing VMSS system-assigned identity **Storage Blob Data Reader** on
the storage account. An administrator can make these assignments separately;
`grant-perms.ps1` provides the publisher grant with the Owner check described below.
Allow the assignments to propagate before switching authentication.

Publish the updated tools bundle using `-Action publish-tools`, then run
`manage-vmss.ps1 -Action install` (copy only) or `-Action deploy` (full deployment)
against each existing VMSS. Both actions download with the VM identity and persist
`TOOLS_BLOB_URL` in `/opt/deploy-actions/storage.env`, enabling subsequent
guest-side `update.ps1 -Fetch` without recreating the VM. New instances receive
the same configuration from cloud-init.

## Identities and roles

| Operation | Identity | Required access |
|-----------|----------|-----------------|
| Deploy shared resources | Signed-in deployer | Resource-management permissions, such as Contributor or the existing TnR DevOps role |
| Run `grant-perms.ps1` | Signed-in deployer | Active, unrestricted **Owner on the resource group** |
| Publish tools | Signed-in publisher | **Storage Blob Data Owner** or **Storage Blob Data Contributor** on the `tools` container or a parent scope |
| Create VMSS with `-GrantStorageAccess` | Signed-in deployer | Deployment permissions plus active, unrestricted **Owner on the resource group** |
| Download tools at boot, or during `deploy` / `install` | VMSS system-assigned identity | **Storage Blob Data Reader** on the storage account |

The scripts check Owner access on the target resource group, including inherited
Owner assignments from parent scopes. Subscription-wide Owner is not required.
Contributor, eligible-only Owner, restricted Owner, and custom delegation do not
satisfy this check. Owner on only the storage account does not cover the group.

Management-plane Owner/Contributor and being the account's creator do not grant
blob data access. Storage Blob Data Contributor is sufficient for publishing;
this setup grants Storage Blob Data Owner as requested, which also allows
managing blob/container permissions. The grant is scoped to the `tools`
container, not the subscription.

## 1. Deploy shared resources

```powershell
pwsh .\01-resources\deploy-common-resources.ps1 -rg <rg>
```

This creates or updates the storage account, `tools` container, network, and Key
Vault. It creates no role assignments. With the default TnR DevOps permissions,
this step does not require PIM.

## 2. Grant publisher access, then publish

Manually activate Owner on the resource group as described below, then run:

```powershell
pwsh .\01-resources\grant-perms.ps1 -rg <rg>
pwsh .\02-platform\manage-vmss.ps1 -rg <rg> -Action publish-tools
```

`grant-perms.ps1`:

1. Prints the Azure CLI account, subscription, and role names.
2. Checks for active, unrestricted Owner access on the resource group,
   including group-based assignments. If missing, exits nonzero with manual PIM
   and validation instructions before changing anything.
3. Discovers the storage account tagged `app=azurebench` and checks that the tools
   container exists. Use `-StorageAccount <name>` to select an account explicitly,
   or `-ContainerName <name>` for a non-default container.
4. Assigns **Storage Blob Data Owner** on that container to the signed-in user.
   If an unrestricted Data Owner or Data Contributor assignment already covers
   publishing, it skips creating another grant.
5. Uploads and deletes a uniquely named temporary validation blob using
   `--auth-mode login`. It retries authorization failures for up to ten minutes
   while data permissions propagate and reports other failures immediately.

New publisher assignments are persistent: they outlive your PIM activation.
Subsequent `publish-tools` calls need no elevation while publisher access remains
active. If the script reuses a temporary existing data-role assignment, that
assignment still expires on its original schedule.

If assignment or verification fails, the script exits nonzero and preserves the
Azure error. A successfully created assignment is not rolled back when verification
fails. If temporary-blob deletion fails, the error names the blob to clean up.

## 3. Deploy VMSS

While Owner on the resource group is active, run:

```powershell
pwsh .\02-platform\manage-vmss.ps1 -rg <rg> -Action create `
  -DeploymentRole server -DeploymentName server-deployment -GrantStorageAccess
```

With `-GrantStorageAccess`, creation performs the same resource-group Owner check as
`grant-perms.ps1`. If it fails, creation exits with manual elevation instructions;
it never requests PIM activation. The existing tools check also verifies that the
published bundle exists using the deployer's Entra login.

The template creates the VMSS system-assigned identity and assigns it **Storage
Blob Data Reader** on the storage account. All instances share that identity. This
grant remains after your Owner activation expires.

Every new or recreated VMSS needs a new Reader assignment and therefore an active
Owner elevation for `create -GrantStorageAccess`. Scaling an existing VMSS does
not require a new grant. Without the flag, creation skips the Owner gate and Reader
assignment; an administrator must grant Reader separately for downloads to work.

## Manual PIM activation

1. Open **Azure Portal > Privileged Identity Management > My roles > Azure
   resources > Eligible assignments**.
2. Find **Owner** at the target **resource group** scope. Select **Activate**,
   provide justification/duration, and complete any required MFA or approval.
   If the role is not eligible, contact your administrator.
3. Confirm that it appears under **Active assignments** and allow time for
   propagation.
4. If Azure CLI reports an authentication challenge, refresh the sign-in and
   select the intended subscription:

   ```powershell
   az login --tenant <tenant-id>
   az account set --subscription <subscription-id>
   az account show --query "{account:user.name,subscription:name}" -o table
   ```

5. Inspect storage readiness, then rerun the original command:

   ```powershell
   pwsh .\01-resources\validate-storage-rbac.ps1 -rg <rg>
   ```

The scripts print instructions with the actual tenant, subscription, and resource
group when those are available. They do not bypass MFA, approval, or tenant policy.
An Owner assignment shown by the check does not guarantee every Azure operation
will succeed: propagation, expiration, deny assignments, and authentication
challenges can still cause authorization failures.

## Validation

The validator reports the signed-in identity/subscription, effective role names,
Shared Key settings, management permissions, blob access, and VMSS identity access.

```powershell
# Read-only readiness and role information
pwsh .\01-resources\validate-storage-rbac.ps1 -rg <rg>

# Confirm publishing access by uploading/deleting a temporary blob
pwsh .\01-resources\validate-storage-rbac.ps1 -rg <rg> -TestWriteAccess

# Inspect a deployed VMSS identity's blob-read assignment
pwsh .\01-resources\validate-storage-rbac.ps1 -rg <rg> -VmssName <vmss-name>
```

Before granting publisher access, failed blob checks can be expected. The
validator's `Create role assignments: PASS` means the permissions API exposes
`roleAssignments/write`; it does not validate delegation conditions or prove
Owner access on the resource group. The grant/create scripts enforce the resource-group Owner
policy separately. VMSS checks inspect role assignments, not a live download
inside the VM.

## Scope, persistence, and troubleshooting

- Recreating the storage account deletes its scoped grants. Repeat
  `grant-perms.ps1` for the new container and grant VMSS Reader on the new account.
- A publisher grant on one account/container does not grant access in another
  resource group. Repeat setup for each deployment.
- Owner elevation alone does not permit uploads: run `grant-perms.ps1` first.
- `TokenIssuedBeforeRevocationTimestamp` / `InteractionRequired` means interactive
  sign-in is required. Refresh Azure CLI authentication; it is not proof of a
  missing role.
- `AuthorizationFailed` on `roleAssignments/write` means Azure rejected the grant.
  Check Owner activation/scope/expiry, propagation, and deny assignments.
- `KeyBasedAuthenticationNotPermitted` means a caller still uses Shared Key or an
  account-key-signed SAS. Use Entra login or managed identity.
