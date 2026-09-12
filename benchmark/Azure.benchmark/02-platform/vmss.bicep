/* VMSS-PARAMETERS.JSON inputs */
param location string
param nsgId string
param subnetName string
param accSubnetName string
param proximityId string = ''
param vnetName string

param vmssName string
param instanceCount int

// Server and client VMSS share accSubnet for their eth1 (data-plane) NICs; they are
// kept separate at runtime by cluster-deploy.ps1, which filters discovered peers by
// VMSS hostname prefix. (Subnet-level role separation is intentionally not modeled.)

@allowed([
  'linux'
  'windows'
])
param operatingSystem string

@allowed([
  'Premium_LRS'
  'Premium_ZRS'
  'Standard_LRS'
  'UltraSSD_LRS'
])
param osDiskType string

////////////////////
/// SKU Options ////
@description('VM family with supported core counts. Pick a family, then choose vmCores from the listed options.')
@allowed([
  // ===== Intel =====
  'DSv2: [8, 16]'
  'Fs_v2: [8, 32, 64, 72]'
  'Bs_v2: [2, 4, 8, 16, 32]'
  'E_v3: [8, 20, 64]'
  'E_v4: [8, 32, 48, 64]'
  'E_v5: [8, 32, 48, 64, 96]'
  'Es_v3: [8, 32, 48, 64]'
  'Es_v4: [8, 32, 48, 64]'
  'Es_v5: [8, 32, 48, 64, 96]'
  'Es_v6: [8, 32, 48, 64, 96]'
  'Ds_v3: [8, 32, 48, 64]'
  'Ds_v4: [8, 32, 48, 64]'
  'Ds_v5: [8, 32, 48, 64, 96]'
  'Ds_v6: [8, 32, 48, 64, 96]'
  'Dds_v6: [8, 32, 48, 64, 96]'
  'Dds_v7: [8, 32, 48, 64, 96]'
  'Dlds_v6: [8, 32, 48, 64, 96]'
  'Dlds_v7: [8, 32, 48, 64, 96]'
  'Ls_v3: [8, 16, 32, 48, 64, 80]'
  'Ls_v4: [2, 4, 8, 16, 32, 48, 64, 80, 96]'
  // ===== AMD =====
  'Bas_v2: [2, 4, 8, 16, 32]'
  'Fas_v6: [8, 32,64]'
  'Fas_v7: [8, 32,64,80]'
  'Eas_v7: [8, 32,64,80]'
  'Las_v3: [8, 16, 32, 48, 64, 80]'
  'Las_v4: [2, 4, 8, 16, 32, 48, 64, 80, 96]'
  // ===== ARM (Ampere) =====
  'Dpls_v5: [8, 32, 64]'
  'Dpls_v6: [8, 32, 48, 64, 96]'
  'Dps_v5: [8, 32, 64]'
  'Dps_v6: [8, 32, 48, 64, 96]'
  'Dpds_v5: [8, 32, 64]'
  'Dpds_v6: [8, 32, 64, 96]'
  'Eps_v5: [8, 20, 32]'
  'Eps_v6: [8, 32, 48, 64, 96]'
  'Epds_v5: [8, 32]'
  'Epds_v6: [8, 32, 64, 96]'
])
param vmFamily string

@description('Number of vCPUs. Enter one of the core counts shown in brackets [] next to your chosen vmFamily above.')
@metadata({
  hint: 'Valid options are listed in the vmFamily selection. Invalid choices will be rejected by Azure.'
})
param vmCores int

@description('Availability zone strategy: "single" pins all instances to zone 1 and uses a Proximity Placement Group for lowest inter-node latency; "all" spreads instances across zones 1, 2 and 3 for higher resilience/capacity (no PPG, since a PPG requires a single zone); "none" assigns no availability zone (required for regions without AZ support, e.g. canadaeast) while still using a Proximity Placement Group for low latency.')
@allowed([
  'single'
  'all'
  'none'
])
param zoneStrategy string = 'single'

@description('Enable Proximity Placement Group for lowest inter-node latency. Set to false to deploy without a PPG (e.g. when no PPG exists in the region, or to allow the platform to spread instances for better allocation success). Ignored when zoneStrategy is "all" (which never uses a PPG).')
param enableProximityPlacement bool = true

// Extract the actual family name by splitting on ':'
// e.g. 'Dps_v6: [32, 48, 64, 96]' → 'Dps_v6'
var familyName = split(vmFamily, ':')[0]

// DSv2 uses model numbers instead of core counts (DS5_v2 = 16 cores)
// Build the full SKU name: Standard_<series><cores><suffix>_v<gen>
// e.g. familyName "Dps_v6" + cores 64 → series "D" + suffix "ps_v6" → "Standard_D64ps_v6"
// e.g. familyName "Fs_v2" + cores 72 → series "F" + suffix "s_v2" → "Standard_F72s_v2"
var seriesLetter = substring(familyName, 0, 1)
var suffixAndVersion = substring(familyName, 1, max(0, length(familyName) - 1))
var vmSKU = familyName == 'DSv2' ? 'Standard_DS5_v2' : 'Standard_${seriesLetter}${vmCores}${suffixAndVersion}'

// ARM-based SKUs do not support Trusted Launch.
// x64 families: E_v3, Fs_v2, Fas_v6, Ds_v6, Dlds_v6, Es_v6, DSv2
var x64Families = ['E_v3', 'E_v4', 'E_v5', 'Fs_v2', 'Fas_v6', 'Ds_v6', 'Dds_v6', 'Dlds_v6', 'Es_v6', 'Dds_v7', 'Dlds_v7', 'DSv2', 'Bs_v2', 'Bas_v2', 'Las_v3', 'Ls_v3', 'Las_v4', 'Ls_v4']
var supportsTrustedLaunch = contains(x64Families, familyName)

// Non-'s' E-series (E_v3/E_v4/E_v5) do not support Premium storage; fall back
// the OS disk to Standard_LRS when a Premium type was requested for them.
var nonPremiumStorageFamilies = ['E_v3', 'E_v4', 'E_v5']
var effectiveOsDiskType = (contains(nonPremiumStorageFamilies, familyName) && startsWith(osDiskType, 'Premium')) ? 'Standard_LRS' : osDiskType

var securityProfileConfig = supportsTrustedLaunch
  ? {
      securityType: 'TrustedLaunch'
      uefiSettings: {
        secureBootEnabled: true
        vTpmEnabled: true
      }
    }
  : {
      securityType: 'Standard'
    }
//////////////////

@allowed([
  'ReadOnly'
  'ReadWrite'
  'None'
])
param diskCaching string = 'ReadWrite'

@description('Optional data disk size in GB. Set to 0 to skip attaching a data disk.')
param dataDiskSizeGB int = 0

@description('Storage type for the data disk.')
@allowed([
  'Premium_LRS'
  'UltraSSD_LRS'
])
param dataDiskType string = 'UltraSSD_LRS'

@description('IOPS for the data disk (Ultra SSD only, ignored for Premium).')
param dataDiskIOPS int = 4000

@description('Throughput in MB/s for the data disk (Ultra SSD only, ignored for Premium).')
param dataDiskMBps int = 125

param adminUsername string = 'guser'
@description('Windows computer name prefix. Used only when operatingSystem = windows.')
param computerName string = vmssName

@description('Windows admin password. Required when operatingSystem = windows.')
@secure()
param adminPassword string = ''

@description('Key Vault name for storing VMSS SSH private key and GitHub PAT. Leave empty to skip.')
param keyVaultName string = 'garnet-kv'

@description('Storage account name for tools tarball delivery. Leave empty to skip the RBAC grant.')
param storageAccountName string = ''

@description('Key Vault secret name holding the tools tarball SAS URL that VMSS instances fetch at boot.')
param toolsSasSecretName string = 'tools-sas-url'

///////////////////////////////////////////////////////
/////////////////// OS Options ////////////////////////
@description('Linux VM image. Used only when operatingSystem = linux.')
@allowed([
  {
    publisher: 'Canonical'
    offer: 'ubuntu-24_04-lts'
    sku: 'server'
    version: 'latest'
  }
  {
    publisher: 'microsoftcblmariner'
    offer: 'azure-linux-3'
    sku: 'azure-linux-3-gen2'
    version: 'latest'
  }
  {
    publisher: 'microsoftcblmariner'
    offer: 'azure-linux-3'
    sku: 'azure-linux-3-arm64'
    version: 'latest'
  }
  {
    publisher: 'microsoftcblmariner'
    offer: 'cbl-mariner'
    sku: 'cbl-mariner-2-gen2'
    version: 'latest'
  }
  {
    publisher: 'Canonical'
    offer: '0001-com-ubuntu-server-jammy'
    sku: '22_04-lts-gen2'
    version: 'latest'
  }
  {
    publisher: 'Canonical'
    offer: '0001-com-ubuntu-server-jammy'
    sku: '22_04-lts-arm64'
    version: 'latest'
  }
])
param linuxImage object

@description('Windows VM image. Used only when operatingSystem = windows.')
param windowsImage object = {
  publisher: 'MicrosoftWindowsServer'
  offer: 'WindowsServer'
  sku: '2022-datacenter-g2'
  version: 'latest'
}

var selectedImage = operatingSystem == 'linux' ? linuxImage : windowsImage

var storageProfileConfig = {
  osDisk: operatingSystem == 'linux'
    ? {
        createOption: 'FromImage'
        caching: diskCaching
        managedDisk: {
          storageAccountType: effectiveOsDiskType
        }
        diskSizeGB: 128
      }
    : {
        createOption: 'FromImage'
        managedDisk: {
          storageAccountType: effectiveOsDiskType
        }
        diskSizeGB: 128
      }
  imageReference: {
    publisher: selectedImage.publisher
    offer: selectedImage.offer
    sku: selectedImage.sku
    version: selectedImage.version
  }
  dataDisks: dataDiskSizeGB > 0
    ? [
        {
          lun: 0
          createOption: 'Empty'
          diskSizeGB: dataDiskSizeGB
          managedDisk: {
            storageAccountType: dataDiskType
          }
          caching: 'None'
          diskIOPSReadWrite: dataDiskType == 'UltraSSD_LRS' ? dataDiskIOPS : null
          diskMBpsReadWrite: dataDiskType == 'UltraSSD_LRS' ? dataDiskMBps : null
        }
      ]
    : []
}
///////////////////////////////////////////////////////

/////////////// LINUX CONFIG OPTIONS ///////////////////
var isAzureLinux = linuxImage.publisher == 'microsoftcblmariner'

// Load cloud-config templates and inject keyVaultName + tools SAS secret name
var cloudInitAzureLinuxRaw = loadTextContent('cloud-config-azurelinux.yml')
var cloudInitUbuntuRaw = loadTextContent('cloud-config.yml')
var cloudInitAzureLinuxFinal = replace(replace(cloudInitAzureLinuxRaw, '__KEYVAULT_NAME__', keyVaultName), '__TOOLS_SAS_SECRET__', toolsSasSecretName)
var cloudInitUbuntuFinal = replace(replace(cloudInitUbuntuRaw, '__KEYVAULT_NAME__', keyVaultName), '__TOOLS_SAS_SECRET__', toolsSasSecretName)

var cloudInitUbuntu = base64(cloudInitUbuntuFinal)
var cloudInitAzureLinux = base64(cloudInitAzureLinuxFinal)
var cloudInitData = isAzureLinux ? cloudInitAzureLinux : cloudInitUbuntu

param sshPublicKeys array
////////////////////////////////////////////////////////

////////////////////// NETWORK CONFIG OPTIONS //////////////////////////
var publicIPAddressName = '${vmssName}-PublicIP'
var dnsLabelPrefix = vmssName
var zones = zoneStrategy == 'all' ? ['1', '2', '3'] : zoneStrategy == 'none' ? [] : ['1']
var useProximityGroup = zoneStrategy != 'all' && enableProximityPlacement && !empty(proximityId)
var nicName = '${vmssName}-nic'
var accNicName = '${vmssName}-acc-nic'
var dataSubnetName = accSubnetName

var ipTagsProfile = {
  ipTagType: 'FirstPartyUsage'
  tag: '/NonProd'
}

resource vnet 'Microsoft.Network/virtualNetworks@2023-04-01' existing = {
  name: vnetName
}

var tagsProfile = {
  Environment: '/NonProd'
  Owner: 'vazois'
  Root: vmssName
}

var networkProfileConfig = {
  networkInterfaceConfigurations: [
    {
      name: nicName
      properties: {
        primary: true
        ipConfigurations: [
          {
            name: 'ipconfig1'
            properties: {
              subnet: {
                id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnet.name, subnetName)
              }
              privateIPAddressVersion: 'IPv4'
              publicIPAddressConfiguration: {
                name: publicIPAddressName
                sku: {
                  name: 'Standard'
                }
                properties: {
                  publicIPAddressVersion: 'IPv4'
                  dnsSettings: {
                    domainNameLabel: dnsLabelPrefix
                  }
                  ipTags: [ipTagsProfile]
                }
              }
            }
          }
        ]
        networkSecurityGroup: {
          id: nsgId
        }
      }
    }
    {
      name: accNicName
      properties: {
        primary: false
        enableAcceleratedNetworking: true
        ipConfigurations: [
          {
            name: 'ipconfig2'
            properties: {
              subnet: {
                id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnet.name, dataSubnetName)
              }
            }
          }
        ]
        networkSecurityGroup: {
          id: nsgId
        }
      }
    }
  ]
}
///////////////////////////////////////////////////////////////////////

resource linuxVmss 'Microsoft.Compute/virtualMachineScaleSets@2024-03-01' = if (operatingSystem == 'linux') {
  name: vmssName
  location: location
  zones: empty(zones) ? null : zones
  tags: tagsProfile
  sku: {
    capacity: instanceCount
    name: vmSKU
    tier: 'Standard'
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    overprovision: false
    singlePlacementGroup: true
    additionalCapabilities: dataDiskType == 'UltraSSD_LRS' && dataDiskSizeGB > 0
      ? {
          ultraSSDEnabled: true
        }
      : null
    upgradePolicy: {
      mode: 'Automatic'
      automaticOSUpgradePolicy: {
        enableAutomaticOSUpgrade: true
      }
    }
    proximityPlacementGroup: useProximityGroup
      ? {
          id: proximityId
        }
      : null
    virtualMachineProfile: {
      osProfile: {
        computerNamePrefix: vmssName
        adminUsername: adminUsername
        linuxConfiguration: {
          disablePasswordAuthentication: true
          ssh: {
            publicKeys: [
              for key in sshPublicKeys: {
                path: '/home/${adminUsername}/.ssh/authorized_keys'
                keyData: key
              }
            ]
          }
        }
        customData: cloudInitData
      }
      storageProfile: storageProfileConfig
      securityProfile: securityProfileConfig
      networkProfile: networkProfileConfig
      extensionProfile: {
        extensions: [
          {
            name: 'AADSSHLoginForLinux'
            properties: {
              publisher: 'Microsoft.Azure.ActiveDirectory'
              type: 'AADSSHLoginForLinux'
              typeHandlerVersion: '1.0'
              autoUpgradeMinorVersion: true
              settings: {}
            }
          }
          {
            name: 'HealthExtension'
            properties: {
              publisher: 'Microsoft.ManagedServices'
              type: 'ApplicationHealthLinux'
              autoUpgradeMinorVersion: true
              enableAutomaticUpgrade: true
              typeHandlerVersion: '1.0'
              settings: {
                protocol: 'tcp'
                port: 22
                intervalInSeconds: 5
                numberOfProbes: 1
              }
            }
          }
        ]
        extensionsTimeBudget: 'PT90M'
      }
    }
  }
}

resource windowsVmss 'Microsoft.Compute/virtualMachineScaleSets@2024-03-01' = if (operatingSystem == 'windows') {
  name: vmssName
  location: location
  zones: empty(zones) ? null : zones
  tags: tagsProfile
  sku: {
    capacity: instanceCount
    name: vmSKU
    tier: 'Standard'
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    overprovision: false
    singlePlacementGroup: true
    additionalCapabilities: dataDiskType == 'UltraSSD_LRS' && dataDiskSizeGB > 0
      ? {
          ultraSSDEnabled: true
        }
      : null
    upgradePolicy: {
      mode: 'Automatic'
    }
    proximityPlacementGroup: useProximityGroup
      ? {
          id: proximityId
        }
      : null
    virtualMachineProfile: {
      osProfile: {
        computerNamePrefix: substring(computerName, 0, 9)
        adminUsername: adminUsername
        adminPassword: adminPassword
        windowsConfiguration: {
          enableAutomaticUpdates: true
        }
      }
      storageProfile: storageProfileConfig
      securityProfile: securityProfileConfig
      networkProfile: networkProfileConfig
      extensionProfile: {
        extensions: [
          {
            name: 'AADLoginForWindows'
            properties: {
              publisher: 'Microsoft.Azure.ActiveDirectory'
              type: 'AADLoginForWindows'
              typeHandlerVersion: '1.0'
              autoUpgradeMinorVersion: true
            }
          }
          {
            name: 'HealthExtension'
            properties: {
              publisher: 'Microsoft.ManagedServices'
              type: 'ApplicationHealthWindows'
              autoUpgradeMinorVersion: true
              enableAutomaticUpgrade: true
              typeHandlerVersion: '1.0'
              settings: {
                protocol: 'tcp'
                port: 22
                intervalInSeconds: 5
                numberOfProbes: 1
              }
            }
          }
        ]
      }
    }
  }
}

resource linuxGuestConfigExtension 'Microsoft.Compute/virtualMachineScaleSets/extensions@2020-12-01' = if (operatingSystem == 'linux' && supportsTrustedLaunch) {
  parent: linuxVmss
  name: 'AzurePolicyforLinux'
  properties: {
    publisher: 'Microsoft.GuestConfiguration'
    type: 'ConfigurationForLinux'
    typeHandlerVersion: '1.0'
    autoUpgradeMinorVersion: true
    enableAutomaticUpgrade: true
    settings: {}
    protectedSettings: {}
  }
}

resource windowsGuestConfigExtension 'Microsoft.Compute/virtualMachineScaleSets/extensions@2020-12-01' = if (operatingSystem == 'windows') {
  parent: windowsVmss
  name: 'AzurePolicyforWindows'
  properties: {
    publisher: 'Microsoft.GuestConfiguration'
    type: 'ConfigurationforWindows'
    typeHandlerVersion: '1.0'
    autoUpgradeMinorVersion: true
    enableAutomaticUpgrade: true
    settings: {}
    protectedSettings: {}
  }
}

// Grant VMSS managed identity access to Key Vault secrets (access policy model)
resource existingKeyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = if (!empty(keyVaultName)) {
  name: keyVaultName
}

resource kvAccessPolicy 'Microsoft.KeyVault/vaults/accessPolicies@2023-07-01' = if (!empty(keyVaultName)) {
  name: 'add'
  parent: existingKeyVault
  properties: {
    accessPolicies: [
      {
        tenantId: subscription().tenantId
        #disable-next-line BCP318
        objectId: operatingSystem == 'linux' ? linuxVmss.identity.principalId : windowsVmss.identity.principalId
        permissions: {
          secrets: ['get']
        }
      }
    ]
  }
}

output vmssResourceId string = operatingSystem == 'linux' ? linuxVmss.id : windowsVmss.id

// Grant the VMSS managed identity read access to the tools tarball blob so it can
// pull tools.tar.gz from the shared storage account at provisioning/update time.
// Uses the built-in Storage Blob Data Reader role. The role assignment is created
// in this same deployment, referencing the VMSS system-assigned principalId (same
// pattern as the Key Vault access policy above). The assignment name is a
// deterministic GUID built from compile-time values so it is stable across runs.
var storageBlobDataReaderRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')

resource existingStorage 'Microsoft.Storage/storageAccounts@2023-01-01' existing = if (!empty(storageAccountName)) {
  name: storageAccountName
}

resource storageBlobRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(storageAccountName)) {
  name: guid(storageAccountName, vmssName, 'StorageBlobDataReader')
  scope: existingStorage
  properties: {
    roleDefinitionId: storageBlobDataReaderRoleId
    #disable-next-line BCP318
    principalId: operatingSystem == 'linux' ? linuxVmss.identity.principalId : windowsVmss.identity.principalId
    principalType: 'ServicePrincipal'
  }
}
