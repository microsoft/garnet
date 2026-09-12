@allowed([
  'westus3'
  'eastus'
  'southcentralus'
  'centralus'
  'canadaeast'
  'australiaeast'
])
param location string

// Storage account names must be globally unique, 3-24 chars, lowercase
// alphanumeric only (no hyphens). A deterministic name derived from the
// resource group id keeps the account stable across redeploys while staying
// globally unique. Override only if you need a specific name.
param storageAccountName string = 'garnetst${uniqueString(resourceGroup().id)}'

// Blob container that holds the tools tarball (tools.tar.gz) delivered to VMSS
// instances at provisioning/update time.
param containerName string = 'tools'

// The app tag lets management scripts discover this account in the resource
// group at deploy time (see manage-vmss.ps1 -Action create) without persisting
// the generated name anywhere.
var tagsProfile = {
  app: 'azurebench'
  Environment: '/NonProd'
  Owner: 'vazois'
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  tags: tagsProfile
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    accessTier: 'Hot'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

output storageAccountName string = storageAccount.name
output containerName string = container.name
