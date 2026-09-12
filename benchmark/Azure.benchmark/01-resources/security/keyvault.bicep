param location string
param keyVaultName string
param tenantId string = subscription().tenantId
param deployerPrincipalId string

var tagsProfile = {
  Environment: '/NonProd'
  Owner: 'vazois'
  app: 'azurebench'
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tagsProfile
  properties: {
    tenantId: tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: false
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    accessPolicies: [
      {
        tenantId: tenantId
        objectId: deployerPrincipalId
        permissions: {
          secrets: ['get', 'set', 'list', 'delete']
        }
      }
    ]
  }
}

output keyVaultUri string = keyVault.properties.vaultUri
output keyVaultName string = keyVault.name
