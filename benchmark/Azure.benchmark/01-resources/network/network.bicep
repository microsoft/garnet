@allowed([
  'westus3'
  'eastus'
  'southcentralus'
  'centralus'
  'canadaeast'
  'australiaeast'
])
param location string

param networkSecurityGroupName string
param virtualNetworkName string
param subnetName string
param accSubnetName string
param proximityGroupName string

// /16 VNet split evenly into two /17 subnets (~32,763 usable IPs each) to support
// tens of thousands of VMs. Each VM consumes one IP in subnetName (primary NIC +
// public IP) and one in accSubnetName (eth1 / accelerated-networking NIC), so the
// two subnets are sized equally (1 VM = 1 IP in each).
var addressPrefix = '10.5.0.0/16'
var subnetAddressPrefix = '10.5.0.0/17'       // 10.5.0.0   - 10.5.127.255
var accSubnetAddressPrefix = '10.5.128.0/17'  // 10.5.128.0 - 10.5.255.255

// The accelerated (eth1) NICs in accSubnet have no public IP and only carry
// private intra-cluster traffic (they never need internet egress). To stop
// relying on Azure "default outbound access" (being retired per SFI NS261), we
// set defaultOutboundAccess:false on BOTH subnets, giving them no implicit
// outbound internet access. The primary subnet's VMs are unaffected in practice
// because each primary NIC already has its own Standard public IP for outbound.
// Note: defaultOutboundAccess is immutable after subnet creation, so applying this
// requires (re)creating the subnets (full redeploy).

// Network Security Group — shared by both subnets
resource nsg 'Microsoft.Network/networkSecurityGroups@2020-06-01' = {
  name: networkSecurityGroupName
  location: location
  properties: {
    securityRules: [
      {
        name: 'AllowCorpnetSSH'
        properties: {
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '22'
          sourceAddressPrefix: 'CorpNetPublic'
          destinationAddressPrefix: '*'
          access: 'Allow'
          priority: 2000
          direction: 'Inbound'
          sourcePortRanges: []
          destinationPortRanges: []
          sourceAddressPrefixes: []
          destinationAddressPrefixes: []
        }
      }
      {
        name: 'AllowCorpnetRDP'
        properties: {
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '3389'
          sourceAddressPrefix: 'CorpNetPublic'
          destinationAddressPrefix: '*'
          access: 'Allow'
          priority: 2010
          direction: 'Inbound'
          sourcePortRanges: []
          destinationPortRanges: []
          sourceAddressPrefixes: []
          destinationAddressPrefixes: []
        }
      }
    ]
  }
}

// Virtual Network with two subnets, both associated with the same NSG
resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: virtualNetworkName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        addressPrefix
      ]
    }
    subnets: [
      {
        name: subnetName
        properties: {
          addressPrefix: subnetAddressPrefix
          privateEndpointNetworkPolicies: 'Enabled'
          privateLinkServiceNetworkPolicies: 'Enabled'
          networkSecurityGroup: { id: nsg.id }
          defaultOutboundAccess: false
        }
      }
      {
        name: accSubnetName
        properties: {
          addressPrefix: accSubnetAddressPrefix
          privateEndpointNetworkPolicies: 'Enabled'
          privateLinkServiceNetworkPolicies: 'Enabled'
          networkSecurityGroup: { id: nsg.id }
          defaultOutboundAccess: false
        }
      }
    ]
  }
}

// Proximity Placement Group
resource prg 'Microsoft.Compute/proximityPlacementGroups@2021-03-01' = {
  name: proximityGroupName
  location: location
  properties: {
    proximityPlacementGroupType: 'Standard'
  }
}

output nsgId string = nsg.id
output vnetName string = vnet.name
output subnetName string = subnetName
output accSubnetName string = accSubnetName
output proximityId string = prg.id
