---
id: security
sidebar_label: Security
title: Secure your Azure Cosmos DB Garnet Cache
---

# Secure your Azure Cosmos DB Garnet Cache

Azure Cosmos DB Garnet Cache provides enterprise-grade security features to protect your data. This guide covers the security capabilities and best practices for securing your cache.


## Network Security

Azure Cosmos DB Garnet Cache does not provide public IP addresses or DNS, meaning it can't be accessed from the public internet. Cache nodes are provisioned on the virtual network provided during cluster creation. They are accessible via the internal IP addresses from the same VNet. 

There are the 2 options to connect to the cache:
1.	Deploy the clients connecting to the cache on the same virtual network with either the same subnet, or a different one. The Redis clients can use the internal IP address of the cache instances to connect. Learn how to [find your internal IP addresses](./quickstart.md#step-4-connect-and-test).
2.	If the client applications are deployed in a separate virtual network, then you can use [virtual network peering](https://learn.microsoft.com/azure/virtual-network/virtual-network-peering-overview) to connect the two. Follow the steps in this [tutorial](https://learn.microsoft.com/azure/virtual-network/tutorial-connect-virtual-networks?tabs=portal). Ensure that the IP address space of the two virtual networks does not overlap. Once peering is established successfully, the client application in one virtual network can access the cache endpoints on the other network using their local IP addresses.

### Outbound access

The cache nodes need outbound connectivity to the destinations listed in [Required outbound network rules](#required-outbound-network-rules) in order to provision and operate. A subnet provides that connectivity through either [default outbound access](https://learn.microsoft.com/azure/virtual-network/ip-services/default-outbound-access) or an explicit outbound method such as a NAT gateway. If the cache subnet has neither, cluster provisioning fails.

When you create a **new** virtual network while provisioning a cache in the Azure portal, the **Enable outbound access** checkbox controls whether that VNet has default outbound access:

- **Checked (default)** — the new VNet has outbound access, so provisioning can reach the required destinations. Leave this checked when creating a new VNet through the portal.
- **Unchecked** — the new VNet has no outbound path, and provisioning fails. Only uncheck this if you configure an explicit outbound method yourself.

If you bring your own virtual network, this checkbox doesn't apply and you're responsible for the subnet's outbound connectivity. Make sure the cache subnet can reach the required destinations, for example through a [NAT gateway](https://learn.microsoft.com/azure/nat-gateway/nat-overview), Azure's recommended [explicit outbound method](https://learn.microsoft.com/azure/virtual-network/ip-services/default-outbound-access#add-an-explicit-outbound-method).

### Required outbound network rules

If you use Azure Firewall to restrict outbound access from the existing subnet, we highly recommend that you use [virtual network service tags](https://learn.microsoft.com/azure/virtual-network/service-tags-overview). The tags in the following table are required to make the Azure Cosmos DB Garnet Cache function properly.

|Destination service tag |Protocol |Port |Use |
|------------------------|---------|-----|----|
|Storage                 |HTTPS    |443  |Required for secure communication between the nodes and Azure Storage for Control Plane communication and configuration. |
|AzureKeyVault           |HTTPS    |443  |Required for secure communication between the nodes and Azure Key Vault. Certificates and keys are used to secure communication inside the cache. |
|EventHub                |HTTPS    |443  |Required to forward logs to Azure. |
|AzureMonitor            |HTTPS    |443  |Required to forward metrics to Azure. |
|AzureActiveDirectory    |HTTPS    |443  |Required for Microsoft Entra authentication. |
|AzureResourceManager    |HTTPS    |443  |Required to gather information about and manage Garnet nodes (for example, reboot). |
|AzureFrontDoor.Firstparty |HTTPS  |443  |Required for logging operations. |
|GuestAndHybridManagement  |HTTPS  |443  |Required to gather information about and manage Garnet nodes (for example, reboot). |
|ApiManagement             |HTTPS  |443  |Required to gather information about and manage Garnet nodes (for example, reboot). |

In addition to the tags table, you need to add the following address prefixes because a service tag doesn't exist for the relevant service: 
- 104.40.0.0/13 
- 13.104.0.0/14 
- 40.64.0.0/10 


## Authentication and Access Control

Access to Azure Cosmos DB Garnet Cache spans two planes:

- **Control plane** — creating, scaling, and managing the cluster resource through Azure Resource Manager.
- **Data plane** — running Redis commands against an existing cluster.

### Permissions to create and manage a cluster

Creating or updating a Garnet cluster is a control plane operation. Because the cluster is deployed into a delegated subnet in your virtual network, the identity performing the operation must be authorized both to manage the Garnet resource and to join that subnet. The identity needs:

- `Microsoft.DocumentDB/garnetClusters/write` — to create or update the Garnet cluster.
- `Microsoft.Network/virtualNetworks/subnets/join/action` — on the subnet the cluster is deployed into.

Both creating and scaling a cluster require these permissions.

The [Owner](https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/privileged#owner) and [Contributor](https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/privileged#contributor) built-in roles include both permissions. Cosmos DB-specific built-in roles — such as Cosmos DB Operator, DocumentDB Account Contributor, and Cosmos DB Account Reader — can't create or scale a Garnet cluster: a Garnet cluster is a distinct resource type (`Microsoft.DocumentDB/garnetClusters`) that their permissions don't cover, and they don't grant subnet `join/action`.

If you use a custom role, it must explicitly include both `Microsoft.DocumentDB/garnetClusters/write` and `Microsoft.Network/virtualNetworks/subnets/join/action` on the subnet. A role that can create networking resources doesn't necessarily include `join/action`.

### Data plane access

Azure Cosmos DB Garnet Cache uses Azure RBAC to secure your data by granting permissions for supported Redis commands. Microsoft Entra ID RBAC roles define fine-grained permissions and are required to use data plane operations. No roles are assigned by default, including to the resource creator. You can assign roles to users, groups, service principals or managed identities for data access. Note that role assignments can take several minutes to propagate after being created or modified.

### Built-in Roles

There are several built-in roles to help you manage data access. Learn how to [assign roles](./quickstart.md#step-2-configure-data-access-with-rbac) using the Azure portal or Azure CLI.

| Role | Use Case | Access Level |
|------|----------|--------------|
| `Garnet Data Reader` | Read-only applications, monitoring | GET, EXISTS etc. |
| `Garnet Data Contributor` | Most applications | Read/write string, hash, set, sorted set |
| `Garnet Data Owner` | Admin access, full control | All operations including destructive ones |
| `Garnet Script Data Contributor` | Script execution and management | All scripting |
| `Garnet PubSub Data Reader` | Reading pub/sub messages | Read access to pub/sub channels |
| `Garnet PubSub Data Contributor` | All pub/sub messaging | Publish/subscribe operations |

To see detailed permissions associated with each role, navigate to the **Access control (IAM)** page of your Garnet Cache resource in the [Azure portal](https://portal.azure.com). The **Roles** tab has all available roles. Search for **garnet** and view the details for each role.

![Built-in Roles](../../static/img/azure/built-in-roles.png)

See a detailed list of allowed commands for each role under **Permissions > DataActions**.

![Garnet Data Contributor permissions](../../static/img/azure/garnet-data-contributor.png)


## Data Encryption

Azure Cosmos DB Garnet Cache implements comprehensive data protection measures to ensure your data remains secure both at rest and in transit, giving you end-to-end encryption. TLS 1.2 or higher and node-to-node encryption are enforced.


## Learn More

- [Cluster Configuration](./cluster-configuration.md)
- [Resiliency](./resiliency.md)
- [Monitoring](./monitoring.md)
