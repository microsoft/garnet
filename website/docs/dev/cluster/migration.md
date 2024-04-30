---
id: slot-migration
sidebar_label: Migration
title: Migration Overview
---

# Slot Migration Overview

In Garnet, slot migration, refers to the act of re-assigning slot ownernship and moving (i.e. ***migrating***) the associated key-values pairs from one node to another while the cluster is fully operational.
The migration operation is only available in cluster mode.
It can be initiated only by the original owner (a.k.a source node) of a given slot and addressed towards another already known, and trusted cluster node (a.k.a target node).
Both the source and target node have to be primary nodes.
Migration is triggered manually by using the MIGRATE command that operates in two modes (1) Migrate individual keys (2) Migrate entire slots or range of slots.
This page is focused on the slot migration implementation details.
For more information about using the associated command refer to the slot migration [user guide](../../cluster/key-migration).


# Implementation Details

The implementation of migration operation is separated into two components

1. Command parsing and validation component.
2. Migrate session operation and management component.


The first component is responsible for parsing and validating the migration command arguments.
Validation involves the following:

1. Parse every migration option and validate arguments associated with that option.
2. Validate ownership of associated slot that is about to be migrate.
3. For KEYS option, validate that keys to be migrated do not hash across different slots.
4. For SLOTS/SLOTSRANGE option, validate no slot is referenced multiple times.
5. For SLOTSRANGE option, make sure a range is defined by a pair of values.
6. Validate that target of migration is known, trusted and has the role of a primary.

When parsing completes succesfully, a migrate session is created and executed.
Depending on the chosen option, the migrate session executes as a foreground task (using KEYS option) or a background task (SLOTS/SLOTSRANGE option).

The second component is separated into the following sub-components:

1. ```MigrationManager```
2. ```MigrateSessionTaskStore```
3. ```MigrateSession```

The ```MigrationManager``` is responsible for managing the active ```MigrateSession``` tasks.
It uses ```MigrateSessionTaskStore``` to atommically add or remove new ```MigrateSession```.
It is also responsible for ensuring that existing sessions do not conflict with session that are about to be added by checking if the referred slots in each session do not conflict.
Finally, it provideds information on the number of ongoing migrate tasks.

## Migration Data Integrity

Since slot migration can be initiated while a Garnet cluster is operational, it needs to be carefully orchestrated to avoid any data integrity issues when clients attempt to write new data.
In addition, whatever solution is put forth should not hinder data avaibility while migration is in progress.

Our implementation leverages on the concept slot based sharding to ensure that keys mapping to the related slot cannot be modified while a migrate task is active.
This is achieved by setting the slot state to ***MIGRATING*** in the source node.
This prevents any write requests though it still allows for reads.
Write requests can be issued towards the target node using ***ASKING***, though there are not consistency guarantees from Garnet if this option is used.
Garnet guarantees that no keys can be lost during regular migration (i.e. with using ***ASKING***).

Because Garnet operates in a multi-threaded environment, the transition from ```STABLE``` to ```MIGRATING``` needs to happen safely, so every thread has then change to observe that state change.
This can be achieved using epoch protection.
Specifically, when a slot transitions to a new state, the segment that implements the actual state transition will have to spin-wait after making the change and return only after all active client sessions have moved to the next epoch.

An excerpt of the code related to epoch protection during slot state transition is shown below.
Every client session, before processing an incoming packet, will first acquire the current epoch.
This also happens for the client session that issues a slot state transition command (i.e. CLUSTER SETSLOT).
On success of updating the local config command, the processing thread of the previous command will perform the following:

1. release the current epoch (in order to avoid waiting for itselft to transition).
2. spin-wait until all thread transition to next epoch.
3. re-acquire the next epoch.

This series of steps ensures that on return, the processing thread guarantees to the command issuer (or the method caller if state transition happens internally), that the state transition is visible to all threads that were active.
Therefore, any active client session will process subsequent commands considering the new slot state.

<details>
        <summary>Spin-Wait Config Transition</summary>
        ```bash
        .
        .
        .
        clusterSession?.AcquireCurrentEpoch();
        .
        .
        .

            case SlotState.MIGRATING:
                setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForMigration(slot, nodeid, out errorMessage);
                break;            
        
        .
        .
        .
        
        if (setSlotsSucceeded)
        {
            UnsafeWaitForConfigTransition();

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }

        .
        .
        .

        /// <summary>
        /// Release epoch, wait for config transition and re-acquire the epoch
        /// </summary>
        public void UnsafeWaitForConfigTransition()
        {
            ReleaseCurrentEpoch();
            clusterProvider.WaitForConfigTransition();
            AcquireCurrentEpoch();
        }

        /// <summary>
        /// Wait for config transition
        /// </summary>
        /// <returns></returns>
        internal bool WaitForConfigTransition()
        {
            var server = storeWrapper.GetServer();
            BumpCurrentEpoch();
            while (true)
            {
            retry:
                var currentEpoch = GarnetCurrentEpoch;
                Thread.Yield();
                var sessions = server.ActiveClusterSessions();
                foreach (var s in sessions)
                {
                    var entryEpoch = s.LocalCurrentEpoch;
                    if (entryEpoch != 0 && entryEpoch >= currentEpoch)
                        goto retry;
                }
                break;
            }
            return true;
        }        
        ```
</details>



<!-- During migration, the slot state is transient, and the owner node can serve only read requests.
The target of migration can potentially serve write requests to that specific slot using the appropriate RESP command.
In this case, the original owner maintains ownership but needs to redirect clients to target node for write requests.
To achieve this, we use _workerId when implementing the redirection logic and workerId property for everything else. -->


## Migrate KEYS Implementation Details

Using the KEYS option, Garnet will iterate through the provided list of keys and migrate them in batches to the target node.
When using this option, the issuer of the migration command will have to make sure that the slot state is set appropriately in the source, and target node.
In addition, the issuer has to provide all keys that map to a specific slot either in one call to MIGRATE or across multiple call before the migration completes.
When all key-value pairs have migrated to the target node, the issues has to reset the slot state and assign ownership of the slot to the new node.

```MigrateKeys``` is the main driver for the migration operation using KEYS option.
This method iterates over the list of provided keys and sends them over to the target node.
This happens in two steps, (1) look in the main store and if a key exists send it over while removing it from the list of keys to be send, (2) search object store for any remaining keys, not found in the main store and send them over if they are found.
It is possible that a key cannot be retrieved from either store, because it might have expired.
In that case, no error is raised.
When data transmission completes, and depending if COPY option is enabled, ```MigrateKeys``` deletes the keys from the both stores.

<details>
        <summary>Migrate KEYS methods</summary>
        ```bash
        /// <summary>
        /// Method used to migrate individual keys from main store to target node.
        /// Used for MIGRATE KEYS option
        /// </summary>
        /// <param name="keysWithSize">List of pairs of address to the network receive buffer, key size </param>
        /// <param name="objectStoreKeys">Output keys not found in main store so we can scan the object store next</param>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromMainStore(ref List<(long, long)> keysWithSize, out List<(long, long)> objectStoreKeys);

        /// <summary>
        /// Method used to migrate individual keys from object store to target node.
        /// Used for MIGRATE KEYS option
        /// </summary>
        /// <param name="objectStoreKeys">List of pairs of address to the network receive buffer, key size that were not found in main store</param>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromObjectStore(ref List<(long, long)> objectStoreKeys);
        
        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// Used for MIGRATE KEYS option
        /// </summary>
        public bool MigrateKeys();
        ```
</details>

## Migrate SLOTS Details

The difference between KEYS and SLOTS options is that the latter allows for migrating all keys associated with a single or a collection of slots.
It does not require
