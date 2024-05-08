---
id: slot-migration
sidebar_label: Migration
title: Migration Overview
---

# Slot Migration Overview

In Garnet, slot migration describes the process of reassigning slot ownership and transferring the associated key-value pairs from one primary node to another within a fully operational cluster. 
This operation allows for efficient resource utilization and load balancing across the cluster when adding or removing nodes.
The migration operation is only available in cluster mode.
Slot migration can be initiated by the owner (referred to as the *source* node) of a given slot, and addressed towards another already known, and trusted primary node (referred to as the *target* node).
Actual data migration can be initiated by using the ```MIGRATE ``` command that operates in two modes: (1) Migrate individual keys (2) Migrate entire slots or range of slots.
This page is focused on the slot migration implementation details.
For more information about using the associated command refer to the slot migration [user guide](../../cluster/key-migration).

# Implementation Details

The implementation of the migration operation is separated into two components:

1. Command parsing and validation component.
2. Migration session operation and management component.

The first component is responsible for parsing and validating the migration command arguments.
Validation involves the following:

1. Parse every migration option and validate arguments associated with that option.
2. Validate ownership of associated slot that is about to be migrated.
3. For ```KEYS``` option, validate that keys to be migrated do not hash across different slots.
4. For ```SLOTS/SLOTSRANGE``` option, validate no slot is referenced multiple times.
5. For ```SLOTSRANGE``` option, make sure a range is defined by a pair of values.
6. Validate that the target of the migration is known, trusted and has the role of a primary.

When parsing completes succesfully, a *migration* session is created and executed.
Depending on the chosen option, the *migration* session executes either as a foreground task (using ```KEYS``` option) or a background task (```SLOTS/SLOTSRANGE``` option).

The second component is separated into the following sub-components:

1. ```MigrationManager```
2. ```MigrateSessionTaskStore```
3. ```MigrateSession```

The ```MigrationManager``` is responsible for managing the active ```MigrateSession``` tasks.
It uses the ```MigrateSessionTaskStore``` to atomically add or remove new instances of ```MigrateSession```.
It is also responsible for ensuring that existing sessions do not conflict with sessions that are about to be added, by checking if the referred slots in each session do not conflict.
Finally, it provideds information on the number of ongoing migrate tasks.

## Migration Data Integrity

Since slot migration can be initiated while a Garnet cluster is operational, it needs to be carefully orchestrated to avoid any data integrity issues when clients attempt to write new data.
In addition, whatever solution is put forth should not hinder data avaibility while migration is in progress.

Our implementation leverages on the concept of slot-based sharding to ensure that keys mapping to the related slot cannot be modified while a migrate task is active.
This is achieved by setting the slot state to ***MIGRATING*** in the source node.
This prevents any write requests though it still allows for reads.
Write requests can be issued towards the target node using ***ASKING***, though there are not consistency guarantees from Garnet if this option is used.
Garnet guarantees that no keys can be lost during regular migration (i.e. with using ***ASKING***).

Because Garnet operates in a multi-threaded environment, the transition from ```STABLE``` to ```MIGRATING``` needs to happen safely, so every thread has a chance to observe that state change.
This can happen using epoch protection.
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

During migration, the change in slot state (i.e., ```MIGRATING```) is transient from the perspective of the source node.
This means that until migration completes the slot is still owned by the source node.
However, because it is necessary to produce ```-ASK``` redirect messages, the *workerId* is set to the *workerId* of the target node, without bumping the current local epoch (to avoid propagation of this transient update to the whole cluster).
Therefore, depending on the context of the operation being executed, the actual owner of the node can be determined by accessing *workerId* property, while the target node for migration is determined through *_workerId* variable.
For example, the ```CLUSTER NODES``` will use *workerId* property (through GetSlotRange(*workerId*)) since it has to return actual owner of the node even during migration.
At the same, time it needs to return all nodes that are in ```MIGRATING``` or ```IMPORTING``` state and the node-id associated with that state which can be done by inspecting the *_workerId* variable (through GetSpecialStates(*workerId*)).

<details>
        <summary>HashSlot Definition</summary>
        ```bash
        /// <summary>
        /// Get formatted (using CLUSTER NODES format) worker info.
        /// </summary>
        /// <param name="workerId">Offset of worker in the worker list.</param>
        /// <returns>Formatted string.</returns>
        public string GetNodeInfo(ushort workerId)
        {
            return $"{workers[workerId].Nodeid} " +
                $"{workers[workerId].Address}:{workers[workerId].Port}@{workers[workerId].Port + 10000},{workers[workerId].hostname} " +
                $"{(workerId == 1 ? "myself," : "")}{(workers[workerId].Role == NodeRole.PRIMARY ? "master" : "slave")} " +
                $"{(workers[workerId].Role == NodeRole.REPLICA ? workers[workerId].ReplicaOfNodeId : "-")} " +
                $"0 " +
                $"0 " +
                $"{workers[workerId].ConfigEpoch} " +
                $"connected" +
                $"{GetSlotRange(workerId)}" +
                $"{GetSpecialStates(workerId)}\n";
        }
        ```
</details>

## Migrate KEYS Implementation Details

Using the ```KEYS`` option, Garnet will iterate through the provided list of keys and migrate them in batches to the target node.
When using this option, the issuer of the migration command will have to make sure that the slot state is set appropriately in the source, and target node.
In addition, the issuer has to provide all keys that map to a specific slot either in one call to ```MIGRATE``` or across multiple call before the migration completes.
When all key-value pairs have migrated to the target node, the issuer has to reset the slot state and assign ownership of the slot to the new node.

```MigrateKeys``` is the main driver for the migration operation using the ```KEYS``` option.
This method iterates over the list of provided keys and sends them over to the target node.
This occurs in the following two phases: 
1. find all keys provided by ```MIGRATE``` command and send them over to the *target* node.
2. for all remaining keys not found in the main store, lookup into the object store, and if they are found send them over to the *target* node.
It is possible that a given key cannot be retrieved from either store, because it might have expired.
In that case, execution proceeds to the next available key and no specific error is raised.
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

The ```SLOTS``` or ```SLOTSRANGE``` options enables Garnet to migrate a collection of slots and all the associated keys mapping to these slots.
These options differ from the ```KEYS``` options in the following ways:

1. There is no need to have specific knowledge of the individual keys that are being migrated and how they map to the associated slots. The user simply needs to provide just a slot number.
2. State transitions are handled entirely on the server side.
3. For the migration operation to complete, we have to scan both main and object stores to find and migrate all keys associated with a given slot. 

It might seem, based on the last bullet point from above that the migration operation using ```SLOTS``` or ```SLOTSRANGE``` is more expensive, especially if the slot that is being migrated contains only a few keys.
However, it is generally less expensive compared to the ```KEYS``` option which requires multiple roundtrips between client and server (so any relevant keys can be used as input to the ```MIGRATE``` command), in addition to having to perform a full scan of both stores.

As shown in the code excerpt below, the ```MIGRATE SLOTS``` task will safely transition the state of a slot in the remote node config to ```IMPORTING``` and the slot state of the local node config to ```MIGRATING```, by relying on the epoch protection mechanism as described previously.
Following, it will start migrating the data to the target node in batches.
On completion of data migration, the task will conclude with performing next slot state transition where ownership of the slots being migrates will be handed to the target node.
The slot ownership exchange becomes visible to the whole cluster by bumping the local node's configuration epoch (i.e. using RelinquishOwnership command).
Finally, the source node will issue ```CLUSTER SETSLOT NODE``` to the target node to explicitly make it an owner of the corresponding slot collection.
This last step is not necessary and it is used only to speed up the config propagation.

<details>
        <summary>Migrate SLOTS Task</summary>
        ```bash
        /// <summary>
        /// Migrate slots session background task
        /// </summary>
        private void BeginAsyncMigrationTask()
        {
                //1. Set target node to import state
                if (!TrySetSlotRanges(GetSourceNodeId, MigrateState.IMPORT))
                {
                    logger?.LogError("Failed to set remote slots {slots} to import state", string.Join(',', GetSlots));
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                #region transitionLocalSlotToMigratingState
                //2. Set source node to migrating state and wait for local threads to see changed state.
                if (!TryPrepareLocalForMigration())
                {
                    logger?.LogError("Failed to set local slots {slots} to migrate state", string.Join(',', GetSlots));
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                if (!clusterProvider.WaitForConfigTransition()) return;
                #endregion

                #region migrateData
                //3. Migrate actual data
                if (!MigrateSlotsDataDriver())
                {
                    logger?.LogError($"MigrateSlotsDriver failed");
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion
                #region migrateData
                //3. Migrate actual data
                if (!MigrateSlotsDataDriver())
                {
                    logger?.LogError($"MigrateSlotsDriver failed");
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion

                #region transferSlotOwnnershipToTargetNode
                //5. Clear local migration set.
                if (!RelinquishOwnership())
                {
                    logger?.LogError($"Failed to relinquish ownerhsip to target node");
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                //6. Change ownership of slots to target node.
                if (!TrySetSlotRanges(GetTargetNodeId, MigrateState.NODE))
                {
                    logger?.LogError($"Failed to assign ownerhsip to target node");
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }        
                #endregion
        }
        ```
</details>