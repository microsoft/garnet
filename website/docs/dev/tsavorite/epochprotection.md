---
id: epochprotection
sidebar_label: EpochProtection 
title: Epoch Protection Framework

---
# Light Epoch Protection in Garnet (Latch-free lazy synchronization)

## Context

We need to ensure shared variables are not being read and mutated simultaneously without deterministic orderings. Commonly used concurreny primitives such as Mutexes and semaphores provided by the language require threads to synchronize frequently with each other. This Synchronization across threads is expensive; Epoch protection **reduces the frequency of synchronization across threads**.

## Epoch Protection (10,000-foot view)

-	LightEpoch provides a synchronization mechanism where the write path does not need to block the thread but can be enqueued as a callback action task. The write path is handed off to the instance of LightEpoch to ensure the write does not run when another thread may have the epoch "acquired" or the state being read at a given version.

-	As a user, you can use Epoch protection to confidently see the latest state of shared variables without worrying about changes to their state. The overall epoch system proceeds with updating the state only when the thread is done viewing the version of the state.

-	LightEpoch is used because it can run certain operations after other threads that have seen the previous state are no longer executing (that is the basis of epoch control). It's not really "mutual exclusion". It's more like setting address boundaries and operating in them in such a way that there is no conflict. Really briefly: threads "protect" the current epoch by saying "I am active in this epoch" (where "epoch" is a counter). When they are done, they remove that protection. When some operation that would change shared variables (like 'HeadAddress') is going to run, it is done by "bumping" the epoch, which increases the counter then waits until no other thread is operating with the previous value protected. Variables are set before the bump so that any thread seeing the new counter value also sees the updated variables (again, e.g., 'HeadAddress') so we know that we are safe to operate up to the current 'HeadAddress', which is past the previous one.

## Implementation Details

-	There is a system-wide LightEpoch threads array with N entries, where ```N = max(128, ProcessorCount * 2)```. This means a given LightEpoch can support up to 128 threads. In the code, you may notice that while we allocate and set the table to the variable tableRaw, all interactions with the Epoch Table are via tableAligned. This optimization is for cache line sizes on modern processors and L1-L3 caches (64-byte cache line size). When a thread becomes part of the Epoch Protection system, we add it to the epoch table and store a thread-local epoch for that thread in the Epoch Table. At the start of being added to the Epoch Table, the thread-local epoch is set to the current global epoch.

-	Each time we "acquire" an epoch, a thread will claim ownership over an epoch counter. Any new incoming threads will take over the epochs only after this one.

-	Each thread that has access to the LightEpoch object can bump the global Epoch (global to the scope of the instance of the LightEpoch class, shared across threads).

-	We can add trigger actions that are executed when all threads are past a safe epoch ``` (For every thread T: SafeEpoch <= thread local Epoch <= Global Epoch) ```. Since the system holds all thread local epochs in a system accesible epoch table we can scan and find a safe epoch.
This gives us the ability to have an exactly-once invoked function that depends on all threads logically coordinating and not having any code being executed at the time of Epoch ending for the trigger.

-	If you look closely, what `epoch.Resume()` essentially does is have a thread find a free entry in the epoch table, put its id there, and "claim" the current epoch (the next thread will increment the epoch by 1 and claim the next epoch). Inside `epoch.Resume()`, there is a loop; if the current target epoch entry is already occupied by another thread, the current thread will yield. When it wakes up, it will try to occupy the next entry. If the epoch table is full, the rest of the threads will keep yielding.

## Relevant Public Methods and How to use them

1.	`ThisInstanceProtected`: Tells us whether the calling thread owns an entry in the epoch table, i.e., is participating in Epoch Protection at the moment. If not, you can have it participate in epoch protection by calling `Resume`.

2.	`ProtectAndDrain`: Marks the current thread as the owner of an updated epoch and performs draining of actions up to that point. This is used by Resume internally. It acts as a way to refresh the shared variables by draining pending actions. Often used inside loops to ensure we are progressively draining actions.

3.	`Suspend`: Use this to let go of the ownership of an epoch. If the thread calling Suspend is the last active thread in the LightEpoch system, the pending actions/writes are invoked.

4.	`Resume`: Use this when a thread needs to view a refreshed state of the shared variables to the latest state that is considered safe by all other threads. It acts as a temporal boundary to apply pending actions/writes before using the shared variables.

5.	`BumpCurrentEpoch(Action)`: Use this to schedule a write or an action for later at a temporal boundary where it is safe to change the state of a shared variable. A call to this may drain actions if, during iterations, it finds values it can drain.