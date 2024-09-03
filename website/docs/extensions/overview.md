---
id: overview
sidebar_label: Overview
title: Overview
---

# Extensibility

Garnet offers several ways to extend its core functionality:

* Custom Raw String Command
* Custom Object Command
* Custom Transaction
* Custom Procedure
* Module

The below section describes those ways and the appropriate scenario to use them.

## Custom Raw String Command

This is used to operate on a single key with raw string values. The records are stored in the main store.

Details available [here](raw-strings.md).

## Custom Object Command

This command operates on a single key as well, but on the object store.
This is used to expose commands that perform operations on custom data types.

Details available [here](objects.md).

## Custom Transaction

Custom transactions allow operating on multiple commands within a single block of execution in a transactional manner.
This ensures atomicity during execution of the whole block.

Details available [here](transactions.md).

## Custom Procedure

This allows to invoke multiple commands within a single block as well. However, the commands are executed in a non-transactional manner as if they were individually issued by a client.

Details available [here](procedure.md).

## Module

A module offers a way to package all related extension commands, procedures and transactions into a single binary to be loaded into Garnet.

Details available [here](module.md).