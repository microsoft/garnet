---
id: slot-migration
sidebar_label: Slot Migration
title: Slot Migration Overview
---

# Slot Migration Overview


<!-- During migration, the slot state is transient, and the owner node can serve only read requests.
The target of migration can potentially serve write requests to that specific slot using the appropriate RESP command.
In this case, the original owner maintains ownership but needs to redirect clients to target node for write requests.
To achieve this, we use _workerId when implementing the redirection logic and workerId property for everything else. -->