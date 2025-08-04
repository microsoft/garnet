﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common
{
    /// <summary>
    /// Exception injection types - used only in debug mode for testing
    /// </summary>
    public enum ExceptionInjectionType
    {
        /// <summary>
        /// Network failure after GarnetServerTcp handler created
        /// </summary>
        Network_After_GarnetServerTcp_Handler_Created,
        /// <summary>
        /// Network failure after TcpNetworkHandlerBase start server
        /// </summary>
        Network_After_TcpNetworkHandlerBase_Start_Server,
        /// <summary>
        /// Primary replication sync orchestration failure right before background aof stream starts
        /// </summary>
        Replication_Fail_Before_Background_AOF_Stream_Task_Start,
        /// <summary>
        /// Acquire checkpoint entry from memory entries
        /// </summary>
        Replication_Acquire_Checkpoint_Entry_Fail_Condition,
        /// <summary>
        /// Wait after checkpoint acquisition
        /// </summary>
        Replication_Wait_After_Checkpoint_Acquisition,
        /// <summary>
        /// Wait at migration slot driver right after acquiring the end scan range
        /// </summary>
        Migration_Slot_End_Scan_Range_Acquisition,
        /// <summary>
        /// Failed to add AOF sync task due to unknown node
        /// </summary>
        Replication_Failed_To_AddAofSyncTask_UnknownNode,
        /// <summary>
        /// Delay response on receive checkpoint to trigger timeout
        /// </summary>
        Replication_Timeout_On_Receive_Checkpoint
    }
}