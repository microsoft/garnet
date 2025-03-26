// Copyright (c) Microsoft Corporation.
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
        Replication_Fail_Before_Background_AOF_Stream_Task_Start
    }
}