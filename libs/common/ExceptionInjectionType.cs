// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common
{
    /// <summary>
    /// Exception injection types - used only in debug mode for testing
    /// </summary>
    public enum ExceptionInjectionType
    {
        Network_After_GarnetServerTcp_Handler_Created,
        Network_After_TcpNetworkHandlerBase_Start_Server
    }
}