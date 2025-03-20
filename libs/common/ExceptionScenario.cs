// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;

namespace Garnet.common
{
    /// <summary>
    /// Exception scenarios for testing
    /// </summary>
    public enum ExceptionScenario
    {
        Network_After_GarnetServerTcp_Handler_Created,
        Network_After_TcpNetworkHandlerBase_Start_Server
    }

    /// <summary>
    /// Exception scenario helper
    /// </summary>
    public static class ExceptionScenarioHelper
    {
        /// <summary>
        /// Exception scenarios
        /// </summary>
        public static bool[] ExceptionScenarios =
            Enum.GetValues<ExceptionScenario>().Select(debugScenarios => false).ToArray();

        [Conditional("DEBUG")]
        public static void EnableException(ExceptionScenario debugScenario) => ExceptionScenarios[(int)debugScenario] = true;

        [Conditional("DEBUG")]
        public static void DisableException(ExceptionScenario debugScenario) => ExceptionScenarios[(int)debugScenario] = false;

        [Conditional("DEBUG")]
        public static void TriggerException(ExceptionScenario debugScenario)
        {
            if (ExceptionScenarios[(int)debugScenario])
                throw new GarnetException($"Debug scenario triggered {debugScenario}");
        }
    }
}