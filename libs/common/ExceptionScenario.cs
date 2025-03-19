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
        FAIL_RIGHT_BEFORE_AOF_STREAM_STARTS,
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