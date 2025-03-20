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
        /// <summary>
        /// Primary replication sync orchestration failure
        /// </summary>
        REPLICATION_FAIL_RIGHT_BEFORE_AOF_STREAM_STARTS,
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

        /// <summary>
        /// Enable exception scenario (NOTE: enable at beginning of test to trigger the exception at runtime)
        /// </summary>
        /// <param name="debugScenario"></param>
        [Conditional("DEBUG")]
        public static void EnableException(ExceptionScenario debugScenario) => ExceptionScenarios[(int)debugScenario] = true;

        /// <summary>
        /// Disable exception scenario (NOTE: for tests you need to always call disable at the end of the test to avoid breaking other tests in the line)
        /// </summary>
        /// <param name="debugScenario"></param>
        [Conditional("DEBUG")]
        public static void DisableException(ExceptionScenario debugScenario) => ExceptionScenarios[(int)debugScenario] = false;

        /// <summary>
        /// Trigger exception scenario (NOTE: add this to the location where the exception should be emulated/triggered)
        /// </summary>
        /// <param name="debugScenario"></param>
        /// <exception cref="GarnetException"></exception>
        [Conditional("DEBUG")]
        public static void TriggerException(ExceptionScenario debugScenario)
        {
            if (ExceptionScenarios[(int)debugScenario])
                throw new GarnetException($"Debug scenario triggered {debugScenario}");
        }
    }
}