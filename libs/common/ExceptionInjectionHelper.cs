// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;

namespace Garnet.common
{
    /// <summary>
    /// Exception injection helper - used only in debug mode for testing
    /// </summary>
    public static class ExceptionInjectionHelper
    {
        /// <summary>
        /// Array of exception injection types
        /// </summary>
        static readonly bool[] ExceptionInjectionTypes =
            Enum.GetValues<ExceptionInjectionType>().Select(_ => false).ToArray();

        /// <summary>
        /// Enable exception scenario (NOTE: enable at beginning of test to trigger the exception at runtime)
        /// </summary>
        /// <param name="exceptionType"></param>
        [Conditional("DEBUG")]
        public static void EnableException(ExceptionInjectionType exceptionType) => ExceptionInjectionTypes[(int)exceptionType] = true;

        /// <summary>
        /// Disable exception scenario (NOTE: for tests you need to always call disable at the end of the test to avoid breaking other tests in the line)
        /// </summary>
        /// <param name="exceptionType"></param>
        [Conditional("DEBUG")]
        public static void DisableException(ExceptionInjectionType exceptionType) => ExceptionInjectionTypes[(int)exceptionType] = false;

        /// <summary>
        /// Trigger exception scenario (NOTE: add this to the location where the exception should be emulated/triggered)
        /// </summary>
        /// <param name="exceptionType"></param>
        /// <exception cref="GarnetException"></exception>
        [Conditional("DEBUG")]
        public static void TriggerException(ExceptionInjectionType exceptionType)
        {
            if (ExceptionInjectionTypes[(int)exceptionType])
                throw new GarnetException($"Exception injection triggered {exceptionType}");
        }
    }
}