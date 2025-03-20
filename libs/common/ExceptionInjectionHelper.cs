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

        [Conditional("DEBUG")]
        public static void EnableException(ExceptionInjectionType exceptionType) => ExceptionInjectionTypes[(int)exceptionType] = true;

        [Conditional("DEBUG")]
        public static void DisableException(ExceptionInjectionType exceptionType) => ExceptionInjectionTypes[(int)exceptionType] = false;

        [Conditional("DEBUG")]
        public static void TriggerException(ExceptionInjectionType exceptionType)
        {
            if (ExceptionInjectionTypes[(int)exceptionType])
                throw new GarnetException($"Exception injection triggered {exceptionType}");
        }
    }
}