// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;

    /// <summary>
    /// Utility methods
    /// </summary>
    public static class Utils
    {
        /// <summary>
        /// Returns true or false whether an exception is considered fatal
        /// </summary>
        public static bool IsFatal(Exception exception)
        {
            if (exception is OutOfMemoryException || exception is StackOverflowException)
            {
                return true;
            }

            return false;
        }
    }
}