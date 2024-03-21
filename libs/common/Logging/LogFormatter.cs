// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;

namespace Garnet.common.Logging
{
    /// <summary>
    /// Log formatter primitives
    /// </summary>
    public static class LogFormatter
    {
        private const string TimeFormat = "HH:mm:ss.ffff";
        private const string DateFormat = "yyyy-MM-dd " + TimeFormat;

        /// <summary>
        /// Format date
        /// </summary>
        /// <param name="dateTime"></param>
        public static string FormatDate(DateTime dateTime) => dateTime.ToString(DateFormat, CultureInfo.InvariantCulture);

        /// <summary>
        /// Format time
        /// </summary>
        /// <param name="dateTime"></param>
        public static string FormatTime(DateTime dateTime) => dateTime.ToString(TimeFormat, CultureInfo.InvariantCulture);
    }
}