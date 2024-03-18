// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;

namespace Tsavorite.test
{
    /// <summary>
    /// Log formatter primitives
    /// </summary>
    public static class LogFormatter
    {
        private const string TIME_FORMAT = "HH:mm:ss.ffff";
        private const string DATE_FORMAT = "yyyy-MM-dd " + TIME_FORMAT;

        /// <summary>
        /// Format date
        /// </summary>
        /// <param name="dateTime"></param>
        public static string FormatDate(DateTime dateTime) => dateTime.ToString(DATE_FORMAT, CultureInfo.InvariantCulture);

        /// <summary>
        /// Format time
        /// </summary>
        /// <param name="dateTime"></param>
        public static string FormatTime(DateTime dateTime) => dateTime.ToString(TIME_FORMAT, CultureInfo.InvariantCulture);
    }
}