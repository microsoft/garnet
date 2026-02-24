// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Specifies the settings used when selecting JSON.
    /// </summary>
    public class JsonSelectSettings
    {
        /// <summary>
        /// Gets or sets a timeout that will be used when executing regular expressions.
        /// </summary>
        /// <value>The timeout that will be used when executing regular expressions.</value>
        public TimeSpan? RegexMatchTimeout { get; set; }

        /// <summary>
        /// Gets or sets a flag that indicates whether an error should be thrown if
        /// no tokens are found when evaluating part of the expression.
        /// </summary>
        /// <value>
        /// A flag that indicates whether an error should be thrown if
        /// no tokens are found when evaluating part of the expression.
        /// </value>
        public bool ErrorWhenNoMatch { get; set; }
    }
}