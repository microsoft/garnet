// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common
{
    /// <summary>
    /// Metrics item (or row), contains metric name and value
    /// </summary>
    public readonly struct MetricsItem
    {
        /// <summary>
        /// Name of metric
        /// </summary>
        public readonly string Name;

        /// <summary>
        /// Value of metrics
        /// </summary>
        public readonly string Value;

        /// <summary>
        /// Metrics Item Constructor
        /// </summary>
        /// <param name="Name"></param>
        /// <param name="Value"></param>
        public MetricsItem(string Name, string Value)
        {
            this.Name = Name;
            this.Value = Value;
        }
    }
}