// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that returns the root JSON node. Eg: $
    /// </summary>
    internal class RootFilter : PathFilter
    {
        /// <summary>
        /// Singleton instance of the RootFilter.
        /// </summary>
        public static readonly RootFilter Instance = new RootFilter();

        /// <summary>
        /// Private constructor to prevent instantiation.
        /// </summary>
        private RootFilter()
        {
        }

        /// <summary>
        /// Executes the filter and returns the root JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node (ignored).</param>
        /// <param name="settings">The settings for JSON selection (ignored).</param>
        /// <returns>An enumerable containing the root JSON node.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            return [root];
        }

        /// <summary>
        /// Executes the filter and returns the root JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON nodes (ignored).</param>
        /// <param name="settings">The settings for JSON selection (ignored).</param>
        /// <returns>An enumerable containing the root JSON node.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
        {
            return [root];
        }
    }
}