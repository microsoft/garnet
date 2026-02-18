// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Abstract base class representing a filter in a JSONPath query.
    /// </summary>
    public abstract class PathFilter
    {
        /// <summary>
        /// Executes the filter on a single JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of JSON nodes that match the filter.</returns>
        public abstract IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings);

        /// <summary>
        /// Executes the filter on a collection of JSON nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The collection of current JSON nodes.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of JSON nodes that match the filter.</returns>
        public abstract IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings);

        /// <summary>
        /// Tries to get a JSON node at the specified index from a JSON array.
        /// </summary>
        /// <param name="t">The JSON node, expected to be a JSON array.</param>
        /// <param name="index">The index of the element to retrieve.</param>
        /// <param name="errorWhenNoMatch">Whether to throw an error if the index is out of bounds.</param>
        /// <param name="jsonNode">The JSON node at the specified index, if found.</param>
        /// <returns>True if the JSON node was found at the specified index; otherwise, false.</returns>
        /// <exception cref="JsonException">Thrown when the index is out of bounds and <paramref name="errorWhenNoMatch"/> is true.</exception>
        protected static bool TryGetTokenIndex(JsonNode? t, int index, bool errorWhenNoMatch, out JsonNode? jsonNode)
        {
            jsonNode = default;
            if (t is JsonArray a)
            {
                int indexToUse = index < 0 ? a.Count + index : index;
                if (indexToUse < 0 || a.Count <= indexToUse)
                {
                    if (errorWhenNoMatch)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                            "Index {0} outside the bounds of JArray.", index));
                    }

                    return false;
                }

                jsonNode = a[indexToUse];
                return true;
            }
            else
            {
                if (errorWhenNoMatch)
                {
                    throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Index {0} not valid on {1}.",
                        index, t?.GetType().Name));
                }

                return false;
            }
        }
    }
}