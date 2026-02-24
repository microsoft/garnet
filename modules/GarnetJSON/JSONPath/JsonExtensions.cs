// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Provides extension methods for JSON node selection using JSONPath.
    /// </summary>
    public static class JsonExtensions
    {
        private static JsonSelectSettings ErrorWhenNoMatchSettings =>
            new JsonSelectSettings { ErrorWhenNoMatch = true };

        /// <summary>
        /// Tries to select a single JSON node based on the specified JSONPath.
        /// </summary>
        /// <param name="jsonNode">The JSON node to search within.</param>
        /// <param name="path">The JSONPath expression to evaluate.</param>
        /// <param name="settings">Optional settings for JSONPath evaluation.</param>
        /// <param name="resultJsonNode">The resulting JSON node if found; otherwise, null.</param>
        /// <returns>True if a single JSON node is found; otherwise, false.</returns>
        /// <exception cref="JsonException">Thrown if the path returns multiple elements.</exception>
        public static bool TrySelectNode(this JsonNode jsonNode, string path, JsonSelectSettings? settings,
            out JsonNode? resultJsonNode)
        {
            JsonPath p = new JsonPath(path);

            resultJsonNode = null;
            var count = 0;
            foreach (var t in p.Evaluate(jsonNode, jsonNode, settings))
            {
                count++;

                if (count != 1)
                {
                    throw new JsonException("Path returned multiple elements.");
                }

                resultJsonNode = t;
            }

            return count == 0 ? false : true;
        }

        /// <summary>
        /// Tries to select a single JSON node based on the specified JSONPath.
        /// </summary>
        /// <param name="jsonNode">The JSON node to search within.</param>
        /// <param name="path">The JSONPath expression to evaluate.</param>
        /// <param name="resultJsonNode">The resulting JSON node if found; otherwise, null.</param>
        /// <returns>True if a single JSON node is found; otherwise, false.</returns>
        public static bool TrySelectNode(this JsonNode jsonNode, string path, out JsonNode? resultJsonNode)
        {
            return jsonNode.TrySelectNode(path, null, out resultJsonNode);
        }

        /// <summary>
        /// Selects multiple JSON nodes based on the specified JSONPath.
        /// </summary>
        /// <param name="jsonNode">The JSON node to search within.</param>
        /// <param name="path">The JSONPath expression to evaluate.</param>
        /// <param name="settings">Optional settings for JSONPath evaluation.</param>
        /// <returns>An enumerable collection of JSON nodes that match the JSONPath.</returns>
        public static IEnumerable<JsonNode?> SelectNodes(this JsonNode jsonNode, string path,
            JsonSelectSettings? settings = null)
        {
            JsonPath p = new JsonPath(path);
            return p.Evaluate(jsonNode, jsonNode, settings);
        }

        /// <summary>
        /// Selects multiple JSON nodes based on the specified JSONPath.
        /// </summary>
        /// <param name="jsonNode">The JSON node to search within.</param>
        /// <param name="path">The JSONPath expression to evaluate.</param>
        /// <param name="errorWhenNoMatch">Indicates whether to throw an error if no match is found.</param>
        /// <returns>An enumerable collection of JSON nodes that match the JSONPath.</returns>
        public static IEnumerable<JsonNode?> SelectNodes(this JsonNode jsonNode, string path, bool errorWhenNoMatch)
        {
            JsonPath p = new JsonPath(path);
            return p.Evaluate(jsonNode, jsonNode, errorWhenNoMatch ? ErrorWhenNoMatchSettings : null);
        }
    }
}