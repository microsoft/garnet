// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that selects multiple fields from a JSON object. Eg: .["field1", "field2"]
    /// </summary>
    internal class FieldMultipleFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the list of field names to filter.
        /// </summary>
        internal List<string> Names;

        /// <summary>
        /// Initializes a new instance of the <see cref="FieldMultipleFilter"/> class with the specified field names.
        /// </summary>
        /// <param name="names">The list of field names to filter.</param>
        public FieldMultipleFilter(List<string> names)
        {
            Names = names;
        }

        /// <summary>
        /// Executes the filter on a single JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        /// <exception cref="JsonException">Thrown when a specified field does not exist and <see cref="JsonSelectSettings.ErrorWhenNoMatch"/> is set to true.</exception>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            if (current is JsonObject obj)
            {
                foreach (string name in Names)
                {
                    if (obj.TryGetPropertyValue(name, out var v))
                    {
                        yield return v;
                    }

                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                            "Property '{0}' does not exist on JsonObject.", name));
                    }
                }
            }
            else
            {
                if (settings?.ErrorWhenNoMatch ?? false)
                {
                    throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                        "Properties {0} not valid on {1}.", string.Join(", ", Names.Select(n => "'" + n + "'")),
                        current?.GetType().Name));
                }
            }
        }

        /// <summary>
        /// Executes the filter on an enumerable of JSON nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The enumerable of current JSON nodes.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        /// <exception cref="JsonException">Thrown when a specified field does not exist and <see cref="JsonSelectSettings.ErrorWhenNoMatch"/> is set to true.</exception>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
        {
            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory.
                if (item is JsonObject obj)
                {
                    foreach (string name in Names)
                    {
                        if (obj.TryGetPropertyValue(name, out var v))
                        {
                            yield return v;
                        }

                        if (settings?.ErrorWhenNoMatch ?? false)
                        {
                            throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                                "Property '{0}' does not exist on JsonObject.", name));
                        }
                    }
                }
                else
                {
                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                            "Properties {0} not valid on {1}.", string.Join(", ", Names.Select(n => "'" + n + "'")),
                            item?.GetType().Name));
                    }
                }
            }
        }
    }
}