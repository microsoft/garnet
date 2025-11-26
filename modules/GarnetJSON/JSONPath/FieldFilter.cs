using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that selects a specific field from a JSON object. Eg: .field
    /// </summary>
    internal class FieldFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the name of the field to filter.
        /// </summary>
        internal string? Name;

        /// <summary>
        /// Initializes a new instance of the <see cref="FieldFilter"/> class with the specified field name.
        /// </summary>
        /// <param name="name">The name of the field to filter.</param>
        public FieldFilter(string? name)
        {
            Name = name;
        }

        /// <summary>
        /// Executes the filter on the specified JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        /// <exception cref="JsonException">Thrown when the specified field does not exist and <see cref="JsonSelectSettings.ErrorWhenNoMatch"/> is set to true.</exception>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            if (current is JsonObject obj)
            {
                if (Name is not null)
                {
                    if (obj.TryGetPropertyValue(Name, out var v))
                    {
                        return [v];
                    }
                    else if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                            "Property '{0}' does not exist on JObject.", Name));
                    }
                }
                else
                {
                    return (obj as IDictionary<string, JsonNode>).Values;
                }
            }
            else if (Name is null && current is JsonArray arr)
            {
                return arr;
            }
            else
            {
                if (settings?.ErrorWhenNoMatch ?? false)
                {
                    throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                        "Property '{0}' not valid on {1}.", Name is not null ? Name : "*", current?.GetType().Name));
                }
            }

            return [];
        }

        /// <summary>
        /// Executes the filter on the specified enumerable of JSON nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current enumerable of JSON nodes.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
        {
            var hasCount = current.TryGetNonEnumeratedCount(out int count);

            if (hasCount && count == 0)
            {
                return [];
            }
            else if (count == 1)
            {
                return ExecuteFilter(root, current.First(), settings);
            }
            else
            {
                return ExecuteFilterMultiple(current, settings?.ErrorWhenNoMatch ?? false);
            }
        }

        /// <summary>
        /// Executes the filter on multiple JSON nodes.
        /// </summary>
        /// <param name="current">The current enumerable of JSON nodes.</param>
        /// <param name="errorWhenNoMatch">Indicates whether to throw an exception when no match is found.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        /// <exception cref="JsonException">Thrown when the specified field does not exist and <paramref name="errorWhenNoMatch"/> is set to true.</exception>
        private IEnumerable<JsonNode?> ExecuteFilterMultiple(IEnumerable<JsonNode?> current, bool errorWhenNoMatch)
        {
            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory. So we have duplicated code here.
                if (item is JsonObject obj)
                {
                    if (Name is not null)
                    {
                        if (obj.TryGetPropertyValue(Name, out var v))
                        {
                            yield return v;
                        }
                        else if (errorWhenNoMatch)
                        {
                            throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                                "Property '{0}' does not exist on JObject.", Name));
                        }
                    }
                    else
                    {
                        foreach (var p in obj)
                        {
                            yield return p.Value;
                        }
                    }
                }
                else if (Name is null && item is JsonArray arr)
                {
                    foreach (var a in arr)
                    {
                        yield return a;
                    }
                }
                else
                {
                    if (errorWhenNoMatch)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                            "Property '{0}' not valid on {1}.", Name is not null ? Name : "*",
                            current?.GetType().Name));
                    }
                }
            }
        }
    }
}