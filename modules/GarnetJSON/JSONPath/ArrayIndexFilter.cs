using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that selects a specific index from a JSON array. Eg: .[0] or .[*]
    /// </summary>
    internal class ArrayIndexFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the index to filter.
        /// </summary>
        public int? Index { get; set; }

        /// <summary>
        /// Executes the filter on the given JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        /// <exception cref="JsonException">Thrown when the index is not valid on the current node and errorWhenNoMatch is true.</exception>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            if (Index != null)
            {
                if (TryGetTokenIndex(current, Index.GetValueOrDefault(), settings?.ErrorWhenNoMatch ?? false,
                        out var jsonNode))
                {
                    return [jsonNode];
                }
            }
            else
            {
                if (current is JsonArray array)
                {
                    return array;
                }
                else if (current is JsonObject obj)
                {
                    return (obj as IDictionary<string, JsonNode>).Values;
                }
                else
                {
                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Index * not valid on {0}.",
                            current?.GetType().Name));
                    }
                }
            }

            return [];
        }

        /// <summary>
        /// Executes the filter on the given enumerable of JSON nodes.
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
        /// <param name="errorWhenNoMatch">Indicates whether to throw an error when no match is found.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        /// <exception cref="JsonException">Thrown when the index is not valid on the current node and errorWhenNoMatch is true.</exception>
        private IEnumerable<JsonNode?> ExecuteFilterMultiple(IEnumerable<JsonNode?> current, bool errorWhenNoMatch)
        {
            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory. So we have duplicated code here.
                if (Index != null)
                {
                    if (TryGetTokenIndex(item, Index.GetValueOrDefault(), errorWhenNoMatch, out var jsonNode))
                    {
                        yield return jsonNode;
                    }
                }
                else
                {
                    if (item is JsonArray array)
                    {
                        foreach (var v in array)
                        {
                            yield return v;
                        }
                    }
                    else if (item is JsonObject obj)
                    {
                        foreach (var kv in obj)
                        {
                            yield return kv.Value;
                        }
                    }
                    else
                    {
                        if (errorWhenNoMatch)
                        {
                            throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                                "Index * not valid on {0}.", current?.GetType().Name));
                        }
                    }
                }
            }
        }
    }
}