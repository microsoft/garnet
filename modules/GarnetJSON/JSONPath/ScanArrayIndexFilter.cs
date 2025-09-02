using System.Collections;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that scans through JSON nodes and selects a specific index from a JSON arrays. Eg: ..[0] or ..[*]
    /// </summary>
    internal class ScanArrayIndexFilter : PathFilter
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
            // Inspired by https://stackoverflow.com/a/30441479/7331395
            IEnumerator? enumerator = null;
            if (current is JsonArray arr)
            {
                enumerator = arr.GetEnumerator();
                if (Index is not null && TryGetTokenIndex(arr, Index.Value, settings?.ErrorWhenNoMatch ?? false,
                        out var foundNode))
                {
                    yield return foundNode;
                }
            }
            else if (current is JsonObject obj)
            {
                enumerator = obj.GetEnumerator();
            }


            if (enumerator is not null)
            {
                var stack = new Stack<IEnumerator>();
                while (true)
                {
                    if (enumerator.MoveNext())
                    {
                        JsonNode? jsonNode = default;
                        if (enumerator is IEnumerator<JsonNode?> arrayEnumerator)
                        {
                            var element = arrayEnumerator.Current;
                            if (Index is null)
                            {
                                yield return element;
                            }

                            jsonNode = element;
                            stack.Push(enumerator);
                        }
                        else if (enumerator is IEnumerator<KeyValuePair<string, JsonNode?>> objectEnumerator)
                        {
                            var element = objectEnumerator.Current;
                            jsonNode = element.Value;
                            if (Index is null)
                            {
                                yield return element.Value;
                            }

                            stack.Push(enumerator);
                        }

                        if (jsonNode is JsonArray innerArr)
                        {
                            if (Index.HasValue)
                            {
                                if (TryGetTokenIndex(jsonNode, Index.Value, settings?.ErrorWhenNoMatch ?? false,
                                        out var foundNode))
                                {
                                    yield return foundNode;
                                }
                            }

                            enumerator = innerArr.GetEnumerator();
                        }
                        else if (jsonNode is JsonObject innerOobj)
                        {
                            enumerator = innerOobj.GetEnumerator();
                        }
                    }
                    else if (stack.Count > 0)
                    {
                        enumerator = stack.Pop();
                    }
                    else
                    {
                        yield break;
                    }
                }
            }
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
            return current.SelectMany(x => ExecuteFilter(root, x, settings));
        }
    }
}