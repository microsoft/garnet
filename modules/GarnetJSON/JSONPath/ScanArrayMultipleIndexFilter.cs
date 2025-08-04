using System.Collections;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that selects multiple indexes from a JSON array. Eg: .[0,1,2]
    /// </summary>
    internal class ScanArrayMultipleIndexFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the list of indexes to filter.
        /// </summary>
        internal List<int> Indexes;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayMultipleIndexFilter"/> class with the specified indexes.
        /// </summary>
        /// <param name="indexes">The list of indexes to filter.</param>
        public ScanArrayMultipleIndexFilter(List<int> indexes)
        {
            Indexes = indexes;
        }

        /// <summary>
        /// Executes the filter on the specified JSON node and returns the filtered nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            // Inspired by https://stackoverflow.com/a/30441479/7331395
            IEnumerator? enumerator = null;
            if (current is JsonArray arr)
            {
                foreach (int i in Indexes)
                {
                    if (TryGetTokenIndex(arr, i, settings?.ErrorWhenNoMatch ?? false, out var foundNode))
                    {
                        yield return foundNode;
                    }
                }

                enumerator = arr.GetEnumerator();
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
                            jsonNode = element;
                            stack.Push(enumerator);
                        }
                        else if (enumerator is IEnumerator<KeyValuePair<string, JsonNode?>> objectEnumerator)
                        {
                            var element = objectEnumerator.Current;
                            jsonNode = element.Value;
                            stack.Push(enumerator);
                        }

                        if (jsonNode is JsonArray innerArr)
                        {
                            foreach (int i in Indexes)
                            {
                                if (TryGetTokenIndex(innerArr, i, settings?.ErrorWhenNoMatch ?? false,
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
        /// Executes the filter on the specified enumerable of JSON nodes and returns the filtered nodes.
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