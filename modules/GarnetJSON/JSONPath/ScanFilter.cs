// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that scans through JSON nodes to find nodes matching a specified name. Eg: ..['name']
    /// </summary>
    internal class ScanFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the name of the JSON node to match.
        /// </summary>
        internal string? Name;

        /// <summary>
        /// Initializes a new instance of the <see cref="ScanFilter"/> class with the specified name.
        /// </summary>
        /// <param name="name">The name of the JSON node to match. If null, all nodes are matched.</param>
        public ScanFilter(string? name)
        {
            Name = name;
        }

        /// <summary>
        /// Executes the filter on the specified JSON node and returns the matching nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of matching JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            // Inspired by https://stackoverflow.com/a/30441479/7331395
            IEnumerator? enumerator = null;
            if (current is JsonArray arr)
            {
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
                            if (Name is null)
                            {
                                yield return element;
                            }

                            stack.Push(enumerator);
                        }
                        else if (enumerator is IEnumerator<KeyValuePair<string, JsonNode?>> objectEnumerator)
                        {
                            var element = objectEnumerator.Current;
                            jsonNode = element.Value;
                            if (Name is null || element.Key == Name)
                            {
                                yield return element.Value;
                            }

                            stack.Push(enumerator);
                        }

                        if (jsonNode is JsonArray innerArr)
                        {
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
        /// Executes the filter on the specified enumerable of JSON nodes and returns the matching nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The enumerable of current JSON nodes.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of matching JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
        {
            return current.SelectMany(x => ExecuteFilter(root, x, settings));
        }
    }
}