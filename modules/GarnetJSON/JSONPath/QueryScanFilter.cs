using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that scans and evaluates JSON nodes based on a query expression. Eg: ..[?(@.name == 'value')]
    /// </summary>
    internal class QueryScanFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the query expression used to evaluate JSON nodes.
        /// </summary>
        internal QueryExpression Expression;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryScanFilter"/> class with the specified query expression.
        /// </summary>
        /// <param name="expression">The query expression to use for filtering JSON nodes.</param>
        public QueryScanFilter(QueryExpression expression)
        {
            Expression = expression;
        }

        /// <summary>
        /// Executes the filter on the specified JSON node and returns the matching nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node to evaluate.</param>
        /// <param name="settings">The settings to use for JSON selection.</param>
        /// <returns>An enumerable collection of matching JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            if (Expression.IsMatch(root, current, settings))
            {
                yield return current;
            }

            IEnumerator<JsonNode?>? enumerator = null;
            if (current is JsonArray arr)
            {
                enumerator = arr.GetEnumerator();
            }
            else if (current is JsonObject obj)
            {
                enumerator = (obj as IDictionary<string, JsonNode>).Values.GetEnumerator();
            }

            if (enumerator is not null)
            {
                var stack = new Stack<IEnumerator<JsonNode?>>();
                while (true)
                {
                    if (enumerator.MoveNext())
                    {
                        var jsonNode = enumerator.Current;
                        if (Expression.IsMatch(root, jsonNode, settings))
                        {
                            yield return jsonNode;
                        }

                        stack.Push(enumerator);

                        if (jsonNode is JsonArray innerArr)
                        {
                            enumerator = innerArr.GetEnumerator();
                        }
                        else if (jsonNode is JsonObject innerOobj)
                        {
                            enumerator = (innerOobj as IDictionary<string, JsonNode>).Values.GetEnumerator();
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
        /// Executes the filter on the specified collection of JSON nodes and returns the matching nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The collection of current JSON nodes to evaluate.</param>
        /// <param name="settings">The settings to use for JSON selection.</param>
        /// <returns>An enumerable collection of matching JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
        {
            return current.SelectMany(x => ExecuteFilter(root, x, settings));
        }
    }
}