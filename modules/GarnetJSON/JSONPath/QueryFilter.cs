using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that applies a query expression to JSON nodes. Eg: .field[?(@.name == 'value')]
    /// </summary>
    internal class QueryFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the query expression used to filter JSON nodes.
        /// </summary>
        internal QueryExpression Expression;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryFilter"/> class with the specified query expression.
        /// </summary>
        /// <param name="expression">The query expression to use for filtering.</param>
        public QueryFilter(QueryExpression expression)
        {
            Expression = expression;
        }

        /// <summary>
        /// Executes the filter on a single JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node to filter.</param>
        /// <param name="settings">The settings to use for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            if (current is JsonArray arr)
            {
                foreach (var v in arr)
                {
                    if (Expression.IsMatch(root, v, settings))
                    {
                        yield return v;
                    }
                }
            }
            else if (current is JsonObject obj)
            {
                foreach (var v in (obj as IDictionary<string, JsonNode>).Values)
                {
                    if (Expression.IsMatch(root, v, settings))
                    {
                        yield return v;
                    }
                }
            }
        }

        /// <summary>
        /// Executes the filter on a collection of JSON nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The collection of current JSON nodes to filter.</param>
        /// <param name="settings">The settings to use for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
        {
            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory.
                if (item is JsonArray arr)
                {
                    foreach (var v in arr)
                    {
                        if (Expression.IsMatch(root, v, settings))
                        {
                            yield return v;
                        }
                    }
                }
                else if (item is JsonObject obj)
                {
                    foreach (var v in (obj as IDictionary<string, JsonNode>).Values)
                    {
                        if (Expression.IsMatch(root, v, settings))
                        {
                            yield return v;
                        }
                    }
                }
            }
        }
    }
}