using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class QueryFilter : PathFilter
    {
        internal QueryExpression Expression;

        public QueryFilter(QueryExpression expression)
        {
            Expression = expression;
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
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

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
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