using System.Collections;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class QueryScanFilter : PathFilter
    {
        internal QueryExpression Expression;

        public QueryScanFilter(QueryExpression expression)
        {
            Expression = expression;
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
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

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
        {
            return current.SelectMany(x => ExecuteFilter(root, x, settings));
        }
    }
}