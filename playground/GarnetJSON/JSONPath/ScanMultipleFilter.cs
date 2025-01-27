using System.Collections;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath;

internal class ScanMultipleFilter : PathFilter
{
    private List<string> _names;

    public ScanMultipleFilter(List<string> names)
    {
        _names = names;
    }

    public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
    {
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
                        stack.Push(enumerator);
                    }
                    else if (enumerator is IEnumerator<KeyValuePair<string, JsonNode?>> objectEnumerator)
                    {
                        var element = objectEnumerator.Current;
                        jsonNode = element.Value;
                        if (_names.Contains(element.Key))
                        {
                            yield return jsonNode;
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

    public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
    {
        return current.SelectMany(x => ExecuteFilter(root, x, settings));
    }
}