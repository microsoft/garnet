using System.Collections;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class ScanFilter : PathFilter
    {
        internal string? Name;

        public ScanFilter(string? name)
        {
            Name = name;
        }

        // Inspired by https://stackoverflow.com/a/30441479/7331395
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
        {
            if (Name is null)
            {
                yield return current;
            }

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

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
        {
            return current.SelectMany(x => ExecuteFilter(root, x, settings));
        }
    }
}