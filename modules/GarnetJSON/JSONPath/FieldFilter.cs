using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class FieldFilter : PathFilter
    {
        internal string? Name;

        public FieldFilter(string? name)
        {
            Name = name;
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
        {
            if (current is JsonObject obj)
            {
                if (Name is not null)
                {
                    if (obj.TryGetPropertyValue(Name, out var v))
                    {
                        return [v];
                    }
                    else if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Property '{0}' does not exist on JObject.", Name));
                    }
                }
                else
                {
                    return (obj as IDictionary<string, JsonNode>).Values;
                }
            }
            else
            {
                if (settings?.ErrorWhenNoMatch ?? false)
                {
                    throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Property '{0}' not valid on {1}.", Name is not null ? Name : "*", current?.GetType().Name));
                }
            }

            return [];
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
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

        private IEnumerable<JsonNode?> ExecuteFilterMultiple(IEnumerable<JsonNode?> current, bool errorWhenNoMatch)
        {
            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory. So we have duplicated code here.
                if (item is JsonObject obj)
                {
                    if (Name is not null)
                    {
                        if (obj.TryGetPropertyValue(Name, out var v))
                        {
                            yield return v;
                        }
                        else if (errorWhenNoMatch)
                        {
                            throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Property '{0}' does not exist on JObject.", Name));
                        }
                    }
                    else
                    {
                        foreach (var p in obj)
                        {
                            yield return p.Value;
                        }
                    }
                }
                else
                {
                    if (errorWhenNoMatch)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Property '{0}' not valid on {1}.", Name is not null ? Name : "*", current?.GetType().Name));
                    }
                }
            }
        }
    }
}