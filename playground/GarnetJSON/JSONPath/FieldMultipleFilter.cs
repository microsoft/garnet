using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class FieldMultipleFilter : PathFilter
    {
        internal List<string> Names;

        public FieldMultipleFilter(List<string> names)
        {
            Names = names;
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
        {
            if (current is JsonObject obj)
            {
                foreach (string name in Names)
                {
                    if (obj.TryGetPropertyValue(name, out var v))
                    {
                        yield return v;
                    }

                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Property '{0}' does not exist on JObject.", name));
                    }
                }
            }
            else
            {
                if (settings?.ErrorWhenNoMatch ?? false)
                {
                    throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Properties {0} not valid on {1}.", string.Join(", ", Names.Select(n => "'" + n + "'")), current?.GetType().Name));
                }
            }
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
        {
            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory.
                if (item is JsonObject obj)
                {
                    foreach (string name in Names)
                    {
                        if (obj.TryGetPropertyValue(name, out var v))
                        {
                            yield return v;
                        }

                        if (settings?.ErrorWhenNoMatch ?? false)
                        {
                            throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Property '{0}' does not exist on JObject.", name));
                        }
                    }
                }
                else
                {
                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Properties {0} not valid on {1}.", string.Join(", ", Names.Select(n => "'" + n + "'")), item?.GetType().Name));
                    }
                }
            }
        }
    }
}