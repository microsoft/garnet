using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    public abstract class PathFilter
    {
        public abstract IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings);
        public abstract IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings);

        protected static bool TryGetTokenIndex(JsonNode? t, int index, bool errorWhenNoMatch, out JsonNode? jsonNode)
        {
            jsonNode = default;
            if (t is JsonArray a)
            {
                if (a.Count <= index)
                {
                    if (errorWhenNoMatch)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Index {0} outside the bounds of JArray.", index));
                    }

                    return false;
                }

                jsonNode = a[index];
                return true;
            }
            else
            {
                if (errorWhenNoMatch)
                {
                    throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Index {0} not valid on {1}.", index, t?.GetType().Name));
                }

                return false;
            }
        }
    }
}