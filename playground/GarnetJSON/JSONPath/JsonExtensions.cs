using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    public static class JsonExtensions
    {
        private static JsonSelectSettings ErrorWhenNoMatchSettings => new JsonSelectSettings { ErrorWhenNoMatch = true };

        public static bool TrySelectNode(this JsonNode jsonNode, string path, JsonSelectSettings? settings, out JsonNode? resultJsonNode)
        {
            JsonPath p = new JsonPath(path);

            resultJsonNode = null;
            var count = 0;
            foreach (var t in p.Evaluate(jsonNode, jsonNode, settings))
            {
                count++;

                if (count != 1)
                {
                    throw new JsonException("Path returned multiple elements.");
                }

                resultJsonNode = t;
            }

            return count == 0 ? false : true;
        }

        public static bool TrySelectNode(this JsonNode jsonNode, string path, out JsonNode? resultJsonNode)
        {
            return jsonNode.TrySelectNode(path, null, out resultJsonNode);
        }

        public static IEnumerable<JsonNode?> SelectNodes(this JsonNode jsonNode, string path, JsonSelectSettings? settings = null)
        {
            JsonPath p = new JsonPath(path);
            return p.Evaluate(jsonNode, jsonNode, settings);
        }

        public static IEnumerable<JsonNode?> SelectNodes(this JsonNode jsonNode, string path, bool errorWhenNoMatch)
        {
            JsonPath p = new JsonPath(path);
            return p.Evaluate(jsonNode, jsonNode, errorWhenNoMatch ? ErrorWhenNoMatchSettings : null);
        }
    }
}