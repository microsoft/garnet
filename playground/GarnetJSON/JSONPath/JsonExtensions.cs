using System.Runtime.CompilerServices;
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

        public static JsonElement? SelectElement(this JsonElement document, string path, JsonSelectSettings? settings = null)
        {
            var documentNode = document.AsNode() ?? throw new ArgumentException("Argument can't be converted into JsonNode", nameof(document));
            return documentNode.TrySelectNode(path, settings, out var jsonNode) ? jsonNode.ToJsonDocument().RootElement : null;
        }

        public static IEnumerable<JsonElement> SelectElements(this JsonElement document, string path, JsonSelectSettings? settings = null)
        {
            var documentNode = document.AsNode() ?? throw new ArgumentException("Argument can't be converted into JsonNode", nameof(document));
            return SelectNodes(documentNode, path, settings).Select(x => x.ToJsonDocument().RootElement);
        }

        public static JsonElement? SelectElement(this JsonDocument document, string path, JsonSelectSettings? settings = null)
        {
            var documentNode = document.RootElement.AsNode() ?? throw new ArgumentException("Argument can't be converted into JsonNode", nameof(document));
            return documentNode.TrySelectNode(path, settings, out var jsonNode) ? jsonNode.ToJsonDocument().RootElement : null;
        }

        public static IEnumerable<JsonElement> SelectElements(this JsonDocument document, string path, JsonSelectSettings? settings = null)
        {
            return document.RootElement.SelectElements(path, settings);
        }

        public static JsonElement? SelectElement(this JsonDocument document, string path, bool errorWhenNoMatch = false)
        {
            return document.RootElement.SelectElement(path, errorWhenNoMatch ? ErrorWhenNoMatchSettings : null);
        }

        public static JsonNode? AsNode(this JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.Array => JsonArray.Create(element),
                JsonValueKind.Object => JsonObject.Create(element),
                _ => JsonValue.Create(element)
            };
        }

        public static JsonDocument ToJsonDocument<T>(this T value, JsonSerializerOptions? options = null)
        {
            if (value is JsonDocument doc) return doc;

            return JsonDocument.Parse(JsonSerializer.Serialize(value, options));
        }
    }
}