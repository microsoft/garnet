using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class RootFilter : PathFilter
    {
        public static readonly RootFilter Instance = new RootFilter();

        private RootFilter()
        {
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
        {
            return [root];
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
        {
            return [root];
        }
    }
}