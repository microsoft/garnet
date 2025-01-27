using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class ArrayMultipleIndexFilter : PathFilter
    {
        internal List<int> Indexes;

        public ArrayMultipleIndexFilter(List<int> indexes)
        {
            Indexes = indexes;
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
        {
            foreach (int i in Indexes)
            {
                if (TryGetTokenIndex(current, i, settings?.ErrorWhenNoMatch ?? false, out var jsonNode))
                {
                    yield return jsonNode;
                }
            }
        }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current, JsonSelectSettings? settings)
        {
            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory. So we have duplicated code here.
                foreach (int i in Indexes)
                {
                    if (TryGetTokenIndex(item, i, settings?.ErrorWhenNoMatch ?? false, out var jsonNode))
                    {
                        yield return jsonNode;
                    }
                }
            }
        }
    }
}