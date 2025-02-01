using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    internal class ArrayIndexFilter : PathFilter
    {
        public int? Index { get; set; }

        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current, JsonSelectSettings? settings)
        {
            if (Index != null)
            {
                if (TryGetTokenIndex(current, Index.GetValueOrDefault(), settings?.ErrorWhenNoMatch ?? false, out var jsonNode))
                {
                    return [jsonNode];
                }
            }
            else
            {
                if (current is JsonArray array)
                {
                    return array;
                }
                else
                {
                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Index * not valid on {0}.", current?.GetType().Name));
                    }
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
                if (Index != null)
                {
                    if (TryGetTokenIndex(item, Index.GetValueOrDefault(), errorWhenNoMatch, out var jsonNode))
                    {
                        yield return jsonNode;
                    }
                }
                else
                {
                    if (item is JsonArray array)
                    {
                        foreach (var v in array)
                        {
                            yield return v;
                        }
                    }
                    else
                    {
                        if (errorWhenNoMatch)
                        {
                            throw new JsonException(string.Format(CultureInfo.InvariantCulture, "Index * not valid on {0}.", current?.GetType().Name));
                        }
                    }
                }
            }
        }
    }
}