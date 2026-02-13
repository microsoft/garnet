// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that selects multiple indexes from a JSON array. Eg: .[0,1,2]
    /// </summary>
    internal class ArrayMultipleIndexFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the list of indexes to filter.
        /// </summary>
        internal List<int> Indexes;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayMultipleIndexFilter"/> class with the specified indexes.
        /// </summary>
        /// <param name="indexes">The list of indexes to filter.</param>
        public ArrayMultipleIndexFilter(List<int> indexes)
        {
            Indexes = indexes;
        }

        /// <summary>
        /// Executes the filter on the specified JSON node and returns the filtered nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            foreach (int i in Indexes)
            {
                if (TryGetTokenIndex(current, i, settings?.ErrorWhenNoMatch ?? false, out var jsonNode))
                {
                    yield return jsonNode;
                }
            }
        }

        /// <summary>
        /// Executes the filter on the specified enumerable of JSON nodes and returns the filtered nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current enumerable of JSON nodes.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of filtered JSON nodes.</returns>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
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