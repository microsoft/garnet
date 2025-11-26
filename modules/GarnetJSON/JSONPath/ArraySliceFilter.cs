using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that slices an array based on the specified start, end, and step values. Eg .[1:5:2]
    /// </summary>
    internal class ArraySliceFilter : PathFilter
    {
        /// <summary>
        /// Gets or sets the start index of the slice.
        /// </summary>
        public int? Start { get; set; }

        /// <summary>
        /// Gets or sets the end index of the slice.
        /// </summary>
        public int? End { get; set; }

        /// <summary>
        /// Gets or sets the step value of the slice.
        /// </summary>
        public int? Step { get; set; }

        /// <summary>
        /// Executes the filter on the specified JSON node.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current JSON node.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of JSON nodes that match the filter.</returns>
        /// <exception cref="JsonException">Thrown when the step value is zero or when no match is found and ErrorWhenNoMatch is true.</exception>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, JsonNode? current,
            JsonSelectSettings? settings)
        {
            if (Step == 0)
            {
                throw new JsonException("Step cannot be zero.");
            }

            if (current is JsonArray array)
            {
                int count = array.Count;

                // set defaults for null arguments
                int stepCount = Step ?? 1;
                int startIndex = Start ?? ((stepCount > 0) ? 0 : count - 1);
                int stopIndex = End ?? ((stepCount > 0) ? count : -1);

                // start from the end of the list if start is negative
                if (Start < 0)
                {
                    startIndex = count + startIndex;
                }

                // end from the start of the list if stop is negative
                if (End < 0)
                {
                    stopIndex = count + stopIndex;
                }

                // ensure indexes keep within collection bounds
                startIndex = Math.Max(startIndex, (stepCount > 0) ? 0 : int.MinValue);
                startIndex = Math.Min(startIndex, (stepCount > 0) ? count : count - 1);
                stopIndex = Math.Max(stopIndex, -1);
                stopIndex = Math.Min(stopIndex, count);

                bool positiveStep = (stepCount > 0);

                if (IsValid(startIndex, stopIndex, positiveStep))
                {
                    for (int i = startIndex; IsValid(i, stopIndex, positiveStep); i += stepCount)
                    {
                        yield return array[i];
                    }
                }
                else
                {
                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                            "Array slice of {0} to {1} returned no results.",
                            Start != null ? Start.GetValueOrDefault().ToString(CultureInfo.InvariantCulture) : "*",
                            End != null ? End.GetValueOrDefault().ToString(CultureInfo.InvariantCulture) : "*"));
                    }
                }
            }
            else
            {
                if (settings?.ErrorWhenNoMatch ?? false)
                {
                    throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                        "Array slice is not valid on {0}.", current?.GetType().Name));
                }
            }
        }

        /// <summary>
        /// Executes the filter on the specified enumerable of JSON nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="current">The current enumerable of JSON nodes.</param>
        /// <param name="settings">The settings for JSON selection.</param>
        /// <returns>An enumerable of JSON nodes that match the filter.</returns>
        /// <exception cref="JsonException">Thrown when the step value is zero or when no match is found and ErrorWhenNoMatch is true.</exception>
        public override IEnumerable<JsonNode?> ExecuteFilter(JsonNode root, IEnumerable<JsonNode?> current,
            JsonSelectSettings? settings)
        {
            if (Step == 0)
            {
                throw new JsonException("Step cannot be zero.");
            }

            foreach (var item in current)
            {
                // Note: Not calling ExecuteFilter with yield return because that approach is slower and uses more memory. So we have duplicated code here.
                if (item is JsonArray array)
                {
                    int count = array.Count;

                    // set defaults for null arguments
                    int stepCount = Step ?? 1;
                    int startIndex = Start ?? ((stepCount > 0) ? 0 : count - 1);
                    int stopIndex = End ?? ((stepCount > 0) ? count : -1);

                    // start from the end of the list if start is negative
                    if (Start < 0)
                    {
                        startIndex = count + startIndex;
                    }

                    // end from the start of the list if stop is negative
                    if (End < 0)
                    {
                        stopIndex = count + stopIndex;
                    }

                    // ensure indexes keep within collection bounds
                    startIndex = Math.Max(startIndex, (stepCount > 0) ? 0 : int.MinValue);
                    startIndex = Math.Min(startIndex, (stepCount > 0) ? count : count - 1);
                    stopIndex = Math.Max(stopIndex, -1);
                    stopIndex = Math.Min(stopIndex, count);

                    bool positiveStep = (stepCount > 0);

                    if (IsValid(startIndex, stopIndex, positiveStep))
                    {
                        for (int i = startIndex; IsValid(i, stopIndex, positiveStep); i += stepCount)
                        {
                            yield return array[i];
                        }
                    }
                    else
                    {
                        if (settings?.ErrorWhenNoMatch ?? false)
                        {
                            throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                                "Array slice of {0} to {1} returned no results.",
                                Start != null ? Start.GetValueOrDefault().ToString(CultureInfo.InvariantCulture) : "*",
                                End != null ? End.GetValueOrDefault().ToString(CultureInfo.InvariantCulture) : "*"));
                        }
                    }
                }
                else
                {
                    if (settings?.ErrorWhenNoMatch ?? false)
                    {
                        throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                            "Array slice is not valid on {0}.", current?.GetType().Name));
                    }
                }
            }
        }

        /// <summary>
        /// Determines whether the specified index is valid based on the stop index and step direction.
        /// </summary>
        /// <param name="index">The current index.</param>
        /// <param name="stopIndex">The stop index.</param>
        /// <param name="positiveStep">A value indicating whether the step is positive.</param>
        /// <returns><c>true</c> if the index is valid; otherwise, <c>false</c>.</returns>
        private bool IsValid(int index, int stopIndex, bool positiveStep)
        {
            if (positiveStep)
            {
                return (index < stopIndex);
            }

            return (index > stopIndex);
        }
    }
}