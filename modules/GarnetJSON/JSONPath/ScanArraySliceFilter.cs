using System.Collections;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a filter that slices an array based on the specified start, end, and step values. Eg .[1:5:2]
    /// </summary>
    internal class ScanArraySliceFilter : PathFilter
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

            IEnumerator? enumerator = null;
            if (current is JsonArray arr)
            {
                int count = arr.Count;

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
                        yield return arr[i];
                    }
                }

                enumerator = arr.GetEnumerator();
            }
            else if (current is JsonObject obj)
            {
                enumerator = obj.GetEnumerator();
            }

            if (enumerator is not null)
            {
                var stack = new Stack<IEnumerator>();
                while (true)
                {
                    if (enumerator.MoveNext())
                    {
                        JsonNode? jsonNode = default;
                        if (enumerator is IEnumerator<JsonNode?> arrayEnumerator)
                        {
                            var element = arrayEnumerator.Current;
                            jsonNode = element;
                            stack.Push(enumerator);
                        }
                        else if (enumerator is IEnumerator<KeyValuePair<string, JsonNode?>> objectEnumerator)
                        {
                            var element = objectEnumerator.Current;
                            jsonNode = element.Value;
                            stack.Push(enumerator);
                        }

                        if (jsonNode is JsonArray innerArr)
                        {
                            int count = innerArr.Count;

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
                                    yield return innerArr[i];
                                }
                            }
                            else
                            {
                                if (settings?.ErrorWhenNoMatch ?? false)
                                {
                                    throw new JsonException(string.Format(CultureInfo.InvariantCulture,
                                        "Array slice of {0} to {1} returned no results.",
                                        Start != null
                                            ? Start.GetValueOrDefault().ToString(CultureInfo.InvariantCulture)
                                            : "*",
                                        End != null
                                            ? End.GetValueOrDefault().ToString(CultureInfo.InvariantCulture)
                                            : "*"));
                                }
                            }


                            enumerator = innerArr.GetEnumerator();
                        }
                        else if (jsonNode is JsonObject innerOobj)
                        {
                            enumerator = innerOobj.GetEnumerator();
                        }
                    }
                    else if (stack.Count > 0)
                    {
                        enumerator = stack.Pop();
                    }
                    else
                    {
                        yield break;
                    }
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
            return current.SelectMany(x => ExecuteFilter(root, x, settings));
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