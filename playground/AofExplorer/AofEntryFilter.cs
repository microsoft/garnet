// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;

namespace AofExplorer
{
    /// <summary>
    /// Handles filtering and transformation of AOF entries based on various criteria
    /// </summary>
    internal static class AofEntryFilter
    {
        /// <summary>
        /// Apply all filters and transformations to the entries based on the provided options
        /// </summary>
        /// <param name="entries">The collection of AOF entries to filter</param>
        /// <param name="options">The options containing filter criteria</param>
        /// <returns>Filtered collection of AOF entries</returns>
        public static IEnumerable<AofEntry> ApplyFilters(IEnumerable<AofEntry> entries, Options options)
        {
            // Apply offset filters
            var minOffset = Options.ParseOffset(options.MinOffset ?? string.Empty);
            var maxOffset = Options.ParseOffset(options.MaxOffset ?? string.Empty);

            if (minOffset.HasValue || maxOffset.HasValue)
            {
                entries = entries.Where(e =>
                    (!minOffset.HasValue || e.FileOffset >= minOffset.Value) &&
                    (!maxOffset.HasValue || e.FileOffset <= maxOffset.Value));
            }

            // Apply operation filter
            if (!string.IsNullOrEmpty(options.FilterOperation))
            {
                entries = entries.Where(e => e.Header.opType.ToString().Equals(options.FilterOperation, StringComparison.OrdinalIgnoreCase));
            }

            // Apply session filter
            if (options.FilterSession.HasValue)
            {
                entries = entries.Where(e => e.Header.sessionID == options.FilterSession.Value);
            }

            // Apply version filter
            if (options.FilterVersion.HasValue)
            {
                entries = entries.Where(e => e.Header.storeVersion == options.FilterVersion.Value);
            }

            // Apply key/value filters
            if (!string.IsNullOrEmpty(options.KeyFilter))
            {
                entries = entries.Where(e => e.Key.Length > 0 &&
                    Encoding.UTF8.GetString(e.Key).Contains(options.KeyFilter, StringComparison.OrdinalIgnoreCase));
            }

            if (!string.IsNullOrEmpty(options.ValueFilter))
            {
                entries = entries.Where(e => e.Value.Length > 0 &&
                    Encoding.UTF8.GetString(e.Value).Contains(options.ValueFilter, StringComparison.OrdinalIgnoreCase));
            }

            // Apply skip and take
            if (options.Skip > 0)
            {
                entries = entries.Skip(options.Skip);
            }

            if (options.MaxEntries != int.MaxValue)
            {
                entries = entries.Take(options.MaxEntries);
            }

            return entries;
        }
    }
}