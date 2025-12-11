// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.


namespace AofExplorer
{
    /// <summary>
    /// Handles exporting AOF entries to various file formats
    /// </summary>
    internal static class Exporter
    {
        /// <summary>
        /// Export AOF entries to CSV format
        /// </summary>
        /// <param name="entries">Collection of AOF entries to export</param>
        /// <param name="filePath">Path where the CSV file will be saved</param>
        public static void ExportToCsv(IEnumerable<AofEntry> entries, string filePath)
        {
            Console.WriteLine($"Exporting to CSV: {filePath}");
            using var writer = new StreamWriter(filePath);

            // CSV header
            writer.WriteLine("Offset,Operation,Version,SessionID,SequenceNumber,LogAccessCount,KeyLength,ValueLength,InputLength,TotalLength");

            foreach (var entry in entries)
            {
                var sequenceNumber = entry.IsExtended && entry.ExtendedHeader.HasValue ? entry.ExtendedHeader.Value.sequenceNumber.ToString() : "";
                //var logAccessCount = entry.IsExtended && entry.ExtendedHeader.HasValue ? entry.ExtendedHeader.Value.sublogAccessCount.ToString() : "";
                var logAccessCount = 0;

                writer.WriteLine($"{entry.FileOffset},{entry.Header.opType},{entry.Header.storeVersion},{entry.Header.sessionID},{sequenceNumber},{logAccessCount},{entry.Key.Length},{entry.Value.Length},{entry.Input.Length},{entry.TotalLength}");
            }

            Console.WriteLine("CSV export complete.");
        }

        /// <summary>
        /// Export AOF entries to JSON format
        /// </summary>
        /// <param name="entries">Collection of AOF entries to export</param>
        /// <param name="filePath">Path where the JSON file will be saved</param>
        public static void ExportToJson(IEnumerable<AofEntry> entries, string filePath)
        {
            Console.WriteLine($"Exporting to JSON: {filePath}");
            using var writer = new StreamWriter(filePath);

            writer.WriteLine("[");
            var first = true;

            foreach (var entry in entries)
            {
                if (!first) writer.WriteLine(",");
                first = false;

                writer.WriteLine("  {");
                writer.WriteLine($"    \"offset\": {entry.FileOffset},");
                writer.WriteLine($"    \"operation\": \"{entry.Header.opType}\",");
                writer.WriteLine($"    \"version\": {entry.Header.storeVersion},");
                writer.WriteLine($"    \"sessionId\": {entry.Header.sessionID},");

                if (entry.IsExtended && entry.ExtendedHeader.HasValue)
                {
                    var ext = entry.ExtendedHeader.Value;
                    writer.WriteLine($"    \"sequenceNumber\": {ext.sequenceNumber},");
                    //writer.WriteLine($"    \"logAccessCount\": {ext.sublogAccessCount},");
                }

                writer.WriteLine($"    \"keyLength\": {entry.Key.Length},");
                writer.WriteLine($"    \"valueLength\": {entry.Value.Length},");
                writer.WriteLine($"    \"inputLength\": {entry.Input.Length},");
                writer.WriteLine($"    \"totalLength\": {entry.TotalLength}");
                writer.Write("  }");
            }

            writer.WriteLine();
            writer.WriteLine("]");

            Console.WriteLine("JSON export complete.");
        }
    }
}