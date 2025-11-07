// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace AofExplorer
{
    /// <summary>
    /// Console application to browse and display AOF files
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed(RunWithOptions)
                .WithNotParsed(HandleParseError);
        }

        static void RunWithOptions(Options options)
        {
            try
            {
                var displayFormat = options.GetDisplayFormat();
                var parser = new AofFileParser(options);

                // Apply filters and limits
                var entries = parser.ParseFile();

                // Apply all filters using the dedicated filter class
                entries = AofEntryFilter.ApplyFilters(entries, options);

                if (options.Skip > 0)
                    Console.WriteLine($"Skipping first {options.Skip} entries");
                if (options.MaxEntries != int.MaxValue)
                    Console.WriteLine($"Limiting to {options.MaxEntries} entries");
                if (!string.IsNullOrEmpty(options.FilterOperation))
                    Console.WriteLine($"Filtering by operation: {options.FilterOperation}");
                if (options.FilterSession.HasValue)
                    Console.WriteLine($"Filtering by session: {options.FilterSession}");
                if (options.FilterVersion.HasValue)
                    Console.WriteLine($"Filtering by version: {options.FilterVersion}");
                if (!string.IsNullOrEmpty(options.KeyFilter))
                    Console.WriteLine($"Filtering by key containing: {options.KeyFilter}");
                if (!string.IsNullOrEmpty(options.ValueFilter))
                    Console.WriteLine($"Filtering by value containing: {options.ValueFilter}");

                if (options.StatsOnly)
                {
                    ConsoleFormatter.DisplayStatistics(entries);
                }
                else
                {
                    ConsoleFormatter.DisplayEntries(entries, displayFormat);
                }

                // Export if requested
                if (!string.IsNullOrEmpty(options.ExportCsv))
                {
                    Exporter.ExportToCsv(entries, options.ExportCsv);
                }

                if (!string.IsNullOrEmpty(options.ExportJson))
                {
                    Exporter.ExportToJson(entries, options.ExportJson);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error browsing file: {ex.Message}");
                if (options.GetEffectiveFormat() == DisplayFormat.Verbose)
                {
                    Console.WriteLine(ex.StackTrace);
                }
            }
        }

        static void HandleParseError(IEnumerable<CommandLine.Error> errors)
        {
            foreach (var error in errors)
            {
                if (error is CommandLine.HelpRequestedError || error is CommandLine.VersionRequestedError)
                    return; // Help or version was already displayed
            }

            Console.WriteLine("Failed to parse command line arguments. Use --help for usage information.");
            Environment.Exit(1);
        }
    }
}