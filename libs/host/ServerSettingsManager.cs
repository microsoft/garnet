// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using CommandLine;
using CommandLine.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Garnet
{
    /// <summary>
    /// Server settings manager
    /// </summary>
    internal static class ServerSettingsManager
    {
        public const string DefaultOptionsEmbeddedFileName = @"defaults.conf";
        private const string OptionPattern = "^\\s+(-[a-z], )?--([a-z-]+)\\s+(.*)$";

        /// <summary>
        /// Parses command line arguments and sets settings to Options object
        /// Options are initialized to values in ConfigDefaultImportPath (defaults to ./defaults.conf if not specified),
        /// overridden by any options specified by ConfigImportPath (if specified)
        /// then overridden by any options specified in the command line arguments (if any).
        /// </summary>
        /// <param name="args">Command line arguments</param>
        /// <param name="logger">Logger</param>
        /// <param name="options">Options object containing parsed configuration settings</param>
        /// <param name="invalidOptions">List of Options properties that did not pass validation</param>
        /// <returns>True if parsing succeeded</returns>
        internal static bool TryParseCommandLineArguments(string[] args, out Options options, out List<string> invalidOptions, ILogger logger = null)
        {
            if (logger == null)
                logger = NullLogger.Instance;

            options = null;
            invalidOptions = new List<string>();

            if (args == null) args = Array.Empty<string>();

            // Initialize command line parser
            var parser = new Parser(settings =>
            {
                settings.AutoHelp = false;
                settings.CaseInsensitiveEnumValues = true;
                settings.GetoptMode = true;
            });

            // Create an Options object and initialize it with default options,
            // then override them with options deserialized from ConfigImportPath (if specified)
            Options initOptions = new Options();

            // Initialize options with defaults
            var importSuccessful = TryImportServerOptions(DefaultOptionsEmbeddedFileName,
                ConfigFileType.GarnetConf, initOptions, logger,
                FileLocationType.EmbeddedResource);

            // Get argument name to default value mapping for argument parser
            var argNameToDefaultValue = GetArgumentNameToValue(initOptions);

            var consolidatedArgs = ConsolidateFlagArguments(args);
            // Parse command line arguments
            if (!parser.TryParseArguments<Options>(consolidatedArgs, argNameToDefaultValue, out var cmdLineOptions))
                return false;

            // Check if any arguments were not parsed
            if (cmdLineOptions.UnparsedArguments?.Count > 0)
            {
                // Ignore any unparsed arguments that appeared after '--'
                var unparsedArguments = new List<string>(cmdLineOptions.UnparsedArguments);
                var dashDashIdx = Array.IndexOf(consolidatedArgs, "--");
                if (dashDashIdx >= 0)
                {
                    for (var i = dashDashIdx + 1; i < consolidatedArgs.Length; i++)
                    {
                        unparsedArguments.Remove(consolidatedArgs[i]);
                    }
                }

                // If any unparsed arguments remain, display a warning to the user
                if (unparsedArguments.Count > 0)
                {
                    logger?.LogWarning(@$"The following command line arguments were not parsed: {string.Join(',', unparsedArguments)}. 
Please check the syntax of your command. For detailed usage information run with --help.");
                }
            }

            if (!importSuccessful)
                return false;

            // If config import file present - import options from file
            if (cmdLineOptions.ConfigImportPath != null)
            {
                importSuccessful = TryImportServerOptions(
                    cmdLineOptions.ConfigImportPath,
                    cmdLineOptions.ConfigImportFormat, initOptions, logger,
                    cmdLineOptions.UseAzureStorageForConfigImport.GetValueOrDefault() ? FileLocationType.AzureStorage : FileLocationType.Local,
                    cmdLineOptions.AzureStorageConnectionString);

                if (!importSuccessful)
                    return false;
            }
            else
            {
                logger?.LogInformation("Configuration file path not specified. Using default values with command-line switches.");
            }

            // Re-parse command line arguments after initializing Options object with initialization function
            // In order to override options specified in the command line arguments
            if (!parser.TryParseArguments(consolidatedArgs, argNameToDefaultValue, out options, () => initOptions))
                return false;

            // Validate options
            if (!options.IsValid(out invalidOptions, logger))
            {
                logger?.LogError("Configuration validation failed.");
                options = null;
                return false;
            }

            // Export the settings to file, if ConfigExportPath is specified
            if (options.ConfigExportPath != null)
                TryExportServerOptions(options.ConfigExportPath,
                    options.ConfigExportFormat,
                    options,
                    logger,
                    options.UseAzureStorageForConfigExport.GetValueOrDefault() ? FileLocationType.AzureStorage : FileLocationType.Local,
                    options.AzureStorageConnectionString);

            return true;
        }

        /// <summary>
        /// Gets a mapping between an argument name (LongName defined by OptionAttribute)
        /// and property value in the specified Options object
        /// </summary>
        /// <param name="options">Options object from which to get values</param>
        /// <returns>Argument name to value mapping</returns>
        private static Dictionary<string, object> GetArgumentNameToValue(Options options)
        {
            var argNameToValue = new Dictionary<string, object>();
            foreach (var prop in typeof(Options).GetProperties())
            {
                var optionAttr =
                    (OptionAttribute)prop.GetCustomAttributes(typeof(OptionAttribute)).FirstOrDefault();
                if (optionAttr == null)
                    continue;

                if (!string.IsNullOrEmpty(optionAttr.LongName) &&
                    !argNameToValue.ContainsKey(optionAttr.LongName))
                {
                    argNameToValue.Add(optionAttr.LongName, prop.GetValue(options));
                }
            }

            return argNameToValue;
        }

        /// <summary>
        /// Extension method for CommandLine.Parser, outputs parsed object if parse successful,
        /// Prints error message to console otherwise
        /// </summary>
        /// <typeparam name="T">Type of object to parse</typeparam>
        /// <param name="parser">CommandLine.Parser object</param>
        /// <param name="args">Command line arguments</param>
        /// <param name="argNameToDefaultValue">Argument long name to default value mapping</param>
        /// <param name="obj">Parsed object, default(T) if parse unsuccessful</param>
        /// <param name="factory">Optional T factory for object initialization</param>
        /// <returns>True if parse successful</returns>
        private static bool TryParseArguments<T>(this Parser parser, string[] args, IDictionary<string, object> argNameToDefaultValue, out T obj, Func<T> factory = null) where T : new()
        {
            var result = parser.ParseArguments(factory ?? (() => new T()), args);

            obj = result.MapResult(parsed => parsed,
                notParsed =>
                {
                    var errors = notParsed.ToList();
                    var helpText = HelpText.AutoBuild(result, h => HelpText.DefaultParsingErrorsHandler(result, h), e => e);
                    helpText.Heading = "GarnetServer";
                    helpText.Copyright = "Copyright (c) Microsoft Corporation";

                    // Customizing help text produced by parser
                    // If parse errors occurred, skip printing usage options
                    // If not (i.e. --help or --version requested), append dynamically loaded default values to usage option help text
                    var helpTextBuilder = new StringBuilder();
                    foreach (var line in helpText.ToString().Split(Environment.NewLine))
                    {
                        var match = Regex.Match(line, OptionPattern);
                        if (match.Success)
                        {
                            if (!errors.IsVersion() && !errors.IsHelp())
                            {
                                helpTextBuilder.AppendLine("Encountered error(s) while parsing command line arguments.");
                                helpTextBuilder.AppendLine("For detailed usage information run with --help.");
                                break;
                            }

                            var longName = match.Groups[2].Value;
                            var helpIdx = match.Groups[3].Index;
                            if (argNameToDefaultValue.ContainsKey(longName))
                            {
                                var defaultValue = argNameToDefaultValue[longName];
                                helpTextBuilder.Append(line.Substring(0, helpIdx));
                                if (defaultValue != null && !string.IsNullOrEmpty(defaultValue.ToString()))
                                {
                                    helpTextBuilder.Append($"(Default: {(defaultValue is string str ? $"\"{str}\"" : defaultValue.ToString())}) ");
                                }
                                helpTextBuilder.AppendLine(line.Substring(helpIdx, line.Length - helpIdx));
                                continue;
                            }
                        }

                        helpTextBuilder.AppendLine(line);
                    }

                    Console.WriteLine(helpTextBuilder.ToString());
                    return default;
                });

            return result.Tag == ParserResultType.Parsed;
        }

        /// <summary>
        /// Tries to import options from file into Options object
        /// </summary>
        /// <param name="path">Path of source configuration file</param>
        /// <param name="configFileType">Configuration file type</param>
        /// <param name="options">Options object to import options into</param>
        /// <param name="logger">Logger</param>
        /// <param name="fileLocationType">Type of file location of configuration file</param>
        /// <param name="connString">Connection string to Azure Storage, if applicable</param>
        /// <returns>True if import succeeded</returns>
        private static bool TryImportServerOptions(string path, ConfigFileType configFileType, Options options, ILogger logger, FileLocationType fileLocationType, string connString = null)
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(fileLocationType, connString);
            var configProvider = ConfigProviderFactory.GetConfigProvider(configFileType);

            using var stream = streamProvider.Read(path);
            var importSucceeded = configProvider.TryImportOptions(path, streamProvider, options, logger);

            var fileLocation = fileLocationType switch
            {
                FileLocationType.Local => "local machine",
                FileLocationType.AzureStorage => "Azure storage",
                FileLocationType.EmbeddedResource => "embedded resource",
                _ => throw new NotImplementedException()
            };

            logger?.Log(importSucceeded ? LogLevel.Information : LogLevel.Error,
                $"Configuration import from {fileLocation} {(importSucceeded ? "succeeded" : "failed")}. Path: {path}.");

            return importSucceeded;
        }

        /// <summary>
        /// Tries to export options from Options object into file
        /// </summary>
        /// <param name="path">Path of destination configuration file</param>
        /// <param name="configFileType">Configuration file type</param>
        /// <param name="options">Options object to export</param>
        /// <param name="logger">Logger</param>
        /// <param name="fileLocationType">Type of file location of configuration file</param>
        /// <param name="connString">Connection string to Azure Storage, if applicable</param>
        /// <returns>True if export succeeded</returns>
        private static bool TryExportServerOptions(string path, ConfigFileType configFileType, Options options, ILogger logger, FileLocationType fileLocationType, string connString = null)
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(fileLocationType, connString);
            var configProvider = ConfigProviderFactory.GetConfigProvider(configFileType);

            var exportSucceeded = configProvider.TryExportOptions(path, streamProvider, options, logger);

            var fileLocation = fileLocationType switch
            {
                FileLocationType.Local => "local machine",
                FileLocationType.AzureStorage => "Azure storage",
                FileLocationType.EmbeddedResource => "embedded resource",
                _ => throw new NotImplementedException()
            };

            logger?.Log(exportSucceeded ? LogLevel.Information : LogLevel.Error,
                $"Configuration export to {fileLocation} {(exportSucceeded ? "succeeded" : "failed")}. File path: {path}.");
            return exportSucceeded;
        }

        /// <summary>
        /// This method takes the current list of command line arguments and injects the boolean value "true" after each flag-type argument,
        /// as the current parser does not support flag-type arguments to set nullable bool properties.
        /// </summary>
        /// <param name="args">List of arguments</param>
        /// <returns>Consolidated list of arguments</returns>
        private static string[] ConsolidateFlagArguments(string[] args)
        {
            // Map option keywords to a boolean determining if the Option is of nullable bool type
            var keywordToIsNullableBool = new Dictionary<string, bool>();
            foreach (var prop in typeof(Options).GetProperties())
            {
                var optionAttr = (OptionAttribute)prop.GetCustomAttributes(typeof(OptionAttribute)).FirstOrDefault();
                if (optionAttr == null)
                    continue;

                if (!string.IsNullOrEmpty(optionAttr.ShortName) &&
                    !keywordToIsNullableBool.ContainsKey($"-{optionAttr.ShortName}"))
                {
                    keywordToIsNullableBool.Add($"-{optionAttr.ShortName}", prop.PropertyType == typeof(bool?));
                }

                if (!string.IsNullOrEmpty(optionAttr.LongName) &&
                    !keywordToIsNullableBool.ContainsKey($"--{optionAttr.LongName}"))
                {
                    keywordToIsNullableBool.Add($"--{optionAttr.LongName}", prop.PropertyType == typeof(bool?));
                }
            }

            // Check if argument list requires consolidation
            // If not, return original argument array
            var consolidateArgs = args.Where((arg, idx) =>
                keywordToIsNullableBool.ContainsKey(arg) && keywordToIsNullableBool[arg] &&
                (idx == args.Length - 1 || keywordToIsNullableBool.ContainsKey(args[idx + 1])
                                        || (args[idx + 1].IndexOf('=') != -1 &&
                                            keywordToIsNullableBool.ContainsKey(args[idx + 1]
                                                .Substring(0, args[idx + 1].IndexOf('=')))))).FirstOrDefault() != null;

            if (!consolidateArgs)
                return args;

            var consolidatedArgs = new List<string>();

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i].Trim();

                // Copy existing argument to consolidated argument list
                consolidatedArgs.Add(arg);

                // Check if current argument matches any existing keywords whose Option type is bool?
                if (!keywordToIsNullableBool.ContainsKey(arg) || !keywordToIsNullableBool[arg])
                    continue;

                // Check if next argument is another keyword or is end of argument array
                // If so, the argument is used as a flag
                if (i == args.Length - 1 || keywordToIsNullableBool.ContainsKey(args[i + 1]) ||
                    (args[i + 1].IndexOf('=') != -1 &&
                     keywordToIsNullableBool.ContainsKey(args[i + 1].Substring(0, args[i + 1].IndexOf('=')))))
                {
                    // Add a true value after the argument
                    consolidatedArgs.Add(true.ToString());
                }
            }

            return consolidatedArgs.ToArray();
        }
    }
}