// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using CommandLine;

namespace Resp.benchmark
{
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class ConfigurationAliasAttribute(string propertyName) : Attribute
    {
        public string PropertyName { get; } = propertyName;
    }

    public partial class Options
    {
        /// <summary>
        /// Parse CLI defaults and explicit overrides, then fill unspecified options from a JSON file.
        /// </summary>
        /// <param name="args">Command-line arguments.</param>
        /// <param name="options">Resolved options, or null on failure.</param>
        /// <param name="exitGracefully">Whether help or version information was requested.</param>
        /// <returns>Whether parsing succeeded.</returns>
        public static bool TryParse(string[] args, out Options options, out bool exitGracefully)
        {
            options = null;
            exitGracefully = false;
            var properties = typeof(Options).GetProperties()
                .Where(property => property.GetCustomAttribute<OptionAttribute>() != null).ToArray();
            var normalizedArgs = new List<string>();
            var booleanOverrides = new Dictionary<PropertyInfo, bool>();
            foreach (var argument in args)
            {
                var name = argument.Split('=', 2)[0];
                var property = properties.FirstOrDefault(property =>
                {
                    var attribute = property.GetCustomAttribute<OptionAttribute>();
                    return name == $"--{attribute.LongName}" ||
                        (!string.IsNullOrEmpty(attribute.ShortName) && name == $"-{attribute.ShortName}");
                });
                if (property?.GetCustomAttribute<ConfigurationAliasAttribute>() is ConfigurationAliasAttribute alias)
                    property = typeof(Options).GetProperty(alias.PropertyName);
                if (property?.PropertyType == typeof(bool) && argument.Contains('='))
                {
                    if (!bool.TryParse(argument[(name.Length + 1)..], out var value))
                    {
                        Console.Error.WriteLine($"Invalid boolean option: {name}. Expected true or false.");
                        return false;
                    }
                    booleanOverrides[property] = value;
                    normalizedArgs.Add($"--{property.GetCustomAttribute<OptionAttribute>().LongName}");
                }
                else
                {
                    normalizedArgs.Add(property == null ? argument :
                        $"--{property.GetCustomAttribute<OptionAttribute>().LongName}{argument[name.Length..]}");
                }
            }
            var result = Parser.Default.ParseArguments<Options>(normalizedArgs);
            if (result is NotParsed<Options> notParsed)
            {
                exitGracefully = notParsed.Errors.All(error => error is HelpRequestedError or VersionRequestedError);
                return false;
            }

            var parsed = ((Parsed<Options>)result).Value;
            foreach (var entry in booleanOverrides)
                entry.Key.SetValue(parsed, entry.Value);
            if (parsed.ConfigFile == null)
            {
                options = parsed;
                return true;
            }

            var byName = properties.ToDictionary(property => property.Name, StringComparer.OrdinalIgnoreCase);
            var overridden = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var argument in normalizedArgs)
            {
                var name = argument.Split('=', 2)[0];
                foreach (var property in properties)
                {
                    var attribute = property.GetCustomAttribute<OptionAttribute>();
                    if (name == $"--{attribute.LongName}")
                    {
                        overridden.Add(property.GetCustomAttribute<ConfigurationAliasAttribute>()?.PropertyName ?? property.Name);
                    }
                }
                if (!argument.StartsWith('-') || argument.StartsWith("--", StringComparison.Ordinal))
                    continue;
                for (var index = 1; index < argument.Length; index++)
                {
                    var shortName = argument[index].ToString();
                    var property = properties.FirstOrDefault(property => property.GetCustomAttribute<OptionAttribute>().ShortName == shortName);
                    if (property == null)
                        break;
                    overridden.Add(property.Name);
                    if (property.PropertyType != typeof(bool))
                        break;
                }
            }

            try
            {
                using var document = JsonDocument.Parse(File.ReadAllText(parsed.ConfigFile));
                if (document.RootElement.ValueKind != JsonValueKind.Object)
                    throw new JsonException("Benchmark configuration must be a JSON object.");
                var serializerOptions = new JsonSerializerOptions
                {
                    Converters = { new JsonStringEnumConverter(allowIntegerValues: false) }
                };
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var entry in document.RootElement.EnumerateObject())
                {
                    if (!byName.TryGetValue(entry.Name, out var property) ||
                        property.Name is nameof(ConfigFile) or nameof(ValidateConfig) ||
                        property.GetCustomAttribute<ConfigurationAliasAttribute>() != null)
                        throw new JsonException($"Unknown benchmark configuration option: {entry.Name}");
                    if (!seen.Add(property.Name))
                        throw new JsonException($"Duplicate benchmark configuration option: {entry.Name}");
                    if (entry.Value.ValueKind == JsonValueKind.Null)
                        throw new JsonException($"Benchmark configuration option cannot be null: {entry.Name}");
                    var value = entry.Value.Deserialize(property.PropertyType, serializerOptions);
                    if (!overridden.Contains(property.Name))
                        property.SetValue(parsed, value);
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
            {
                Console.Error.WriteLine($"Unable to load benchmark configuration '{parsed.ConfigFile}': {ex.Message}");
                return false;
            }

            options = parsed;
            return true;
        }
    }
}