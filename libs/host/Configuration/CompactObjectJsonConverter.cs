// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using CommandLine;

namespace Garnet
{
    /// <summary>
    /// Serializes an object into a JSON stream while skipping default values
    /// defined by specified object in the constructor.
    /// </summary>
    /// <typeparam name="T">Destination object type</typeparam>
    internal class CompactObjectJsonConverter<T> : JsonConverter<T> where T : class, new()
    {
        private readonly T defaultObj;
        readonly HashSet<string> serializableProperties;

        /// <summary>
        /// Creates a converter that serializes an object to json while skipping default values
        /// </summary>
        /// <param name="defaultInstance">The object containing default values to skip</param>
        /// <param name="includeAttributes">Attributes marking properties to include in serialization (empty of all included)</param>
        /// <param name="excludeAttributes">Attributes marking properties to exclude in serialization (empty if none excluded)</param>
        public CompactObjectJsonConverter(T defaultInstance, Type[] includeAttributes = null, Type[] excludeAttributes = null)
        {
            defaultObj = defaultInstance;
            var attrsToExclude = excludeAttributes == null ? new HashSet<Type>()
                : [.. excludeAttributes.Where(t => t.IsAssignableTo(typeof(Attribute))).Union([typeof(JsonIgnoreAttribute)])];
            var attrsToInclude = includeAttributes == null ? new HashSet<Type>()
                : [.. includeAttributes.Where(t => t.IsAssignableTo(typeof(Attribute)))];

            // Determine properties to include in serialization according to excludeAttributes and includeAttributes
            serializableProperties = new HashSet<string>();
            foreach (var prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                // Property should have at least one included attribute (if any exist) and none of the excluded attributes
                // in order to be serialized
                var writeProp = false;
                foreach (var customAttribute in prop.GetCustomAttributes())
                {
                    if (attrsToExclude.Contains(customAttribute.GetType()))
                    {
                        writeProp = false;
                        break;
                    }

                    if (!writeProp && (attrsToInclude.Count == 0 || attrsToInclude.Contains(customAttribute.GetType())))
                        writeProp = true;
                }

                if (writeProp)
                {
                    serializableProperties.Add(prop.Name);
                }
            }
        }

        /// <inheritdoc />
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return JsonSerializer.Deserialize<T>(ref reader, options);
        }

        /// <inheritdoc />
        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            foreach (var prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                // Check if property can be serialized
                if (!serializableProperties.Contains(prop.Name))
                    continue;

                // Get the property values from the current object and the default object
                var val = prop.GetValue(value);
                var def = prop.GetValue(defaultObj);

                // Only serialize if the current object value is different from the default object value
                if (!AreEqual(prop.PropertyType, val, def))
                {
                    writer.WritePropertyName(prop.Name);
                    JsonSerializer.Serialize(writer, val, prop.PropertyType, options);
                }
            }

            writer.WriteEndObject();
        }

        private static bool AreEqual(Type type, object value, object defaultValue)
        {
            if (value == null && defaultValue == null)
                return true;
            if (value == null || defaultValue == null)
                return false;

            // Handle IEnumerable<T> properties
            if (type != typeof(string) && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                var valueEnum = ((IEnumerable)value).Cast<object>();
                var defaultEnum = ((IEnumerable)defaultValue).Cast<object>();
                return valueEnum.SequenceEqual(defaultEnum);
            }

            return Equals(value, defaultValue);
        }
    }
}