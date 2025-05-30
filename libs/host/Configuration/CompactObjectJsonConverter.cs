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
    /// defined by specified object in the constructor
    /// </summary>
    /// <typeparam name="T">Destination object type</typeparam>
    public class CompactObjectJsonConverter<T> : JsonConverter<T> where T : class, new()
    {
        private readonly T defaultObj;

        public CompactObjectJsonConverter(T defaultInstance)
        {
            defaultObj = defaultInstance;
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
                // Only write properties with the OptionsAttribute, and are not marked with the JsonIgnore or HiddenOption attributes
                var ignoreAttr = prop.GetCustomAttributes(typeof(JsonIgnoreAttribute)).FirstOrDefault();
                if (ignoreAttr != null)
                    continue;

                var hiddenAttr = prop.GetCustomAttributes(typeof(HiddenOptionAttribute)).FirstOrDefault();
                if (hiddenAttr != null)
                    continue;

                var optionAttr = prop.GetCustomAttributes(typeof(OptionAttribute)).FirstOrDefault();
                if (optionAttr == null)
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
