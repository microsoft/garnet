// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Garnet
{
    /// <summary>
    /// Deserializes a JSON-serialized stream into an existing object
    /// </summary>
    /// <typeparam name="T">Destination object type</typeparam>
    public class PopulateObjectJsonConverter<T> : JsonConverter<T> where T : class, new()
    {
        private readonly T existingInstance;

        /// <summary>
        /// Create a new converter instance
        /// </summary>
        /// <param name="existingInstance">Instance to populate</param>
        public PopulateObjectJsonConverter(T existingInstance)
        {
            this.existingInstance = existingInstance;
        }

        /// <inheritdoc />
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException("Expected start of JSON object.");
            }

            var jsonDocument = JsonDocument.ParseValue(ref reader);

            // Only override properties that are specified in the source document
            foreach (var property in jsonDocument.RootElement.EnumerateObject())
            {
                var propertyInfo = typeof(T).GetProperty(property.Name);
                if (propertyInfo != null && propertyInfo.CanWrite)
                {
                    var propertyValue = JsonSerializer.Deserialize(property.Value.GetRawText(), propertyInfo.PropertyType, options);
                    propertyInfo.SetValue(existingInstance, propertyValue);
                }
            }

            return existingInstance;
        }

        /// <inheritdoc />
        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            JsonSerializer.Serialize(writer, value, options);
        }
    }

}