// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GarnetJSON
{
    /// <summary>
    /// Represents a factory for creating instances of <see cref="JsonObject"/>.
    /// </summary>
    public class JsonObjectFactory : CustomObjectFactory
    {
        /// <summary>
        /// Creates a new instance of <see cref="JsonObject"/> with the specified type.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <returns>A new instance of <see cref="JsonObject"/>.</returns>
        public override CustomObjectBase Create(byte type)
            => new JsonObject(type);

        /// <summary>
        /// Deserializes a <see cref="JsonObject"/> from the specified binary reader.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <param name="reader">The binary reader to deserialize from.</param>
        /// <returns>A deserialized instance of <see cref="JsonObject"/>.</returns>
        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new JsonObject(type, reader);
    }

    /// <summary>
    /// Represents a JSON object that supports SET and GET operations using JSON path.
    /// </summary>
    public class JsonObject : CustomObjectBase
    {
        private JObject jObject;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonObject"/> class with the specified type.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        public JsonObject(byte type)
            : base(type, 0, MemoryUtils.DictionaryOverhead)
        {
            jObject = new();
            // TODO: update size
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonObject"/> class by deserializing from the specified binary reader.
        /// </summary>
        /// <param name="type">The type of the object.</param>
        /// <param name="reader">The binary reader to deserialize from.</param>
        public JsonObject(byte type, BinaryReader reader)
            : base(type, reader, MemoryUtils.DictionaryOverhead)
        {
            Debug.Assert(reader != null);

            var jsonString = reader.ReadString();
            jObject = jsonString != null ? JsonConvert.DeserializeObject<JObject>(jsonString) ?? new() : new();
            // TODO: update size
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonObject"/> class by cloning another <see cref="JsonObject"/> instance.
        /// </summary>
        /// <param name="obj">The <see cref="JsonObject"/> instance to clone.</param>
        public JsonObject(JsonObject obj)
            : base(obj)
        {
            jObject = obj.jObject;
        }

        /// <summary>
        /// Creates a new instance of <see cref="JsonObject"/> that is a clone of the current instance.
        /// </summary>
        /// <returns>A new instance of <see cref="JsonObject"/> that is a clone of the current instance.</returns>
        public override CustomObjectBase CloneObject() => new JsonObject(this);

        /// <summary>
        /// Serializes the <see cref="JsonObject"/> to the specified binary writer.
        /// </summary>
        /// <param name="writer">The binary writer to serialize to.</param>
        public override void SerializeObject(BinaryWriter writer)
        {
            writer.Write(JsonConvert.SerializeObject(jObject));
        }

        /// <summary>
        /// Disposes the <see cref="JsonObject"/> instance.
        /// </summary>
        public override void Dispose() { }

        /// <summary>
        /// Tries to set the value at the specified JSON path.
        /// </summary>
        /// <param name="path">The JSON path.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="logger">The logger to log any errors.</param>
        /// <returns><c>true</c> if the value was successfully set; otherwise, <c>false</c>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="path"/> or <paramref name="value"/> is <c>null</c>.</exception>
        public bool TrySet(string path, string value, Microsoft.Extensions.Logging.ILogger logger)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            try
            {
                Set(path, value);
                return true;
            }
            catch (JsonException ex)
            {
                logger?.LogError(ex, "Failed to set JSON value");
                return false;
            }
        }

        private void Set(string path, string value)
        {
            var tokens = jObject.SelectTokens(path);
            if (!tokens.Any())
            {
                // Create the element if it doesn't exist
                JToken? parentToken;
                parentToken = jObject.SelectToken(GetParentPath(path), true);
                var propertyName = GetPropertyName(path);
                if (parentToken is JObject parentObject)
                {
                    parentObject.Add(propertyName, JToken.Parse(value));
                }
                else if (parentToken is JArray parentArray)
                {
                    var index = GetArrayIndex(path);
                    parentArray.Insert(index, JToken.Parse(value));
                }
            }
            else
            {
                foreach (var token in tokens)
                {
                    if (token == jObject)
                    {
                        jObject = JObject.Parse(value);
                    }
                    else
                    {
                        var newToken = JToken.Parse(value);
                        token.Replace(newToken);
                    }
                }
            }
        }

        private string GetParentPath(string path)
        {
            // TODO: Support other notations as well
            var lastDotIndex = path.LastIndexOf('.');
            return lastDotIndex >= 0 ? path.Substring(0, lastDotIndex) : "";
        }

        private string GetPropertyName(string path)
        {
            var lastDotIndex = path.LastIndexOf('.');
            return lastDotIndex >= 0 ? path.Substring(lastDotIndex + 1) : path;
        }

        private int GetArrayIndex(string path)
        {
            var startIndex = path.LastIndexOf('[');
            var endIndex = path.LastIndexOf(']');
            if (startIndex >= 0 && endIndex >= 0 && endIndex > startIndex)
            {
                var indexString = path.Substring(startIndex + 1, endIndex - startIndex - 1);
                if (int.TryParse(indexString, out var index))
                {
                    return index;
                }
            }

            throw new ArgumentException("Invalid array index in path");
        }


        /// <summary>
        /// Tries to get the value at the specified JSON path.
        /// </summary>
        /// <param name="path">The JSON path.</param>
        /// <param name="jsonString">The JSON string value at the specified path, or <c>null</c> if the value is not found.</param>
        /// <param name="logger">The logger to log any errors.</param>
        /// <returns><c>true</c> if the value was successfully retrieved; otherwise, <c>false</c>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="path"/> is <c>null</c>.</exception>
        public bool TryGet(string path, out string jsonString, ILogger logger)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            try
            {
                jsonString = new JArray(jObject.SelectTokens(path))?.ToString(Formatting.None) ?? "[]";
                return true;
            }
            catch (JsonException ex)
            {
                logger?.LogError(ex, "Failed to get JSON value");
                jsonString = string.Empty;
                return false;
            }
        }

        /// <inheritdoc/>
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0) => throw new NotImplementedException();
    }
}