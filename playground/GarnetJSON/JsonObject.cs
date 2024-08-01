// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.server;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GarnetJSON
{
    public class JsonObjectFactory : CustomObjectFactory
    {
        public override CustomObjectBase Create(byte type)
            => new JsonObject(type);

        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new JsonObject(type, reader);
    }

    public class JsonObject : CustomObjectBase
    {
        //readonly Dictionary<string, object> dict;
        JObject jObject;

        public JsonObject(byte type)
            : base(type, 0, MemoryUtils.DictionaryOverhead)
        {
            jObject = new();

            // TODO: update size
        }

        public JsonObject(byte type, BinaryReader reader)
            : base(type, reader, MemoryUtils.DictionaryOverhead)
        {
            Debug.Assert(reader != null);

            var jsonString = reader.ReadString();
            jObject = jsonString != null ? JsonConvert.DeserializeObject<JObject>(jsonString) ?? new() : new();

            // TODO: update size
        }

        public JsonObject(JsonObject obj)
            : base(obj)
        {
            jObject = obj.jObject;
        }

        public override CustomObjectBase CloneObject() => new JsonObject(this);

        public override void SerializeObject(BinaryWriter writer)
        {
            writer.Write(JsonConvert.SerializeObject(jObject));
        }

        public override void Dispose() { }

        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0) => throw new NotImplementedException();

        public void Set(string path, string value)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            foreach (var token in jObject.SelectTokens(path).ToList())
            {
                if (token == jObject)
                {
                    jObject = JObject.Parse(value);
                }
                else
                {
                    token.Replace(JToken.FromObject(value));
                }
            }
        }

        internal string Get(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            return new JArray(jObject.SelectTokens(path))?.ToString() ?? "{}";
        }
    }
}