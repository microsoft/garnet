// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.JSONPath
{
    public static class JsonAssert
    {
        public static void AreEqual(JsonElement? left, JsonElement? right)
        {
            ClassicAssert.That(left, Is.EqualTo(right).Using(JsonElementEqualityComparer.Instance));
        }

        public static void AreEqual(JsonDocument left, JsonElement? right)
        {
            ClassicAssert.That(left.RootElement, Is.EqualTo(right).Using(JsonElementEqualityComparer.Instance));
        }

        public static void AreEqual(JsonNode left, JsonNode right)
        {
            ClassicAssert.That(left, Is.EqualTo(right).Using(JsonNodeEqualityComparer.Instance));
        }
    }

    public static class JsonTestExtensions
    {
        public static bool DeepEquals(this JsonElement left, JsonElement? right)
        {
            if (right is null)
            {
                return false;
            }

            return JsonNode.DeepEquals(JsonNode.Parse(JsonSerializer.Serialize(left)),
                JsonNode.Parse(JsonSerializer.Serialize(right.Value)));
        }
    }

    public class JsonElementEqualityComparer : IEqualityComparer<JsonElement?>
    {
        public static JsonElementEqualityComparer Instance { get; } = new JsonElementEqualityComparer();

        public bool Equals(JsonElement? x, JsonElement? y)
        {
            if (x is null && y is null)
            {
                return true;
            }

            if (x is null || y is null)
            {
                return false;
            }

            return x.Value.DeepEquals(y);
        }

        public int GetHashCode([DisallowNull] JsonElement? obj)
        {
            return obj is null ? 0 : obj.GetHashCode();
        }
    }

    public class JsonNodeEqualityComparer : IEqualityComparer<JsonNode>
    {
        public static JsonNodeEqualityComparer Instance { get; } = new JsonNodeEqualityComparer();

        public bool Equals(JsonNode x, JsonNode y)
        {
            if (x is null && y is null)
            {
                return true;
            }

            if (x is null || y is null)
            {
                return false;
            }

            return JsonNode.DeepEquals(x, y);
        }

        public int GetHashCode([DisallowNull] JsonNode obj)
        {
            return obj is null ? 0 : obj.GetHashCode();
        }
    }
}