// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text.Json;
using Garnet.server.Vector.Filter;

namespace Garnet.test
{
    internal static class VectorFilterTestHelpers
    {
        internal static object EvaluateFilter(string expression, string json)
        {
            var tokens = VectorFilterTokenizer.Tokenize(expression);
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            using var doc = JsonDocument.Parse(json);
            var result = VectorFilterEvaluator.EvaluateExpression(expr, doc.RootElement);

            return result.Kind switch
            {
                FilterValueKind.Number => (object)result.AsNumber(),
                FilterValueKind.String => result.AsString(),
                FilterValueKind.Null => null,
                _ => result.AsNumber()
            };
        }

        internal static bool EvaluateFilterTruthy(string expression, string json)
        {
            var tokens = VectorFilterTokenizer.Tokenize(expression);
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            using var doc = JsonDocument.Parse(json);
            var result = VectorFilterEvaluator.EvaluateExpression(expr, doc.RootElement);
            return VectorFilterEvaluator.IsTruthy(result);
        }
    }
}
