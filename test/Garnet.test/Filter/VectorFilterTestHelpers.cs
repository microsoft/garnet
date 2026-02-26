// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text.Json;
using Garnet.server.Vector.Filter;

namespace Garnet.test
{
    internal static class VectorFilterTestHelpers
    {
        internal static FilterValue EvaluateFilter(string expression, string json)
        {
            if (!VectorFilterTokenizer.TryTokenize(expression, out var tokens, out var error))
                throw new System.InvalidOperationException($"Tokenization failed: {error}");
            if (!VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out error))
                throw new System.InvalidOperationException($"Parse failed: {error}");
            using var doc = JsonDocument.Parse(json);
            return VectorFilterEvaluator.EvaluateExpression(expr, doc.RootElement);
        }

        internal static bool EvaluateFilterTruthy(string expression, string json)
        {
            if (!VectorFilterTokenizer.TryTokenize(expression, out var tokens, out var error))
                throw new System.InvalidOperationException($"Tokenization failed: {error}");
            if (!VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out error))
                throw new System.InvalidOperationException($"Parse failed: {error}");
            using var doc = JsonDocument.Parse(json);
            return VectorFilterEvaluator.EvaluateFilterBool(expr, doc.RootElement);
        }

        internal static bool TryTokenize(string expression, out System.Collections.Generic.List<Token> tokens, out string error)
        {
            return VectorFilterTokenizer.TryTokenize(expression, out tokens, out error);
        }

        internal static bool TryParse(string expression, out Expr result, out int end, out string error)
        {
            if (!VectorFilterTokenizer.TryTokenize(expression, out var tokens, out error))
            {
                result = null;
                end = 0;
                return false;
            }
            return VectorFilterParser.TryParseExpression(tokens, 0, out result, out end, out error);
        }
    }
}