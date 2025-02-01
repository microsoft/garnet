using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.Text.Json.Nodes;
using System.Linq;
using System.Text.Json;
using System.Runtime.InteropServices;
using System.Diagnostics.CodeAnalysis;

namespace GarnetJSON.JSONPath
{
    internal enum QueryOperator
    {
        None = 0,
        Equals = 1,
        NotEquals = 2,
        Exists = 3,
        LessThan = 4,
        LessThanOrEquals = 5,
        GreaterThan = 6,
        GreaterThanOrEquals = 7,
        And = 8,
        Or = 9,
        RegexEquals = 10,
        StrictEquals = 11,
        StrictNotEquals = 12
    }

    internal abstract class QueryExpression
    {
        internal QueryOperator Operator;

        public QueryExpression(QueryOperator @operator)
        {
            Operator = @operator;
        }

        public abstract bool IsMatch(JsonNode root, JsonNode? t, JsonSelectSettings? settings = null);
    }

    internal class CompositeExpression : QueryExpression
    {
        public List<QueryExpression> Expressions { get; set; }

        public CompositeExpression(QueryOperator @operator) : base(@operator)
        {
            Expressions = new List<QueryExpression>();
        }

        public override bool IsMatch(JsonNode root, JsonNode? t, JsonSelectSettings? settings = null)
        {
            switch (Operator)
            {
                case QueryOperator.And:
                    foreach (QueryExpression e in Expressions)
                    {
                        if (!e.IsMatch(root, t, settings))
                        {
                            return false;
                        }
                    }
                    return true;
                case QueryOperator.Or:
                    foreach (QueryExpression e in Expressions)
                    {
                        if (e.IsMatch(root, t, settings))
                        {
                            return true;
                        }
                    }
                    return false;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    internal class BooleanQueryExpression : QueryExpression
    {
        public readonly object? Left;
        public readonly object? Right;

        public BooleanQueryExpression(QueryOperator @operator, object? left, object? right) : base(@operator)
        {
            Left = left;
            Right = right;
        }

        public override bool IsMatch(JsonNode root, JsonNode? t, JsonSelectSettings? settings = null)
        {
            if (Operator == QueryOperator.Exists)
            {
                return Left is List<PathFilter> left ? JsonPath.Evaluate(left, root, t, settings).Any() : true;
            }

            if (Left is List<PathFilter> leftPath)
            {
                foreach (var leftResult in JsonPath.Evaluate(leftPath, root, t, settings))
                {
                    if (EvaluateMatch(root, t, settings, leftResult))
                    {
                        return true;
                    }
                }
            }
            else if (Left is JsonNode left)
            {
                return EvaluateMatch(root, t, settings, left);
            }
            else if (Left is null)
            {
                return EvaluateMatch(root, t, settings, null);
            }

            return false;

            bool EvaluateMatch(JsonNode root, JsonNode? t, JsonSelectSettings? settings, JsonNode? leftResult)
            {
                if (Right is List<PathFilter> right)
                {
                    foreach (var rightResult in JsonPath.Evaluate(right, root, t, settings))
                    {
                        if (MatchTokens(leftResult, rightResult, settings))
                        {
                            return true;
                        }
                    }
                }
                else if (Right is JsonNode rightNode)
                {
                    return MatchTokens(leftResult, rightNode, settings);
                }
                else if (Right is null)
                {
                    return MatchTokens(leftResult, null, settings);
                }

                return false;
            }
        }

        private bool MatchTokens(JsonNode? leftResult, JsonNode? rightResult, JsonSelectSettings? settings)
        {
            if (leftResult is JsonValue or null && rightResult is JsonValue or null)
            {
                var left = leftResult as JsonValue;
                var right = rightResult as JsonValue;
                switch (Operator)
                {
                    case QueryOperator.RegexEquals:
                        return RegexEquals(left, right, settings);
                    case QueryOperator.Equals:
                        return EqualsWithStringCoercion(left, right);
                    case QueryOperator.StrictEquals:
                        return EqualsWithStrictMatch(left, right);
                    case QueryOperator.NotEquals:
                        return !EqualsWithStringCoercion(left, right);
                    case QueryOperator.StrictNotEquals:
                        return !EqualsWithStrictMatch(left, right);
                    case QueryOperator.GreaterThan:
                        return CompareTo(left, right) > 0;
                    case QueryOperator.GreaterThanOrEquals:
                        return CompareTo(left, right) >= 0;
                    case QueryOperator.LessThan:
                        return CompareTo(left, right) < 0;
                    case QueryOperator.LessThanOrEquals:
                        return CompareTo(left, right) <= 0;
                    case QueryOperator.Exists:
                        return true;
                }
            }
            else
            {
                switch (Operator)
                {
                    case QueryOperator.Exists:
                    case QueryOperator.NotEquals:
                        return true;
                }
            }

            return false;
        }

        internal static int CompareTo(JsonValue? leftValue, JsonValue? rightValue)
        {
            if (leftValue is null)
            {
                return rightValue is null ? 0 : -1;
            }

            if (rightValue is null)
            {
                return 1;
            }

            if (leftValue.GetValueKind() == rightValue.GetValueKind())
            {
                if (leftValue is null)
                {
                    return 0;
                }

                switch (leftValue.GetValueKind())
                {
                    case JsonValueKind.False:
                    case JsonValueKind.True:
                    case JsonValueKind.Null:
                    case JsonValueKind.Undefined:
                        return 0;
                    case JsonValueKind.String:
                        return leftValue.GetValue<string>()!.CompareTo(rightValue!.GetValue<string>());
                    case JsonValueKind.Number:
                        if (leftValue.TryGetValue<long>(out var left))
                        {
                            return rightValue.TryGetValue<long>(out var right) ? left.CompareTo(right) : left.CompareTo((long)rightValue.GetValue<double>());
                        }
                        else
                        {
                            return rightValue.TryGetValue<long>(out var right) ? ((long)leftValue.GetValue<double>()).CompareTo(right) : leftValue.GetValue<double>().CompareTo(rightValue.GetValue<double>());
                        }
                    default:
                        throw new InvalidOperationException($"Can compare only value types, but the current type is: {leftValue.GetValueKind()}");
                }
            }

            if (IsBoolean(leftValue) && IsBoolean(rightValue))
            {
                return leftValue.GetValue<bool>().CompareTo(rightValue.GetValue<bool>());
            }

            if (TryGetAsDouble(leftValue, out double leftNum) && TryGetAsDouble(rightValue, out double rightNum))
            {
                return leftNum.CompareTo(rightNum);
            }

            if (leftValue is null || rightValue is null)
            {
                return 0;
            }

            return leftValue.GetValue<string>().CompareTo(rightValue.GetValue<string>());
        }

        private static bool RegexEquals(JsonValue? input, JsonValue? pattern, JsonSelectSettings? settings)
        {
            if (input is null || pattern is null || input.GetValueKind() != JsonValueKind.String || pattern.GetValueKind() != JsonValueKind.String)
            {
                return false;
            }

            string regexText = pattern.GetValue<string>();
            int patternOptionDelimiterIndex = regexText.LastIndexOf('/');

            string patternText = regexText.Substring(1, patternOptionDelimiterIndex - 1);
            string optionsText = regexText.Substring(patternOptionDelimiterIndex + 1);

            TimeSpan timeout = settings?.RegexMatchTimeout ?? Regex.InfiniteMatchTimeout;
            return Regex.IsMatch(input.GetValue<string>(), patternText, GetRegexOptions(optionsText), timeout);

            RegexOptions GetRegexOptions(string optionsText)
            {
                RegexOptions options = RegexOptions.None;

                for (int i = 0; i < optionsText.Length; i++)
                {
                    switch (optionsText[i])
                    {
                        case 'i':
                            options |= RegexOptions.IgnoreCase;
                            break;
                        case 'm':
                            options |= RegexOptions.Multiline;
                            break;
                        case 's':
                            options |= RegexOptions.Singleline;
                            break;
                        case 'x':
                            options |= RegexOptions.ExplicitCapture;
                            break;
                    }
                }

                return options;
            }
        }

        internal static bool EqualsWithStringCoercion(JsonValue? value, JsonValue? queryValue)
        {
            if (value is null && value is null)
            {
                return true;
            }

            if (value is null || queryValue is null)
            {
                return false;
            }

            if (TryGetAsDouble(value, out double leftNum) && TryGetAsDouble(queryValue, out double rightNum))
            {
                return leftNum == rightNum;
            }

            if (IsBoolean(value) && IsBoolean(queryValue))
            {
                return value.GetValue<bool>() == queryValue.GetValue<bool>();
            }

            if (queryValue.GetValueKind() != JsonValueKind.String)
            {
                return false;
            }

            var queryValueText = queryValue.GetValue<string>();

            switch (value.GetValueKind())
            {
                case JsonValueKind.String:
                    return string.Equals(value.GetValue<string>(), queryValueText, StringComparison.Ordinal);
                case JsonValueKind.True:
                case JsonValueKind.False:
                    return bool.TryParse(queryValueText, out var queryBool) && queryBool == value.GetValue<bool>();
                default:
                    return false;
            }
        }

        internal static bool EqualsWithStrictMatch(JsonValue? value, JsonValue? queryValue)
        {
            if (value is null)
            {
                return queryValue is null;
            }

            if (queryValue is null)
            {
                return false;
            }

            if (IsBoolean(value) && IsBoolean(queryValue))
            {
                return value.GetValue<bool>() == queryValue.GetValue<bool>();
            }

            if (value.GetValueKind() != queryValue.GetValueKind())
            {
                return false;
            }

            if (value.GetValueKind() == JsonValueKind.Number)
            {
                if (value.TryGetValue<double>(out var valueNum))
                {
                    return queryValue.TryGetValue<double>(out var queryNum) ? valueNum == queryNum : valueNum == queryValue.GetValue<long>();
                }
                else
                {
                    return queryValue.TryGetValue<double>(out var queryNum) ? value.GetValue<long>() == queryNum : value.GetValue<long>() == queryValue.GetValue<long>();
                }
            }

            if (value.GetValueKind() == JsonValueKind.String)
            {
                return string.Equals(value.GetValue<string>(), queryValue.GetValue<string>(), StringComparison.Ordinal);
            }

            if (value.GetValueKind() == JsonValueKind.Null && queryValue.GetValueKind() == JsonValueKind.Null)
            {
                return true;
            }

            if (value.GetValueKind() == JsonValueKind.Undefined && queryValue.GetValueKind() == JsonValueKind.Undefined)
            {
                return true;
            }

            return false;
        }

        private static bool IsBoolean([NotNullWhen(true)] JsonNode? v) => v is not null && (v.GetValueKind() == JsonValueKind.False || v.GetValueKind() == JsonValueKind.True);

        private static bool TryGetAsDouble(JsonValue? value, out double num)
        {
            if (value is null)
            {
                num = default;
                return false;
            }

            if (value.GetValueKind() == JsonValueKind.Number)
            {
                num = value.TryGetValue<double>(out var valueNum) ? valueNum : value.GetValue<long>();
                return true;
            }

            if (value.GetValueKind() == JsonValueKind.String && double.TryParse(value.GetValue<string>(), out num))
            {
                return true;
            }

            num = default;
            return false;
        }
    }
}