#region License
// Copyright (c) 2007 James Newton-King
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion

using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GarnetJSON.JSONPath
{
    /// <summary>
    /// Represents a JSON Path expression.
    /// </summary>
    internal class JsonPath
    {
        private static readonly char[] FloatCharacters = ['.', 'E', 'e'];
        private static readonly JsonValue TrueJsonValue = JsonValue.Create(true);
        private static readonly JsonValue FalseJsonValue = JsonValue.Create(false);

        private readonly string _expression;

        /// <summary>
        /// Gets the list of path filters.
        /// </summary>
        /// <value>
        /// A list of <see cref="PathFilter"/> objects that represent the filters in the JSON Path expression.
        /// </value>
        public List<PathFilter> Filters { get; }

        private int _currentIndex;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonPath"/> class with the specified expression.
        /// </summary>
        /// <param name="expression">The JSON Path expression.</param>
        /// <exception cref="ArgumentNullException">Thrown when the expression is null.</exception>
        public JsonPath(string expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            _expression = expression;
            Filters = new List<PathFilter>();

            ParseMain();
        }

        /// <summary>
        /// Determines whether the current JPath is a static path.
        /// A static path is guaranteed to return at most one result.
        /// </summary>
        /// <returns>true if the path is static; otherwise, false.</returns>
        internal bool IsStaticPath()
        {
            return Filters.All(filter => filter switch
            {
                FieldFilter fieldFilter => fieldFilter.Name is not null,
                ArrayIndexFilter arrayFilter => arrayFilter.Index.HasValue,
                RootFilter => true,
                _ => false
            });
        }

        /// <summary>
        /// Evaluates the JSON Path expression against the provided JSON nodes.
        /// </summary>
        /// <param name="root">The root JSON node.</param>
        /// <param name="t">The current JSON node.</param>
        /// <param name="settings">The settings used for JSON selection.</param>
        /// <returns>An enumerable of JSON nodes that match the JSON Path expression.</returns>
        internal IEnumerable<JsonNode?> Evaluate(JsonNode root, JsonNode? t, JsonSelectSettings? settings)
        {
            return Evaluate(Filters, root, t, settings);
        }

        /// <summary>
        /// Evaluates the JSON Path expression against the provided JSON nodes using the specified filters.
        /// </summary>
        /// <param name="filters">The list of path filters to apply.</param>
        /// <param name="root">The root JSON node.</param>
        /// <param name="t">The current JSON node.</param>
        /// <param name="settings">The settings used for JSON selection.</param>
        /// <returns>An enumerable of JSON nodes that match the JSON Path expression.</returns>
        internal static IEnumerable<JsonNode?> Evaluate(List<PathFilter> filters, JsonNode root, JsonNode? t, JsonSelectSettings? settings)
        {
            if (filters.Count >= 1)
            {
                var firstFilter = filters[0];
                var current = firstFilter.ExecuteFilter(root, t, settings);

                for (int i = 1; i < filters.Count; i++)
                {
                    current = filters[i].ExecuteFilter(root, current, settings);
                }
                return current;
            }
            else
            {
                return new List<JsonNode?> { t };
            }
        }

        /// <summary>
        /// Parses the main JSON Path expression and builds the filter list.
        /// </summary>
        private void ParseMain()
        {
            int currentPartStartIndex = _currentIndex;

            EatWhitespace();

            if (_expression.Length == _currentIndex)
            {
                return;
            }

            if (_expression[_currentIndex] == '$')
            {
                if (_expression.Length == 1)
                {
                    return;
                }

                // only increment position for "$." or "$["
                // otherwise assume property that starts with $
                char c = _expression[_currentIndex + 1];
                if (c == '.' || c == '[')
                {
                    _currentIndex++;
                    currentPartStartIndex = _currentIndex;
                }
            }

            if (!ParsePath(Filters, currentPartStartIndex, false))
            {
                int lastCharacterIndex = _currentIndex;

                EatWhitespace();

                if (_currentIndex < _expression.Length)
                {
                    throw new JsonException("Unexpected character while parsing path: " + _expression[lastCharacterIndex]);
                }
            }
        }

        /// <summary>
        /// Parses a path segment and adds appropriate filters to the filter list.
        /// </summary>
        /// <param name="filters">The list to add parsed filters to.</param>
        /// <param name="currentPartStartIndex">The starting index of the current path segment.</param>
        /// <param name="query">Indicates if parsing within a query context.</param>
        /// <returns>True if parsing reached the end of the expression; otherwise, false.</returns>
        private bool ParsePath(List<PathFilter> filters, int currentPartStartIndex, bool query)
        {
            bool scan = false;
            bool followingIndexer = false;
            bool followingDot = false;

            bool ended = false;
            while (_currentIndex < _expression.Length && !ended)
            {
                char currentChar = _expression[_currentIndex];

                switch (currentChar)
                {
                    case '[':
                    case '(':
                        if (_currentIndex > currentPartStartIndex)
                        {
                            string? member = _expression.Substring(currentPartStartIndex, _currentIndex - currentPartStartIndex);
                            if (member == "*")
                            {
                                member = null;
                            }

                            filters.Add(CreatePathFilter(member, scan));
                            scan = false;
                        }

                        filters.Add(ParseIndexer(currentChar, scan));
                        scan = false;

                        _currentIndex++;
                        currentPartStartIndex = _currentIndex;
                        followingIndexer = true;
                        followingDot = false;
                        break;
                    case ']':
                    case ')':
                        ended = true;
                        break;
                    case ' ':
                        if (_currentIndex < _expression.Length)
                        {
                            ended = true;
                        }
                        break;
                    case '.':
                        if (_currentIndex > currentPartStartIndex)
                        {
                            string? member = _expression.Substring(currentPartStartIndex, _currentIndex - currentPartStartIndex);
                            if (member == "*")
                            {
                                member = null;
                            }

                            filters.Add(CreatePathFilter(member, scan));
                            scan = false;
                        }
                        if (_currentIndex + 1 < _expression.Length && _expression[_currentIndex + 1] == '.')
                        {
                            scan = true;
                            _currentIndex++;
                        }
                        _currentIndex++;
                        currentPartStartIndex = _currentIndex;
                        followingIndexer = false;
                        followingDot = true;
                        break;
                    default:
                        if (query && (currentChar == '=' || currentChar == '<' || currentChar == '!' || currentChar == '>' || currentChar == '|' || currentChar == '&'))
                        {
                            ended = true;
                        }
                        else
                        {
                            if (followingIndexer)
                            {
                                throw new JsonException("Unexpected character following indexer: " + currentChar);
                            }

                            _currentIndex++;
                        }
                        break;
                }
            }

            bool atPathEnd = (_currentIndex == _expression.Length);

            if (_currentIndex > currentPartStartIndex)
            {
                // TODO: Check performance of using AsSpan and TrimEnd then convert to string for the critical path
                string? member = _expression.Substring(currentPartStartIndex, _currentIndex - currentPartStartIndex).TrimEnd();
                if (member == "*")
                {
                    member = null;
                }
                filters.Add(CreatePathFilter(member, scan));
            }
            else
            {
                // no field name following dot in path and at end of base path/query
                if (followingDot && (atPathEnd || query))
                {
                    throw new JsonException("Unexpected end while parsing path.");
                }
            }

            return atPathEnd;
        }

        /// <summary>
        /// Creates a path filter based on the member name and scan flag.
        /// </summary>
        /// <param name="member">The member name for the filter.</param>
        /// <param name="scan">Indicates if this is a scan operation.</param>
        /// <returns>A new PathFilter instance.</returns>
        private static PathFilter CreatePathFilter(string? member, bool scan)
        {
            PathFilter filter = (scan) ? (PathFilter)new ScanFilter(member) : new FieldFilter(member);
            return filter;
        }

        /// <summary>
        /// Parses an indexer expression starting with '[' or '('.
        /// </summary>
        /// <param name="indexerOpenChar">The opening character of the indexer.</param>
        /// <param name="scan">Indicates if this is a scan operation.</param>
        /// <returns>A PathFilter representing the parsed indexer.</returns>
        private PathFilter ParseIndexer(char indexerOpenChar, bool scan)
        {
            _currentIndex++;

            char indexerCloseChar = (indexerOpenChar == '[') ? ']' : ')';

            EnsureLength("Path ended with open indexer.");

            EatWhitespace();

            if (_expression[_currentIndex] == '\'')
            {
                return ParseQuotedField(indexerCloseChar, scan);
            }
            else if (_expression[_currentIndex] == '?')
            {
                return ParseQuery(indexerCloseChar, scan);
            }
            else
            {
                return ParseArrayIndexer(indexerCloseChar);
            }
        }

        /// <summary>
        /// Parses an array indexer expression, supporting single index, multiple indexes, or slice notation.
        /// </summary>
        /// <param name="indexerCloseChar">The closing character of the indexer.</param>
        /// <returns>A PathFilter representing the array indexer.</returns>
        private PathFilter ParseArrayIndexer(char indexerCloseChar)
        {
            int start = _currentIndex;
            int? end = null;
            List<int>? indexes = null;
            int colonCount = 0;
            int? startIndex = null;
            int? endIndex = null;
            int? step = null;

            while (_currentIndex < _expression.Length)
            {
                char currentCharacter = _expression[_currentIndex];

                if (currentCharacter == ' ')
                {
                    end = _currentIndex;
                    EatWhitespace();
                    continue;
                }

                if (currentCharacter == indexerCloseChar)
                {
                    int length = (end ?? _currentIndex) - start;

                    if (indexes != null)
                    {
                        if (length == 0)
                        {
                            throw new JsonException("Array index expected.");
                        }

                        var indexer = _expression.AsSpan(start, length);
                        int index = int.Parse(indexer, CultureInfo.InvariantCulture);

                        indexes.Add(index);
                        return new ArrayMultipleIndexFilter(indexes);
                    }
                    else if (colonCount > 0)
                    {
                        if (length > 0)
                        {
                            var indexer = _expression.AsSpan(start, length);
                            int index = int.Parse(indexer, CultureInfo.InvariantCulture);

                            if (colonCount == 1)
                            {
                                endIndex = index;
                            }
                            else
                            {
                                step = index;
                            }
                        }

                        return new ArraySliceFilter { Start = startIndex, End = endIndex, Step = step };
                    }
                    else
                    {
                        if (length == 0)
                        {
                            throw new JsonException("Array index expected.");
                        }

                        var indexer = _expression.AsSpan(start, length);
                        int index = int.Parse(indexer, CultureInfo.InvariantCulture);

                        return new ArrayIndexFilter { Index = index };
                    }
                }
                else if (currentCharacter == ',')
                {
                    int length = (end ?? _currentIndex) - start;

                    if (length == 0)
                    {
                        throw new JsonException("Array index expected.");
                    }

                    if (indexes == null)
                    {
                        indexes = new List<int>();
                    }

                    var indexer = _expression.AsSpan(start, length);
                    indexes.Add(int.Parse(indexer, CultureInfo.InvariantCulture));

                    _currentIndex++;

                    EatWhitespace();

                    start = _currentIndex;
                    end = null;
                }
                else if (currentCharacter == '*')
                {
                    _currentIndex++;
                    EnsureLength("Path ended with open indexer.");
                    EatWhitespace();

                    if (_expression[_currentIndex] != indexerCloseChar)
                    {
                        throw new JsonException("Unexpected character while parsing path indexer: " + currentCharacter);
                    }

                    return new ArrayIndexFilter();
                }
                else if (currentCharacter == ':')
                {
                    int length = (end ?? _currentIndex) - start;

                    if (length > 0)
                    {
                        var indexer = _expression.AsSpan(start, length);
                        int index = int.Parse(indexer, CultureInfo.InvariantCulture);

                        if (colonCount == 0)
                        {
                            startIndex = index;
                        }
                        else if (colonCount == 1)
                        {
                            endIndex = index;
                        }
                        else
                        {
                            step = index;
                        }
                    }

                    colonCount++;

                    _currentIndex++;

                    EatWhitespace();

                    start = _currentIndex;
                    end = null;
                }
                else if (!char.IsDigit(currentCharacter) && currentCharacter != '-')
                {
                    throw new JsonException("Unexpected character while parsing path indexer: " + currentCharacter);
                }
                else
                {
                    if (end != null)
                    {
                        throw new JsonException("Unexpected character while parsing path indexer: " + currentCharacter);
                    }

                    _currentIndex++;
                }
            }

            throw new JsonException("Path ended with open indexer.");
        }

        /// <summary>
        /// Advances the current index past any whitespace characters.
        /// </summary>
        private void EatWhitespace()
        {
            while (_currentIndex < _expression.Length)
            {
                if (_expression[_currentIndex] != ' ')
                {
                    break;
                }

                _currentIndex++;
            }
        }

        /// <summary>
        /// Parses a query expression within an indexer.
        /// </summary>
        /// <param name="indexerCloseChar">The closing character of the indexer.</param>
        /// <param name="scan">Indicates if this is a scan operation.</param>
        /// <returns>A QueryFilter or QueryScanFilter based on the parsed expression.</returns>
        private PathFilter ParseQuery(char indexerCloseChar, bool scan)
        {
            _currentIndex++;
            EnsureLength("Path ended with open indexer.");

            if (_expression[_currentIndex] != '(')
            {
                throw new JsonException("Unexpected character while parsing path indexer: " + _expression[_currentIndex]);
            }

            _currentIndex++;

            QueryExpression expression = ParseExpression();

            _currentIndex++;
            EnsureLength("Path ended with open indexer.");
            EatWhitespace();

            if (_expression[_currentIndex] != indexerCloseChar)
            {
                throw new JsonException("Unexpected character while parsing path indexer: " + _expression[_currentIndex]);
            }

            if (!scan)
            {
                return new QueryFilter(expression);
            }
            else
            {
                return new QueryScanFilter(expression);
            }
        }

        /// <summary>
        /// Attempts to parse an expression starting with '$' or '@'.
        /// </summary>
        /// <param name="expressionPath">When successful, contains the list of filters for the expression.</param>
        /// <returns>True if successfully parsed an expression; otherwise, false.</returns>
        private bool TryParseExpression(out List<PathFilter>? expressionPath)
        {
            if (_expression[_currentIndex] == '$')
            {
                expressionPath = new List<PathFilter> { RootFilter.Instance };
            }
            else if (_expression[_currentIndex] == '@')
            {
                expressionPath = new List<PathFilter>();
            }
            else
            {
                expressionPath = null;
                return false;
            }

            _currentIndex++;

            if (ParsePath(expressionPath!, _currentIndex, true))
            {
                throw new JsonException("Path ended with open query.");
            }

            return true;
        }

        /// <summary>
        /// Creates a JsonException for unexpected characters encountered during parsing.
        /// </summary>
        /// <returns>A new JsonException with details about the unexpected character.</returns>
        private JsonException CreateUnexpectedCharacterException()
        {
            return new JsonException("Unexpected character while parsing path query: " + _expression[_currentIndex]);
        }

        /// <summary>
        /// Parses one side of a query expression.
        /// </summary>
        /// <returns>An object representing either a path filter list or a value.</returns>
        private object? ParseSide()
        {
            EatWhitespace();

            if (TryParseExpression(out List<PathFilter>? expressionPath))
            {
                EatWhitespace();
                EnsureLength("Path ended with open query.");

                return expressionPath;
            }

            if (TryParseValue(out var value))
            {
                EatWhitespace();
                EnsureLength("Path ended with open query.");

                return value;
            }

            throw CreateUnexpectedCharacterException();
        }

        /// <summary>
        /// Parses a complete query expression, including boolean operations.
        /// </summary>
        /// <returns>A QueryExpression representing the parsed expression.</returns>
        private QueryExpression ParseExpression()
        {
            QueryExpression? rootExpression = null;
            CompositeExpression? parentExpression = null;

            while (_currentIndex < _expression.Length)
            {
                object? left = ParseSide();
                object? right = null;

                QueryOperator op;
                if (_expression[_currentIndex] == ')'
                    || _expression[_currentIndex] == '|'
                    || _expression[_currentIndex] == '&')
                {
                    op = QueryOperator.Exists;
                }
                else
                {
                    op = ParseOperator();

                    right = ParseSide();
                }

                BooleanQueryExpression booleanExpression = new BooleanQueryExpression(op, left, right);

                if (_expression[_currentIndex] == ')')
                {
                    if (parentExpression != null)
                    {
                        parentExpression.Expressions.Add(booleanExpression);
                        return rootExpression!;
                    }

                    return booleanExpression;
                }
                if (_expression[_currentIndex] == '&')
                {
                    if (!Match("&&"))
                    {
                        throw CreateUnexpectedCharacterException();
                    }

                    if (parentExpression == null || parentExpression.Operator != QueryOperator.And)
                    {
                        CompositeExpression andExpression = new CompositeExpression(QueryOperator.And);

                        parentExpression?.Expressions.Add(andExpression);

                        parentExpression = andExpression;

                        if (rootExpression == null)
                        {
                            rootExpression = parentExpression;
                        }
                    }

                    parentExpression.Expressions.Add(booleanExpression);
                }
                if (_expression[_currentIndex] == '|')
                {
                    if (!Match("||"))
                    {
                        throw CreateUnexpectedCharacterException();
                    }

                    if (parentExpression == null || parentExpression.Operator != QueryOperator.Or)
                    {
                        CompositeExpression orExpression = new CompositeExpression(QueryOperator.Or);

                        parentExpression?.Expressions.Add(orExpression);

                        parentExpression = orExpression;

                        if (rootExpression == null)
                        {
                            rootExpression = parentExpression;
                        }
                    }

                    parentExpression.Expressions.Add(booleanExpression);
                }
            }

            throw new JsonException("Path ended with open query.");
        }

        /// <summary>
        /// Attempts to parse a JSON value (string, number, boolean, or null).
        /// </summary>
        /// <param name="value">When successful, contains the parsed JsonValue.</param>
        /// <returns>True if successfully parsed a value; otherwise, false.</returns>
        private bool TryParseValue(out JsonValue? value)
        {
            char currentChar = _expression[_currentIndex];
            if (currentChar == '\'')
            {
                value = JsonValue.Create(ReadQuotedString());
                return true;
            }
            else if (char.IsDigit(currentChar) || currentChar == '-')
            {
                var start = _currentIndex;
                _currentIndex++;
                while (_currentIndex < _expression.Length)
                {
                    currentChar = _expression[_currentIndex];
                    if (currentChar == ' ' || currentChar == ')')
                    {
                        var numberText = _expression.AsSpan(start, _currentIndex - start);

                        if (numberText.IndexOfAny(FloatCharacters) != -1)
                        {
                            bool result = double.TryParse(numberText, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var d);
                            value = JsonValue.Create(d);
                            return result;
                        }
                        else
                        {
                            bool result = long.TryParse(numberText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l);
                            value = JsonValue.Create(l);
                            return result;
                        }
                    }
                    else
                    {
                        _currentIndex++;
                    }
                }
            }
            else if (currentChar == 't')
            {
                if (Match("true"))
                {
                    value = TrueJsonValue;
                    return true;
                }
            }
            else if (currentChar == 'f')
            {
                if (Match("false"))
                {
                    value = FalseJsonValue;
                    return true;
                }
            }
            else if (currentChar == 'n')
            {
                if (Match("null"))
                {
                    value = null;
                    return true;
                }
            }
            else if (currentChar == '/')
            {
                value = JsonValue.Create(ReadRegexString());
                return true;
            }

            value = null;
            return false;
        }

        /// <summary>
        /// Reads a quoted string value, handling escape sequences.
        /// </summary>
        /// <returns>The parsed string value.</returns>
        private string ReadQuotedString()
        {
            StringBuilder sb = new StringBuilder();

            _currentIndex++;
            while (_currentIndex < _expression.Length)
            {
                char currentChar = _expression[_currentIndex];
                if (currentChar == '\\' && _currentIndex + 1 < _expression.Length)
                {
                    _currentIndex++;
                    currentChar = _expression[_currentIndex];

                    char resolvedChar;
                    switch (currentChar)
                    {
                        case 'b':
                            resolvedChar = '\b';
                            break;
                        case 't':
                            resolvedChar = '\t';
                            break;
                        case 'n':
                            resolvedChar = '\n';
                            break;
                        case 'f':
                            resolvedChar = '\f';
                            break;
                        case 'r':
                            resolvedChar = '\r';
                            break;
                        case '\\':
                        case '"':
                        case '\'':
                        case '/':
                            resolvedChar = currentChar;
                            break;
                        default:
                            throw new JsonException(@"Unknown escape character: \" + currentChar);
                    }

                    sb.Append(resolvedChar);

                    _currentIndex++;
                }
                else if (currentChar == '\'')
                {
                    _currentIndex++;
                    return sb.ToString();
                }
                else
                {
                    _currentIndex++;
                    sb.Append(currentChar);
                }
            }

            throw new JsonException("Path ended with an open string.");
        }

        /// <summary>
        /// Reads a regular expression string, including any flags.
        /// </summary>
        /// <returns>The complete regular expression string including delimiters and flags.</returns>
        private string ReadRegexString()
        {
            int startIndex = _currentIndex;

            _currentIndex++;
            while (_currentIndex < _expression.Length)
            {
                char currentChar = _expression[_currentIndex];

                // handle escaped / character
                if (currentChar == '\\' && _currentIndex + 1 < _expression.Length)
                {
                    _currentIndex += 2;
                }
                else if (currentChar == '/')
                {
                    _currentIndex++;

                    while (_currentIndex < _expression.Length)
                    {
                        currentChar = _expression[_currentIndex];

                        if (char.IsLetter(currentChar))
                        {
                            _currentIndex++;
                        }
                        else
                        {
                            break;
                        }
                    }

                    return _expression.Substring(startIndex, _currentIndex - startIndex);
                }
                else
                {
                    _currentIndex++;
                }
            }

            throw new JsonException("Path ended with an open regex.");
        }

        /// <summary>
        /// Attempts to match a specific string at the current position.
        /// </summary>
        /// <param name="s">The string to match.</param>
        /// <returns>True if the string matches at the current position; otherwise, false.</returns>
        private bool Match(string s)
        {
            int currentPosition = _currentIndex;
            for (int i = 0; i < s.Length; i++)
            {
                if (currentPosition < _expression.Length && _expression[currentPosition] == s[i])
                {
                    currentPosition++;
                }
                else
                {
                    return false;
                }
            }

            _currentIndex = currentPosition;
            return true;
        }

        /// <summary>
        /// Parses a comparison operator in a query expression.
        /// </summary>
        /// <returns>The QueryOperator enum value representing the parsed operator.</returns>
        private QueryOperator ParseOperator()
        {
            if (_currentIndex + 1 >= _expression.Length)
            {
                throw new JsonException("Path ended with open query.");
            }

            if (Match("==="))
            {
                return QueryOperator.StrictEquals;
            }

            if (Match("=="))
            {
                return QueryOperator.Equals;
            }

            if (Match("=~"))
            {
                return QueryOperator.RegexEquals;
            }

            if (Match("!=="))
            {
                return QueryOperator.StrictNotEquals;
            }

            if (Match("!=") || Match("<>"))
            {
                return QueryOperator.NotEquals;
            }
            if (Match("<="))
            {
                return QueryOperator.LessThanOrEquals;
            }
            if (Match("<"))
            {
                return QueryOperator.LessThan;
            }
            if (Match(">="))
            {
                return QueryOperator.GreaterThanOrEquals;
            }
            if (Match(">"))
            {
                return QueryOperator.GreaterThan;
            }

            throw new JsonException("Could not read query operator.");
        }

        /// <summary>
        /// Parses a quoted field name in an indexer, supporting single or multiple field names.
        /// </summary>
        /// <param name="indexerCloseChar">The closing character of the indexer.</param>
        /// <param name="scan">Indicates if this is a scan operation.</param>
        /// <returns>A PathFilter representing the field(s) access.</returns>
        private PathFilter ParseQuotedField(char indexerCloseChar, bool scan)
        {
            List<string>? fields = null;

            while (_currentIndex < _expression.Length)
            {
                string field = ReadQuotedString();

                EatWhitespace();
                EnsureLength("Path ended with open indexer.");

                if (_expression[_currentIndex] == indexerCloseChar)
                {
                    if (fields != null)
                    {
                        fields.Add(field);
                        return (scan)
                            ? (PathFilter)new ScanMultipleFilter(fields)
                            : (PathFilter)new FieldMultipleFilter(fields);
                    }
                    else
                    {
                        return CreatePathFilter(field, scan);
                    }
                }
                else if (_expression[_currentIndex] == ',')
                {
                    _currentIndex++;
                    EatWhitespace();

                    if (fields == null)
                    {
                        fields = new List<string>();
                    }

                    fields.Add(field);
                }
                else
                {
                    throw new JsonException("Unexpected character while parsing path indexer: " + _expression[_currentIndex]);
                }
            }

            throw new JsonException("Path ended with open indexer.");
        }

        /// <summary>
        /// Ensures that there are remaining characters to parse in the expression.
        /// </summary>
        /// <param name="message">The error message to use if the check fails.</param>
        /// <exception cref="JsonException">Thrown when there are no remaining characters to parse.</exception>
        private void EnsureLength(string message)
        {
            if (_currentIndex >= _expression.Length)
            {
                throw new JsonException(message);
            }
        }
    }
}