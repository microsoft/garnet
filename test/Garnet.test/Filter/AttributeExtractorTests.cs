// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Allure.NUnit;
using Garnet.server.Vector.Filter;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests for <see cref="AttributeExtractor"/> — the raw-byte JSON field extractor
    /// used by the filter expression VM to resolve selectors on demand.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class AttributeExtractorTests : AllureTestBase
    {
        private static ExprToken Extract(string json, string field)
            => AttributeExtractor.ExtractField(Encoding.UTF8.GetBytes(json), field);

        /// <summary>
        /// Get the string value from an ExprToken, handling both allocated strings and JSON byte refs.
        /// </summary>
        private static string GetStr(string json, ExprToken token)
        {
            if (token.Str != null) return token.Str;
            if (token.IsJsonRef)
            {
                var bytes = Encoding.UTF8.GetBytes(json);
                return Encoding.UTF8.GetString(bytes, token.Utf8Start, token.Utf8Length);
            }
            return null;
        }

        // ======================== Number extraction ========================

        [Test]
        public void ExtractField_Integer()
        {
            var token = Extract("{\"year\":1980}", "year");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1980.0, token.Num);
        }

        [Test]
        public void ExtractField_NegativeInteger()
        {
            var token = Extract("{\"temp\":-42}", "temp");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(-42.0, token.Num);
        }

        [Test]
        public void ExtractField_Decimal()
        {
            var token = Extract("{\"rating\":4.5}", "rating");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(4.5, token.Num, 0.001);
        }

        [Test]
        public void ExtractField_ScientificNotation()
        {
            var token = Extract("{\"val\":1.5e3}", "val");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1500.0, token.Num);
        }

        [Test]
        public void ExtractField_Zero()
        {
            var token = Extract("{\"val\":0}", "val");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(0.0, token.Num);
        }

        // ======================== String extraction ========================

        [Test]
        public void ExtractField_SimpleString()
        {
            var json = "{\"genre\":\"action\"}";
            var token = Extract(json, "genre");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsTrue(token.IsJsonRef, "Non-escaped strings should be JSON byte refs");
            ClassicAssert.AreEqual("action", GetStr(json, token));
        }

        [Test]
        public void ExtractField_EmptyString()
        {
            var json = "{\"name\":\"\"}";
            var token = Extract(json, "name");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsTrue(token.IsJsonRef);
            ClassicAssert.AreEqual("", GetStr(json, token));
        }

        [Test]
        public void ExtractField_StringWithEscapedQuote()
        {
            var token = Extract("{\"name\":\"hello\\\"world\"}", "name");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsFalse(token.IsJsonRef, "Escaped strings should be materialized");
            ClassicAssert.AreEqual("hello\"world", token.Str);
        }

        [Test]
        public void ExtractField_StringWithEscapedBackslash()
        {
            var token = Extract("{\"path\":\"c:\\\\temp\"}", "path");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsFalse(token.IsJsonRef);
            ClassicAssert.AreEqual("c:\\temp", token.Str);
        }

        [Test]
        public void ExtractField_StringWithEscapedNewline()
        {
            var token = Extract("{\"text\":\"line1\\nline2\"}", "text");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsFalse(token.IsJsonRef);
            ClassicAssert.AreEqual("line1\nline2", token.Str);
        }

        [Test]
        public void ExtractField_StringWithEscapedTab()
        {
            var token = Extract("{\"text\":\"col1\\tcol2\"}", "text");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsFalse(token.IsJsonRef);
            ClassicAssert.AreEqual("col1\tcol2", token.Str);
        }

        [Test]
        public void ExtractField_StringWithSlashEscape()
        {
            var token = Extract("{\"url\":\"http:\\/\\/example.com\"}", "url");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsFalse(token.IsJsonRef);
            ClassicAssert.AreEqual("http://example.com", token.Str);
        }

        // ======================== Boolean extraction ========================

        [Test]
        public void ExtractField_True()
        {
            var token = Extract("{\"active\":true}", "active");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1.0, token.Num);
        }

        [Test]
        public void ExtractField_False()
        {
            var token = Extract("{\"deleted\":false}", "deleted");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(0.0, token.Num);
        }

        // ======================== Null extraction ========================

        [Test]
        public void ExtractField_Null()
        {
            var token = Extract("{\"value\":null}", "value");
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TokenType);
        }

        // ======================== Array extraction ========================

        [Test]
        public void ExtractField_StringArray()
        {
            var json = "{\"tags\":[\"classic\",\"popular\"]}";
            var token = Extract(json, "tags");
            ClassicAssert.AreEqual(ExprTokenType.Tuple, token.TokenType);
            ClassicAssert.AreEqual(2, token.TupleLength);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TupleElements[0].TokenType);
            ClassicAssert.AreEqual("classic", GetStr(json, token.TupleElements[0]));
            ClassicAssert.AreEqual("popular", GetStr(json, token.TupleElements[1]));
        }

        [Test]
        public void ExtractField_NumericArray()
        {
            var token = Extract("{\"scores\":[1,2,3]}", "scores");
            ClassicAssert.AreEqual(ExprTokenType.Tuple, token.TokenType);
            ClassicAssert.AreEqual(3, token.TupleLength);
            ClassicAssert.AreEqual(1.0, token.TupleElements[0].Num);
            ClassicAssert.AreEqual(2.0, token.TupleElements[1].Num);
            ClassicAssert.AreEqual(3.0, token.TupleElements[2].Num);
        }

        [Test]
        public void ExtractField_MixedArray()
        {
            var json = "{\"data\":[1,\"two\",true,null]}";
            var token = Extract(json, "data");
            ClassicAssert.AreEqual(ExprTokenType.Tuple, token.TokenType);
            ClassicAssert.AreEqual(4, token.TupleLength);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TupleElements[0].TokenType);
            ClassicAssert.AreEqual(1.0, token.TupleElements[0].Num);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TupleElements[1].TokenType);
            ClassicAssert.AreEqual("two", GetStr(json, token.TupleElements[1]));
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TupleElements[2].TokenType);
            ClassicAssert.AreEqual(1.0, token.TupleElements[2].Num); // true → 1
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TupleElements[3].TokenType);
        }

        [Test]
        public void ExtractField_EmptyArray()
        {
            var token = Extract("{\"items\":[]}", "items");
            ClassicAssert.AreEqual(ExprTokenType.Tuple, token.TokenType);
            ClassicAssert.AreEqual(0, token.TupleLength);
        }

        // ======================== Multiple fields ========================

        [Test]
        public void ExtractField_FirstField()
        {
            var token = Extract("{\"a\":1,\"b\":2,\"c\":3}", "a");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1.0, token.Num);
        }

        [Test]
        public void ExtractField_MiddleField()
        {
            var token = Extract("{\"a\":1,\"b\":2,\"c\":3}", "b");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(2.0, token.Num);
        }

        [Test]
        public void ExtractField_LastField()
        {
            var token = Extract("{\"a\":1,\"b\":2,\"c\":3}", "c");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(3.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsValuesOfDifferentTypes()
        {
            // Ensure the extractor correctly skips strings, arrays, objects, booleans, nulls, and numbers
            // when seeking a later field
            var json = "{\"s\":\"hello\",\"a\":[1,2],\"o\":{\"nested\":true},\"b\":false,\"n\":null,\"target\":42}";
            var token = Extract(json, "target");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(42.0, token.Num);
        }

        // ======================== Missing / not found ========================

        [Test]
        public void ExtractField_MissingField_ReturnsNone()
        {
            var token = Extract("{\"year\":1980}", "rating");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_EmptyObject_ReturnsNone()
        {
            var token = Extract("{}", "anything");
            ClassicAssert.IsTrue(token.IsNone);
        }

        // ======================== Whitespace handling ========================

        [Test]
        public void ExtractField_WithWhitespace()
        {
            var token = Extract("  {  \"year\"  :  1980  }  ", "year");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1980.0, token.Num);
        }

        [Test]
        public void ExtractField_WithNewlines()
        {
            var json = "{\n  \"year\": 1980,\n  \"rating\": 4.5\n}";
            var token = Extract(json, "rating");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(4.5, token.Num, 0.001);
        }

        // ======================== Nested objects (skipped) ========================

        [Test]
        public void ExtractField_NestedObject_ReturnsNone()
        {
            // Nested objects are not supported as values — should return IsNone
            var token = Extract("{\"meta\":{\"key\":\"val\"}}", "meta");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_SkipsNestedObjectToFindLaterField()
        {
            var json = "{\"meta\":{\"key\":\"val\"},\"year\":2020}";
            var token = Extract(json, "year");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(2020.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsDeeplyNestedObject()
        {
            var json = "{\"deep\":{\"a\":{\"b\":{\"c\":1}}},\"target\":99}";
            var token = Extract(json, "target");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(99.0, token.Num);
        }

        // ======================== Malformed / non-JSON input ========================

        [Test]
        public void ExtractField_NotJson_ReturnsNone()
        {
            var token = Extract("this is not json", "year");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_EmptyInput_ReturnsNone()
        {
            var token = AttributeExtractor.ExtractField([], "year");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_ArrayAtRoot_ReturnsNone()
        {
            var token = Extract("[1,2,3]", "year");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_TruncatedJson_ReturnsNone()
        {
            var token = Extract("{\"year\":", "year");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_MissingColon_ReturnsNone()
        {
            var token = Extract("{\"year\" 1980}", "year");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_UnterminatedString_ReturnsNone()
        {
            var token = Extract("{\"name\":\"hello}", "name");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_UnterminatedKey_ReturnsNone()
        {
            var token = Extract("{\"name:\"hello\"}", "name");
            // The key "name will match to :, parsing should fail gracefully
            ClassicAssert.IsTrue(token.IsNone);
        }

        // ======================== Edge cases ========================

        [Test]
        public void ExtractField_StringValueContainingBraces()
        {
            var json = "{\"data\":\"{not an object}\"}";
            var token = Extract(json, "data");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.AreEqual("{not an object}", GetStr(json, token));
        }

        [Test]
        public void ExtractField_StringValueContainingBrackets()
        {
            var json = "{\"data\":\"[not an array]\"}";
            var token = Extract(json, "data");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.AreEqual("[not an array]", GetStr(json, token));
        }

        [Test]
        public void ExtractField_StringValueContainingComma()
        {
            var json = "{\"msg\":\"hello, world\"}";
            var token = Extract(json, "msg");
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.AreEqual("hello, world", GetStr(json, token));
        }

        [Test]
        public void ExtractField_FieldNameCaseSensitive()
        {
            var token = Extract("{\"Year\":2020}", "year");
            ClassicAssert.IsTrue(token.IsNone); // Case mismatch

            var token2 = Extract("{\"Year\":2020}", "Year");
            ClassicAssert.AreEqual(ExprTokenType.Num, token2.TokenType);
            ClassicAssert.AreEqual(2020.0, token2.Num);
        }

        [Test]
        public void ExtractField_FieldWithHyphen()
        {
            // Hyphens in JSON keys are valid
            var token = Extract("{\"my-field\":42}", "my-field");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(42.0, token.Num);
        }

        [Test]
        public void ExtractField_FieldWithUnderscore()
        {
            var token = Extract("{\"my_field\":42}", "my_field");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(42.0, token.Num);
        }

        [Test]
        public void ExtractField_FieldWithDigits()
        {
            var token = Extract("{\"field123\":99}", "field123");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(99.0, token.Num);
        }

        [Test]
        public void ExtractField_BooleanLiteralNotFollowedByDelimiter_ReturnsNone()
        {
            // "trueish" should not match as true
            var token = Extract("{\"val\":trueish}", "val");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_NullLiteralNotFollowedByDelimiter_ReturnsNone()
        {
            var token = Extract("{\"val\":nullify}", "val");
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_ArrayWithNestedArrays()
        {
            // ParseArrayToken calls ParseValueToken which handles nested arrays recursively
            var token = Extract("{\"matrix\":[[1,2],[3,4]]}", "matrix");
            ClassicAssert.AreEqual(ExprTokenType.Tuple, token.TokenType);
            ClassicAssert.AreEqual(2, token.TupleLength);
            // Each inner element is itself a Tuple
            ClassicAssert.AreEqual(ExprTokenType.Tuple, token.TupleElements[0].TokenType);
            ClassicAssert.AreEqual(2, token.TupleElements[0].TupleLength);
        }

        [Test]
        public void ExtractField_LargeNumberOfFields()
        {
            // Ensure we can skip many fields to find the target
            var sb = new StringBuilder("{");
            for (var i = 0; i < 100; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append($"\"field{i}\":{i}");
            }
            sb.Append('}');

            var token = Extract(sb.ToString(), "field99");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(99.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsArrayWithStringsContainingQuotes()
        {
            // Ensure the array skipper handles escaped quotes inside string elements
            var json = "{\"arr\":[\"he\\\"llo\",\"world\"],\"target\":1}";
            var token = Extract(json, "target");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsStringWithEscapedBackslashBeforeClosingQuote()
        {
            // The value is the string: ends_with_backslash\  (the JSON encodes \\ at the end)
            // This tests that \\\" is parsed as \\ + " (close quote), not \ + \"
            var json = "{\"a\":\"ends_with_backslash\\\\\",\"b\":2}";
            var token = Extract(json, "b");
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(2.0, token.Num);
        }
    }
}