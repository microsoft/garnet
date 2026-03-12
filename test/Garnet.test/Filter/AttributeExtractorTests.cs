// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// <summary>
        /// Extract a field from JSON using the new byte-span API.
        /// </summary>
        private static ExprToken Extract(byte[] jsonBytes, ReadOnlySpan<byte> field)
            => AttributeExtractor.ExtractField(jsonBytes, field);

        /// <summary>
        /// Get the string value from an ExprToken that references into json bytes.
        /// </summary>
        private static string GetStr(byte[] jsonBytes, ExprToken token)
        {
            if (token.TokenType != ExprTokenType.Str) return null;
            return Encoding.UTF8.GetString(jsonBytes, token.Utf8Start, token.Utf8Length);
        }

        // ======================== Number extraction ========================

        [Test]
        public void ExtractField_Integer()
        {
            var json = Encoding.UTF8.GetBytes("{\"year\":1980}");
            var token = Extract(json, "year"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1980.0, token.Num);
        }

        [Test]
        public void ExtractField_NegativeInteger()
        {
            var json = Encoding.UTF8.GetBytes("{\"temp\":-42}");
            var token = Extract(json, "temp"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(-42.0, token.Num);
        }

        [Test]
        public void ExtractField_Decimal()
        {
            var json = Encoding.UTF8.GetBytes("{\"rating\":4.5}");
            var token = Extract(json, "rating"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(4.5, token.Num, 0.001);
        }

        [Test]
        public void ExtractField_ScientificNotation()
        {
            var json = Encoding.UTF8.GetBytes("{\"val\":1.5e3}");
            var token = Extract(json, "val"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1500.0, token.Num);
        }

        [Test]
        public void ExtractField_Zero()
        {
            var json = Encoding.UTF8.GetBytes("{\"val\":0}");
            var token = Extract(json, "val"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(0.0, token.Num);
        }

        // ======================== String extraction ========================

        [Test]
        public void ExtractField_SimpleString()
        {
            var json = Encoding.UTF8.GetBytes("{\"genre\":\"action\"}");
            var token = Extract(json, "genre"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsFalse(token.HasEscape, "Non-escaped strings should not have escape flag set");
            ClassicAssert.AreEqual("action", GetStr(json, token));
        }

        [Test]
        public void ExtractField_EmptyString()
        {
            var json = Encoding.UTF8.GetBytes("{\"name\":\"\"}");
            var token = Extract(json, "name"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.AreEqual("", GetStr(json, token));
        }

        [Test]
        public void ExtractField_StringWithEscapedQuote()
        {
            var json = Encoding.UTF8.GetBytes("{\"name\":\"hello\\\"world\"}");
            var token = Extract(json, "name"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsTrue(token.HasEscape, "Escaped strings should have escape flag set");
            // The raw bytes contain the escape sequences; the test verifies we get the right byte range
            var raw = GetStr(json, token);
            ClassicAssert.IsTrue(raw.Contains("hello") && raw.Contains("world"));
        }

        [Test]
        public void ExtractField_StringWithEscapedBackslash()
        {
            var json = Encoding.UTF8.GetBytes("{\"path\":\"c:\\\\temp\"}");
            var token = Extract(json, "path"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsTrue(token.HasEscape);
            // Raw bytes include the escape sequences
            var raw = GetStr(json, token);
            ClassicAssert.IsTrue(raw.Contains("c:") && raw.Contains("temp"));
        }

        [Test]
        public void ExtractField_StringWithEscapedNewline()
        {
            var json = Encoding.UTF8.GetBytes("{\"text\":\"line1\\nline2\"}");
            var token = Extract(json, "text"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsTrue(token.HasEscape);
            var raw = GetStr(json, token);
            ClassicAssert.IsTrue(raw.Contains("line1") && raw.Contains("line2"));
        }

        [Test]
        public void ExtractField_StringWithEscapedTab()
        {
            var json = Encoding.UTF8.GetBytes("{\"text\":\"col1\\tcol2\"}");
            var token = Extract(json, "text"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsTrue(token.HasEscape);
            var raw = GetStr(json, token);
            ClassicAssert.IsTrue(raw.Contains("col1") && raw.Contains("col2"));
        }

        [Test]
        public void ExtractField_StringWithSlashEscape()
        {
            var json = Encoding.UTF8.GetBytes("{\"url\":\"http:\\/\\/example.com\"}");
            var token = Extract(json, "url"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.IsTrue(token.HasEscape);
            var raw = GetStr(json, token);
            ClassicAssert.IsTrue(raw.Contains("http") && raw.Contains("example.com"));
        }

        // ======================== Boolean extraction ========================

        [Test]
        public void ExtractField_True()
        {
            var json = Encoding.UTF8.GetBytes("{\"active\":true}");
            var token = Extract(json, "active"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1.0, token.Num);
        }

        [Test]
        public void ExtractField_False()
        {
            var json = Encoding.UTF8.GetBytes("{\"deleted\":false}");
            var token = Extract(json, "deleted"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(0.0, token.Num);
        }

        // ======================== Null extraction ========================

        [Test]
        public void ExtractField_Null()
        {
            var json = Encoding.UTF8.GetBytes("{\"value\":null}");
            var token = Extract(json, "value"u8);
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TokenType);
        }

        // ======================== Array extraction ========================
        // NOTE: In the current refactored API, AttributeExtractor.ParseArrayToken returns Null
        // for JSON arrays (array extraction via tuple pool is not yet re-added).
        // These tests verify the current behavior: arrays are treated as Null.

        [Test]
        public void ExtractField_StringArray_ReturnsNull()
        {
            var json = Encoding.UTF8.GetBytes("{\"tags\":[\"classic\",\"popular\"]}");
            var token = Extract(json, "tags"u8);
            // Arrays are currently returned as Null by the extractor
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TokenType);
        }

        [Test]
        public void ExtractField_NumericArray_ReturnsNull()
        {
            var json = Encoding.UTF8.GetBytes("{\"scores\":[1,2,3]}");
            var token = Extract(json, "scores"u8);
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TokenType);
        }

        [Test]
        public void ExtractField_MixedArray_ReturnsNull()
        {
            var json = Encoding.UTF8.GetBytes("{\"data\":[1,\"two\",true,null]}");
            var token = Extract(json, "data"u8);
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TokenType);
        }

        [Test]
        public void ExtractField_EmptyArray_ReturnsNull()
        {
            var json = Encoding.UTF8.GetBytes("{\"items\":[]}");
            var token = Extract(json, "items"u8);
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TokenType);
        }

        // ======================== Multiple fields ========================

        [Test]
        public void ExtractField_FirstField()
        {
            var json = Encoding.UTF8.GetBytes("{\"a\":1,\"b\":2,\"c\":3}");
            var token = Extract(json, "a"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1.0, token.Num);
        }

        [Test]
        public void ExtractField_MiddleField()
        {
            var json = Encoding.UTF8.GetBytes("{\"a\":1,\"b\":2,\"c\":3}");
            var token = Extract(json, "b"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(2.0, token.Num);
        }

        [Test]
        public void ExtractField_LastField()
        {
            var json = Encoding.UTF8.GetBytes("{\"a\":1,\"b\":2,\"c\":3}");
            var token = Extract(json, "c"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(3.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsValuesOfDifferentTypes()
        {
            // Ensure the extractor correctly skips strings, arrays, objects, booleans, nulls, and numbers
            // when seeking a later field
            var json = Encoding.UTF8.GetBytes("{\"s\":\"hello\",\"a\":[1,2],\"o\":{\"nested\":true},\"b\":false,\"n\":null,\"target\":42}");
            var token = Extract(json, "target"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(42.0, token.Num);
        }

        // ======================== Missing / not found ========================

        [Test]
        public void ExtractField_MissingField_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("{\"year\":1980}");
            var token = Extract(json, "rating"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_EmptyObject_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("{}");
            var token = Extract(json, "anything"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        // ======================== Whitespace handling ========================

        [Test]
        public void ExtractField_WithWhitespace()
        {
            var json = Encoding.UTF8.GetBytes("  {  \"year\"  :  1980  }  ");
            var token = Extract(json, "year"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1980.0, token.Num);
        }

        [Test]
        public void ExtractField_WithNewlines()
        {
            var json = Encoding.UTF8.GetBytes("{\n  \"year\": 1980,\n  \"rating\": 4.5\n}");
            var token = Extract(json, "rating"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(4.5, token.Num, 0.001);
        }

        // ======================== Nested objects (skipped) ========================

        [Test]
        public void ExtractField_NestedObject_ReturnsNone()
        {
            // Nested objects are not supported as values — should return IsNone
            var json = Encoding.UTF8.GetBytes("{\"meta\":{\"key\":\"val\"}}");
            var token = Extract(json, "meta"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_SkipsNestedObjectToFindLaterField()
        {
            var json = Encoding.UTF8.GetBytes("{\"meta\":{\"key\":\"val\"},\"year\":2020}");
            var token = Extract(json, "year"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(2020.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsDeeplyNestedObject()
        {
            var json = Encoding.UTF8.GetBytes("{\"deep\":{\"a\":{\"b\":{\"c\":1}}},\"target\":99}");
            var token = Extract(json, "target"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(99.0, token.Num);
        }

        // ======================== Malformed / non-JSON input ========================

        [Test]
        public void ExtractField_NotJson_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("this is not json");
            var token = Extract(json, "year"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_EmptyInput_ReturnsNone()
        {
            var token = AttributeExtractor.ExtractField([], "year"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_ArrayAtRoot_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("[1,2,3]");
            var token = Extract(json, "year"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_TruncatedJson_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("{\"year\":");
            var token = Extract(json, "year"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_MissingColon_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("{\"year\" 1980}");
            var token = Extract(json, "year"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_UnterminatedString_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("{\"name\":\"hello}");
            var token = Extract(json, "name"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_UnterminatedKey_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("{\"name:\"hello\"}");
            var token = Extract(json, "name"u8);
            // The key "name will match to :, parsing should fail gracefully
            ClassicAssert.IsTrue(token.IsNone);
        }

        // ======================== Edge cases ========================

        [Test]
        public void ExtractField_StringValueContainingBraces()
        {
            var json = Encoding.UTF8.GetBytes("{\"data\":\"{not an object}\"}");
            var token = Extract(json, "data"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.AreEqual("{not an object}", GetStr(json, token));
        }

        [Test]
        public void ExtractField_StringValueContainingBrackets()
        {
            var json = Encoding.UTF8.GetBytes("{\"data\":\"[not an array]\"}");
            var token = Extract(json, "data"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.AreEqual("[not an array]", GetStr(json, token));
        }

        [Test]
        public void ExtractField_StringValueContainingComma()
        {
            var json = Encoding.UTF8.GetBytes("{\"msg\":\"hello, world\"}");
            var token = Extract(json, "msg"u8);
            ClassicAssert.AreEqual(ExprTokenType.Str, token.TokenType);
            ClassicAssert.AreEqual("hello, world", GetStr(json, token));
        }

        [Test]
        public void ExtractField_FieldNameCaseSensitive()
        {
            var json = Encoding.UTF8.GetBytes("{\"Year\":2020}");
            var token = Extract(json, "year"u8);
            ClassicAssert.IsTrue(token.IsNone); // Case mismatch

            var token2 = Extract(json, "Year"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token2.TokenType);
            ClassicAssert.AreEqual(2020.0, token2.Num);
        }

        [Test]
        public void ExtractField_FieldWithHyphen()
        {
            // Hyphens in JSON keys are valid
            var json = Encoding.UTF8.GetBytes("{\"my-field\":42}");
            var token = Extract(json, "my-field"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(42.0, token.Num);
        }

        [Test]
        public void ExtractField_FieldWithUnderscore()
        {
            var json = Encoding.UTF8.GetBytes("{\"my_field\":42}");
            var token = Extract(json, "my_field"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(42.0, token.Num);
        }

        [Test]
        public void ExtractField_FieldWithDigits()
        {
            var json = Encoding.UTF8.GetBytes("{\"field123\":99}");
            var token = Extract(json, "field123"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(99.0, token.Num);
        }

        [Test]
        public void ExtractField_BooleanLiteralNotFollowedByDelimiter_ReturnsNone()
        {
            // "trueish" should not match as true
            var json = Encoding.UTF8.GetBytes("{\"val\":trueish}");
            var token = Extract(json, "val"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_NullLiteralNotFollowedByDelimiter_ReturnsNone()
        {
            var json = Encoding.UTF8.GetBytes("{\"val\":nullify}");
            var token = Extract(json, "val"u8);
            ClassicAssert.IsTrue(token.IsNone);
        }

        [Test]
        public void ExtractField_ArrayWithNestedArrays_ReturnsNull()
        {
            // Arrays are currently returned as Null by the extractor
            var json = Encoding.UTF8.GetBytes("{\"matrix\":[[1,2],[3,4]]}");
            var token = Extract(json, "matrix"u8);
            ClassicAssert.AreEqual(ExprTokenType.Null, token.TokenType);
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

            var json = Encoding.UTF8.GetBytes(sb.ToString());
            var token = Extract(json, "field99"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(99.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsArrayWithStringsContainingQuotes()
        {
            // Ensure the array skipper handles escaped quotes inside string elements
            var json = Encoding.UTF8.GetBytes("{\"arr\":[\"he\\\"llo\",\"world\"],\"target\":1}");
            var token = Extract(json, "target"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(1.0, token.Num);
        }

        [Test]
        public void ExtractField_SkipsStringWithEscapedBackslashBeforeClosingQuote()
        {
            // The value is the string: ends_with_backslash\  (the JSON encodes \\ at the end)
            // This tests that \\\" is parsed as \\ + " (close quote), not \ + \"
            var json = Encoding.UTF8.GetBytes("{\"a\":\"ends_with_backslash\\\\\",\"b\":2}");
            var token = Extract(json, "b"u8);
            ClassicAssert.AreEqual(ExprTokenType.Num, token.TokenType);
            ClassicAssert.AreEqual(2.0, token.Num);
        }
    }
}
