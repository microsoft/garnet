// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests for ExprRunner (stack-based VM) + AttributeExtractor (raw byte JSON extractor).
    /// Verifies the compile-once-run-many evaluation pipeline.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class ExprRunnerTests : AllureTestBase
    {
        [Test]
        public void Runner_Arithmetic()
        {
            var json = "{}";
            ClassicAssert.AreEqual(5.0, ExprTestHelpers.EvaluateFilter("2 + 3", json).Num);
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("3 - 2", json).Num);
            ClassicAssert.AreEqual(6.0, ExprTestHelpers.EvaluateFilter("2 * 3", json).Num);
            ClassicAssert.AreEqual(2.5, ExprTestHelpers.EvaluateFilter("5 / 2", json).Num);
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("7 % 3", json).Num);
            ClassicAssert.AreEqual(8.0, ExprTestHelpers.EvaluateFilter("2 ** 3", json).Num);
        }

        [Test]
        public void Runner_SubtractionWithField()
        {
            var json = "{\"year\":1980}";
            ClassicAssert.AreEqual(1975.0, ExprTestHelpers.EvaluateFilter(".year - 5", json).Num);
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".year - 5 > 0", json));
        }

        [Test]
        public void Runner_Comparison()
        {
            var json = "{}";
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("5 > 3", json).Num);
            ClassicAssert.AreEqual(0.0, ExprTestHelpers.EvaluateFilter("3 > 5", json).Num);
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("3 < 5", json).Num);
            ClassicAssert.AreEqual(0.0, ExprTestHelpers.EvaluateFilter("5 < 3", json).Num);
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("5 >= 5", json).Num);
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("5 <= 5", json).Num);
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("5 == 5", json).Num);
            ClassicAssert.AreEqual(1.0, ExprTestHelpers.EvaluateFilter("5 != 3", json).Num);
            ClassicAssert.AreEqual(0.0, ExprTestHelpers.EvaluateFilter("5 != 5", json).Num);
        }

        [Test]
        public void Runner_LogicalAnd()
        {
            var json = "{}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("true and true", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("true and false", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("false and true", json));
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("true && true", json));
        }

        [Test]
        public void Runner_LogicalOr()
        {
            var json = "{}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("true or false", json));
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("false or true", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("false or false", json));
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("false || true", json));
        }

        [Test]
        public void Runner_LogicalNot()
        {
            var json = "{}";
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("not true", json));
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("not false", json));
        }

        [Test]
        public void Runner_StringEquality()
        {
            var json = "{\"genre\":\"action\"}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".genre == \"action\"", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy(".genre == \"drama\"", json));
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".genre != \"drama\"", json));
        }

        [Test]
        public void Runner_MemberAccess()
        {
            var json = "{\"year\":1980,\"rating\":4.5}";
            ClassicAssert.AreEqual(1980.0, ExprTestHelpers.EvaluateFilter(".year", json).Num);
            ClassicAssert.AreEqual(4.5, ExprTestHelpers.EvaluateFilter(".rating", json).Num);
        }

        [Test]
        public void Runner_MissingFieldReturnsFalse()
        {
            var json = "{\"year\":1980}";
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy(".missing", json));
        }

        [Test]
        public void Runner_InOperatorWithJsonArray()
        {
            var json = "{\"tags\":[\"classic\",\"popular\"]}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("\"classic\" in .tags", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("\"modern\" in .tags", json));
        }

        [Test]
        public void Runner_InOperatorWithNumericJsonArray()
        {
            var json = "{\"scores\":[1,2,3]}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("2 in .scores", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("5 in .scores", json));
        }

        [Test]
        public void Runner_InOperatorWithTupleLiteral()
        {
            var json = "{\"director\":\"Nolan\"}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".director in [\"Spielberg\", \"Nolan\"]", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy(".director in [\"Spielberg\", \"Kubrick\"]", json));
        }

        [Test]
        public void Runner_InOperatorSubstringCheck()
        {
            var json = "{\"name\":\"barfoobar\"}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("\"foo\" in .name", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("\"xyz\" in .name", json));
        }

        [Test]
        public void Runner_ComplexExpression()
        {
            var json = "{\"year\":1980,\"rating\":4.5,\"genre\":\"action\"}";

            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(
                ".rating * 2 > 8 and .year >= 1980", json));

            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy(
                "(.year > 2000 or .year < 1970) and .rating >= 4.0", json));

            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("not (.genre == \"drama\")", json));
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".year / 10 >= 198", json));
        }

        [Test]
        public void Runner_BooleanJsonValues()
        {
            var json = "{\"active\":true,\"deleted\":false}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".active", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy(".deleted", json));
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".active == true", json));
        }

        [Test]
        public void Runner_ArithmeticWithNonNumericString_CoercesToZero()
        {
            var json = "{\"genre\":\"action\"}";
            ClassicAssert.AreEqual(2.0, ExprTestHelpers.EvaluateFilter(".genre + 2", json).Num);
            ClassicAssert.AreEqual(-1.0, ExprTestHelpers.EvaluateFilter(".genre - 1", json).Num);
        }

        [Test]
        public void Runner_NullLiteral()
        {
            var json = "{\"year\":1980}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".year != null", json));
        }

        [Test]
        public void Runner_NonJsonAttributesExcluded()
        {
            var filterBytes = Encoding.UTF8.GetBytes(".year > 1950");
            Span<ExprToken> instrBuf = stackalloc ExprToken[128];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[64];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[128];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[128];
            var instrCount = ExprCompiler.TryCompile(filterBytes, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
            ClassicAssert.IsTrue(instrCount > 0);

            Span<ExprToken> runtimePoolBuf = stackalloc ExprToken[64];
            var program = new ExprProgram
            {
                Instructions = instrBuf[..instrCount],
                Length = instrCount,
                TuplePool = tuplePoolBuf[..tupleCount],
                TuplePoolLength = tupleCount,
                RuntimePool = runtimePoolBuf,
                RuntimePoolLength = 0,
            };

            // Compute selector ranges
            Span<(int, int)> selectorBuf = stackalloc (int, int)[32];
            var selectorCount = 0;
            for (var idx = 0; idx < instrCount; idx++)
            {
                if (instrBuf[idx].TokenType != ExprTokenType.Selector) continue;
                var found = false;
                for (var j = 0; j < selectorCount; j++)
                    if (((ReadOnlySpan<byte>)filterBytes.AsSpan(selectorBuf[j].Item1, selectorBuf[j].Item2)).SequenceEqual(filterBytes.AsSpan(instrBuf[idx].Utf8Start, instrBuf[idx].Utf8Length))) { found = true; break; }
                if (!found) selectorBuf[selectorCount++] = (instrBuf[idx].Utf8Start, instrBuf[idx].Utf8Length);
            }
            var selectorRanges = selectorBuf[..selectorCount];

            Span<ExprToken> extractedFields = stackalloc ExprToken[selectorCount > 0 ? selectorCount : 1];
            Span<ExprToken> stackBuf2 = stackalloc ExprToken[16];
            var stack = new ExprStack(stackBuf2);

            var nonJson = Encoding.UTF8.GetBytes("this is not json");
            AttributeExtractor.ExtractFields(nonJson, filterBytes, selectorRanges, extractedFields, ref program);
            ClassicAssert.IsFalse(ExprRunner.Run(ref program, nonJson, filterBytes, selectorRanges, extractedFields, ref stack));

            var emptyJson = Encoding.UTF8.GetBytes("");
            AttributeExtractor.ExtractFields(emptyJson, filterBytes, selectorRanges, extractedFields, ref program);
            ClassicAssert.IsFalse(ExprRunner.Run(ref program, emptyJson, filterBytes, selectorRanges, extractedFields, ref stack));
        }

        [Test]
        public void Runner_ExactNumericEquality()
        {
            var json = "{}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy("5 == 5", json));
            ClassicAssert.IsFalse(ExprTestHelpers.EvaluateFilterTruthy("5 == 5.0001", json));
        }

        [Test]
        public void Runner_HyphenatedField()
        {
            var json = "{\"my-field\":42}";
            ClassicAssert.AreEqual(42.0, ExprTestHelpers.EvaluateFilter(".my-field", json).Num);
        }

        [Test]
        public void Runner_JsonEscapeHandling()
        {
            var json = "{\"name\":\"hello\\\"world\"}";
            ClassicAssert.IsTrue(ExprTestHelpers.EvaluateFilterTruthy(".name == \"hello\\\"world\"", json));
        }
    }
}