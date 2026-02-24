// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Allure.NUnit;
using Garnet.server.Vector.Filter;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class VectorFilterEvaluatorTests : AllureTestBase
    {
        [Test]
        public void Evaluator_Arithmetic()
        {
            var json = "{}";
            ClassicAssert.AreEqual(5.0, VectorFilterTestHelpers.EvaluateFilter("2 + 3", json).AsNumber());
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("3 - 2", json).AsNumber());
            ClassicAssert.AreEqual(6.0, VectorFilterTestHelpers.EvaluateFilter("2 * 3", json).AsNumber());
            ClassicAssert.AreEqual(2.5, VectorFilterTestHelpers.EvaluateFilter("5 / 2", json).AsNumber());
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("7 % 3", json).AsNumber());
            ClassicAssert.AreEqual(8.0, VectorFilterTestHelpers.EvaluateFilter("2 ** 3", json).AsNumber());
        }

        [Test]
        public void Evaluator_SubtractionWithField()
        {
            var json = "{\"year\":1980}";
            ClassicAssert.AreEqual(1975.0, VectorFilterTestHelpers.EvaluateFilter(".year - 5", json).AsNumber());
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(".year - 5 > 0", json));
        }

        [Test]
        public void Evaluator_Comparison()
        {
            var json = "{}";
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("5 > 3", json).AsNumber());
            ClassicAssert.AreEqual(0.0, VectorFilterTestHelpers.EvaluateFilter("3 > 5", json).AsNumber());
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("3 < 5", json).AsNumber());
            ClassicAssert.AreEqual(0.0, VectorFilterTestHelpers.EvaluateFilter("5 < 3", json).AsNumber());
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("5 >= 5", json).AsNumber());
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("5 <= 5", json).AsNumber());
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("5 == 5", json).AsNumber());
            ClassicAssert.AreEqual(1.0, VectorFilterTestHelpers.EvaluateFilter("5 != 3", json).AsNumber());
            ClassicAssert.AreEqual(0.0, VectorFilterTestHelpers.EvaluateFilter("5 != 5", json).AsNumber());
        }

        [Test]
        public void Evaluator_LogicalAnd()
        {
            var json = "{}";
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("true and true", json));
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy("true and false", json));
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy("false and true", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("true && true", json));
        }

        [Test]
        public void Evaluator_LogicalOr()
        {
            var json = "{}";
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("true or false", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("false or true", json));
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy("false or false", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("false || true", json));
        }

        [Test]
        public void Evaluator_LogicalNot()
        {
            var json = "{}";
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy("not true", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("not false", json));
        }

        [Test]
        public void Evaluator_StringEquality()
        {
            var json = "{\"genre\":\"action\"}";
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(".genre == \"action\"", json));
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy(".genre == \"drama\"", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(".genre != \"drama\"", json));
        }

        [Test]
        public void Evaluator_MemberAccess()
        {
            var json = "{\"year\":1980,\"rating\":4.5}";
            ClassicAssert.AreEqual(1980.0, VectorFilterTestHelpers.EvaluateFilter(".year", json).AsNumber());
            ClassicAssert.AreEqual(4.5, VectorFilterTestHelpers.EvaluateFilter(".rating", json).AsNumber());
        }

        [Test]
        public void Evaluator_MissingFieldReturnsNull()
        {
            var json = "{\"year\":1980}";
            var result = VectorFilterTestHelpers.EvaluateFilter(".missing", json);
            ClassicAssert.IsTrue(result.IsNull);
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy(".missing", json));
        }

        [Test]
        public void Evaluator_InOperatorWithArray()
        {
            var json = "{\"tags\":[\"classic\",\"popular\"]}";
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("\"classic\" in .tags", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("\"popular\" in .tags", json));
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy("\"modern\" in .tags", json));
        }

        [Test]
        public void Evaluator_InOperatorWithNumericArray()
        {
            var json = "{\"scores\":[1,2,3]}";
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("2 in .scores", json));
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy("5 in .scores", json));
        }

        [Test]
        public void Evaluator_IsTruthy_FilterValue()
        {
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.Null));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.False));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.FromNumber(0.0)));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.FromString("")));

            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.True));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.FromNumber(1.0)));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.FromNumber(-1.0)));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.FromString("hello")));
        }

        [Test]
        public void Evaluator_ComplexExpression()
        {
            var json = "{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"tags\":[\"classic\",\"popular\"]}";

            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(
                ".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags)", json));

            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy(
                "(.year > 2000 or .year < 1970) and .rating >= 4.0", json));

            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy("not (.genre == \"drama\")", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(".year / 10 >= 198", json));
        }

        [Test]
        public void Evaluator_ComparisonWithMissingField()
        {
            var json = "{\"year\":1980}";
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy(".missing > 1950", json));
        }

        [Test]
        public void Evaluator_BooleanJsonValues()
        {
            var json = "{\"active\":true,\"deleted\":false}";
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(".active", json));
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy(".deleted", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(".active == true", json));
        }

        [Test]
        public void Evaluator_ArithmeticWithNonNumericString_CoercesToZero()
        {
            var json = "{\"genre\":\"action\"}";
            ClassicAssert.AreEqual(2.0, VectorFilterTestHelpers.EvaluateFilter(".genre + 2", json).AsNumber());
            ClassicAssert.AreEqual(-1.0, VectorFilterTestHelpers.EvaluateFilter(".genre - 1", json).AsNumber());
        }

        [Test]
        public void Evaluator_InOperatorWithNonArrayHaystack_ReturnsFalse()
        {
            var json = "{\"genre\":\"action\"}";
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy("\"action\" in .genre", json));
        }

        [Test]
        public void Evaluator_EqualityBetweenNumberAndNonNumericString_ReturnsFalse()
        {
            var json = "{\"genre\":\"action\"}";
            ClassicAssert.IsFalse(VectorFilterTestHelpers.EvaluateFilterTruthy(".genre == 1", json));
            ClassicAssert.IsTrue(VectorFilterTestHelpers.EvaluateFilterTruthy(".genre == 0", json));
        }
    }
}