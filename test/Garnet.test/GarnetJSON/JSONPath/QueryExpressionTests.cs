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
using System.Collections.Generic;
using System.Text.Json.Nodes;
using Allure.NUnit;
using GarnetJSON.JSONPath;
using NUnit.Framework;
using NUnit.Framework.Legacy;


namespace Garnet.test.JSONPath
{
    [AllureNUnit]
    [TestFixture]
    public class QueryExpressionTests : AllureTestBase
    {
        [Test]
        public void AndExpressionTest()
        {
            CompositeExpression compositeExpression = new CompositeExpression(QueryOperator.And)
            {
                Expressions = new List<QueryExpression>
                {
                    new BooleanQueryExpression(QueryOperator.Exists,
                        new List<PathFilter> { new FieldFilter("FirstName") }, null),
                    new BooleanQueryExpression(QueryOperator.Exists,
                        new List<PathFilter> { new FieldFilter("LastName") }, null)
                }
            };

            var o1 = JsonNode.Parse("{\"Title\":\"Title!\",\"FirstName\":\"FirstName!\",\"LastName\":\"LastName!\"}");

            ClassicAssert.IsTrue(compositeExpression.IsMatch(o1, o1));

            var o2 = JsonNode.Parse("{\"Title\":\"Title!\",\"FirstName\":\"FirstName!\"}");

            ClassicAssert.IsFalse(compositeExpression.IsMatch(o2, o2));

            var o3 = JsonNode.Parse("{\"Title\":\"Title!\"}");

            ClassicAssert.IsFalse(compositeExpression.IsMatch(o3, o3));
        }

        [Test]
        public void OrExpressionTest()
        {
            CompositeExpression compositeExpression = new CompositeExpression(QueryOperator.Or)
            {
                Expressions = new List<QueryExpression>
                {
                    new BooleanQueryExpression(QueryOperator.Exists,
                        new List<PathFilter> { new FieldFilter("FirstName") }, null),
                    new BooleanQueryExpression(QueryOperator.Exists,
                        new List<PathFilter> { new FieldFilter("LastName") }, null)
                }
            };

            var o1 = JsonNode.Parse("{\"Title\":\"Title!\",\"FirstName\":\"FirstName!\",\"LastName\":\"LastName!\"}");

            ClassicAssert.IsTrue(compositeExpression.IsMatch(o1, o1));

            var o2 = JsonNode.Parse("{\"Title\":\"Title!\",\"FirstName\":\"FirstName!\"}");

            ClassicAssert.IsTrue(compositeExpression.IsMatch(o2, o2));

            var o3 = JsonNode.Parse("{\"Title\":\"Title!\"}");

            ClassicAssert.IsFalse(compositeExpression.IsMatch(o3, o3));
        }

        [Test]
        public void BooleanExpression_EqualsOperator()
        {
            string json = """
                          {
                          "field1": "test",
                          "field2": "hi",
                          "field3": true,
                          "field4": 1234,
                          "field5": null
                          }
                          """;
            JsonNode payload = JsonNode.Parse(json)!;
            (string FieldName, JsonNode Value, bool ShouldMatch)[] checks =
            [
                ("field1", JsonNode.Parse("null"), false),
                ("field2", JsonNode.Parse("\"hi\""), true),
                ("field3", JsonNode.Parse("true"), true),
                ("field3", JsonNode.Parse("false"), false),
                ("field4", JsonNode.Parse("1234"), true),
                ("field5", JsonNode.Parse("null"), true),
                ("field5", JsonNode.Parse("123"), false)
            ];

            foreach (var check in checks)
            {
                BooleanQueryExpression EqualExpression = new BooleanQueryExpression(QueryOperator.Equals,
                    new List<PathFilter> { new FieldFilter(check.FieldName) }, check.Value);
                bool EqualResult = EqualExpression.IsMatch(payload, payload);
                ClassicAssert.AreEqual(check.ShouldMatch, EqualResult, "Equals {0} - {1}", check.FieldName,
                    check.Value);

                BooleanQueryExpression NotEqualExpression = new BooleanQueryExpression(QueryOperator.NotEquals,
                    new List<PathFilter> { new FieldFilter(check.FieldName) }, check.Value);

                bool NotEqualResult = NotEqualExpression.IsMatch(payload, payload);
                ClassicAssert.AreNotEqual(check.ShouldMatch, NotEqualResult, "Not Equals {0} - {1}", check.FieldName,
                    check.Value);
            }
        }

        [Test]
        public void BooleanExpressionTest_RegexEqualsOperator()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.RegexEquals,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/foo.*d/\""));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"food\"]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"fooood and drink\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"FOOD\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"foo\", \"foog\", \"good\"]")));

            BooleanQueryExpression e2 = new BooleanQueryExpression(QueryOperator.RegexEquals,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/Foo.*d/i\""));

            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"food\"]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"fooood and drink\"]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"FOOD\"]")));
            ClassicAssert.IsFalse(e2.IsMatch(null, JsonNode.Parse("[\"foo\", \"foog\", \"good\"]")));
        }

        [Test]
        public void BooleanExpressionTest_RegexEqualsOperator_CornerCase()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.RegexEquals,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/// comment/\""));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"// comment\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"//comment\", \"/ comment\"]")));

            BooleanQueryExpression e2 = new BooleanQueryExpression(QueryOperator.RegexEquals,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/<tag>.*</tag>/i\""));

            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"<Tag>Test</Tag>\", \"\"]")));
            ClassicAssert.IsFalse(e2.IsMatch(null, JsonNode.Parse("[\"<tag>Test<tag>\"]")));
        }

        [Test]
        public void BooleanExpressionTest()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.LessThan,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[1, 2, 3, 4, 5]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 3, 4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[3, 4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"11\", 5]")));

            BooleanQueryExpression e2 = new BooleanQueryExpression(QueryOperator.LessThanOrEquals,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[1, 2, 3, 4, 5]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[2, 3, 4, 5]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[3, 4, 5]")));
            ClassicAssert.IsFalse(e2.IsMatch(null, JsonNode.Parse("[4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"11\", 5]")));
        }

        [Test]
        public void BooleanExpressionTest_GreaterThanOperator()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.GreaterThan,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"26\"]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 26]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[2, 3]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"3\"]")));
        }

        [Test]
        public void BooleanExpressionTest_GreaterThanOrEqualsOperator()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.GreaterThanOrEquals,
                new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"26\"]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 26]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 3]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"3\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[2, 1]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"1\"]")));
        }
    }
}