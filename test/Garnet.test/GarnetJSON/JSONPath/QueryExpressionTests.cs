using GarnetJSON.JSONPath;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Garnet.test.JSONPath
{
    [TestFixture]
    public class QueryExpressionTests
    {
        [Test]
        public void AndExpressionTest()
        {
            CompositeExpression compositeExpression = new CompositeExpression(QueryOperator.And)
            {
                Expressions = new List<QueryExpression>
                {
                    new BooleanQueryExpression(QueryOperator.Exists, new List<PathFilter> { new FieldFilter("FirstName") }, null),
                    new BooleanQueryExpression(QueryOperator.Exists, new List<PathFilter> { new FieldFilter("LastName") }, null)
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
                    new BooleanQueryExpression(QueryOperator.Exists, new List<PathFilter> { new FieldFilter("FirstName") }, null),
                    new BooleanQueryExpression(QueryOperator.Exists, new List<PathFilter> { new FieldFilter("LastName") }, null)
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
        public void BooleanExpressionTest_RegexEqualsOperator()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.RegexEquals, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/foo.*d/\""));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"food\"]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"fooood and drink\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"FOOD\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"foo\", \"foog\", \"good\"]")));

            BooleanQueryExpression e2 = new BooleanQueryExpression(QueryOperator.RegexEquals, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/Foo.*d/i\""));

            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"food\"]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"fooood and drink\"]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"FOOD\"]")));
            ClassicAssert.IsFalse(e2.IsMatch(null, JsonNode.Parse("[\"foo\", \"foog\", \"good\"]")));
        }

        [Test]
        public void BooleanExpressionTest_RegexEqualsOperator_CornerCase()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.RegexEquals, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/// comment/\""));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"// comment\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"//comment\", \"/ comment\"]")));

            BooleanQueryExpression e2 = new BooleanQueryExpression(QueryOperator.RegexEquals, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("\"/<tag>.*</tag>/i\""));

            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[\"<Tag>Test</Tag>\", \"\"]")));
            ClassicAssert.IsFalse(e2.IsMatch(null, JsonNode.Parse("[\"<tag>Test<tag>\"]")));
        }

        [Test]
        public void BooleanExpressionTest()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.LessThan, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[1, 2, 3, 4, 5]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 3, 4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[3, 4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"11\", 5]")));

            BooleanQueryExpression e2 = new BooleanQueryExpression(QueryOperator.LessThanOrEquals, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[1, 2, 3, 4, 5]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[2, 3, 4, 5]")));
            ClassicAssert.IsTrue(e2.IsMatch(null, JsonNode.Parse("[3, 4, 5]")));
            ClassicAssert.IsFalse(e2.IsMatch(null, JsonNode.Parse("[4, 5]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"11\", 5]")));
        }

        [Test]
        public void BooleanExpressionTest_GreaterThanOperator()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.GreaterThan, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"26\"]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 26]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[2, 3]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"3\"]")));
        }

        [Test]
        public void BooleanExpressionTest_GreaterThanOrEqualsOperator()
        {
            BooleanQueryExpression e1 = new BooleanQueryExpression(QueryOperator.GreaterThanOrEquals, new List<PathFilter> { new ArrayIndexFilter() }, JsonNode.Parse("3"));

            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"26\"]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 26]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[2, 3]")));
            ClassicAssert.IsTrue(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"3\"]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[2, 1]")));
            ClassicAssert.IsFalse(e1.IsMatch(null, JsonNode.Parse("[\"2\", \"1\"]")));
        }
    }
}