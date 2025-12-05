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

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Allure.NUnit;
using GarnetJSON.JSONPath;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.JSONPath
{
    [AllureNUnit]
    [TestFixture]
    public class JsonPathParseTests : AllureTestBase
    {
        [Test]
        public void BooleanQuery_TwoValues()
        {
            JsonPath path = new JsonPath("[?(1 > 2)]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            BooleanQueryExpression booleanExpression =
                (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(1, ((JsonValue)booleanExpression.Left).GetValue<long>());
            ClassicAssert.AreEqual(2, ((JsonValue)booleanExpression.Right).GetValue<long>());
            ClassicAssert.AreEqual(QueryOperator.GreaterThan, booleanExpression.Operator);
        }

        [Test]
        public void BooleanQuery_TwoPaths()
        {
            JsonPath path = new JsonPath("[?(@.price > @.max_price)]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            BooleanQueryExpression booleanExpression =
                (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            List<PathFilter> leftPaths = (List<PathFilter>)booleanExpression.Left;
            List<PathFilter> rightPaths = (List<PathFilter>)booleanExpression.Right;

            ClassicAssert.AreEqual("price", ((FieldFilter)leftPaths[0]).Name);
            ClassicAssert.AreEqual("max_price", ((FieldFilter)rightPaths[0]).Name);
            ClassicAssert.AreEqual(QueryOperator.GreaterThan, booleanExpression.Operator);
        }

        [Test]
        public void SingleProperty()
        {
            JsonPath path = new JsonPath("Blah");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void SingleQuotedProperty()
        {
            JsonPath path = new JsonPath("['Blah']");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void SingleQuotedPropertyWithWhitespace()
        {
            JsonPath path = new JsonPath("[  'Blah'  ]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void SingleQuotedPropertyWithDots()
        {
            JsonPath path = new JsonPath("['Blah.Ha']");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("Blah.Ha", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void SingleQuotedPropertyWithBrackets()
        {
            JsonPath path = new JsonPath("['[*]']");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("[*]", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void SinglePropertyWithRoot()
        {
            JsonPath path = new JsonPath("$.Blah");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void SinglePropertyWithRootWithStartAndEndWhitespace()
        {
            JsonPath path = new JsonPath(" $.Blah ");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void RootWithBadWhitespace()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("$ .Blah"); },
                @"Unexpected character while parsing path:  ");
        }

        [Test]
        public void NoFieldNameAfterDot()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("$.Blah."); },
                @"Unexpected end while parsing path.");
        }

        [Test]
        public void RootWithBadWhitespace2()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("$. Blah"); },
                @"Unexpected character while parsing path:  ");
        }

        [Test]
        public void WildcardPropertyWithRoot()
        {
            JsonPath path = new JsonPath("$.*");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(null, ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void WildcardArrayWithRoot()
        {
            JsonPath path = new JsonPath("$.[*]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(null, ((ArrayIndexFilter)path.Filters[0]).Index);
        }

        [Test]
        public void RootArrayNoDot()
        {
            JsonPath path = new JsonPath("$[1]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(1, ((ArrayIndexFilter)path.Filters[0]).Index);
        }

        [Test]
        public void WildcardArray()
        {
            JsonPath path = new JsonPath("[*]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(null, ((ArrayIndexFilter)path.Filters[0]).Index);
        }

        [Test]
        public void WildcardArrayWithProperty()
        {
            JsonPath path = new JsonPath("[ * ].derp");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual(null, ((ArrayIndexFilter)path.Filters[0]).Index);
            ClassicAssert.AreEqual("derp", ((FieldFilter)path.Filters[1]).Name);
        }

        [Test]
        public void QuotedWildcardPropertyWithRoot()
        {
            JsonPath path = new JsonPath("$.['*']");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("*", ((FieldFilter)path.Filters[0]).Name);
        }

        [Test]
        public void SingleScanWithRoot()
        {
            JsonPath path = new JsonPath("$..Blah");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((ScanFilter)path.Filters[0]).Name);
        }

        [Test]
        public void QueryTrue()
        {
            JsonPath path = new JsonPath("$.elements[?(true)]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("elements", ((FieldFilter)path.Filters[0]).Name);
            ClassicAssert.AreEqual(QueryOperator.Exists, ((QueryFilter)path.Filters[1]).Expression.Operator);
        }

        [Test]
        public void ScanQuery()
        {
            JsonPath path = new JsonPath("$.elements..[?(@.id=='AAA')]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("elements", ((FieldFilter)path.Filters[0]).Name);

            BooleanQueryExpression expression = (BooleanQueryExpression)((QueryScanFilter)path.Filters[1]).Expression;

            List<PathFilter> paths = (List<PathFilter>)expression.Left;

            ClassicAssert.IsInstanceOf(typeof(FieldFilter), paths[0]);
        }

        [Test]
        public void WildcardScanWithRoot()
        {
            JsonPath path = new JsonPath("$..*");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(null, ((ScanFilter)path.Filters[0]).Name);
        }

        [Test]
        public void WildcardScanWithRootWithWhitespace()
        {
            JsonPath path = new JsonPath("$..* ");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(null, ((ScanFilter)path.Filters[0]).Name);
        }

        [Test]
        public void TwoProperties()
        {
            JsonPath path = new JsonPath("Blah.Two");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            ClassicAssert.AreEqual("Two", ((FieldFilter)path.Filters[1]).Name);
        }

        [Test]
        public void OnePropertyOneScan()
        {
            JsonPath path = new JsonPath("Blah..Two");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            ClassicAssert.AreEqual("Two", ((ScanFilter)path.Filters[1]).Name);
        }

        [Test]
        public void SinglePropertyAndIndexer()
        {
            JsonPath path = new JsonPath("Blah[0]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            ClassicAssert.AreEqual(0, ((ArrayIndexFilter)path.Filters[1]).Index);
        }

        [Test]
        public void SinglePropertyAndExistsQuery()
        {
            JsonPath path = new JsonPath("Blah[ ?( @..name ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.Exists, expressions.Operator);
            List<PathFilter> paths = (List<PathFilter>)expressions.Left;
            ClassicAssert.AreEqual(1, paths.Count);
            ClassicAssert.AreEqual("name", ((ScanFilter)paths[0]).Name);
        }

        [Test]
        public void SinglePropertyAndFilterWithWhitespace()
        {
            JsonPath path = new JsonPath("Blah[ ?( @.name=='hi' ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.Equals, expressions.Operator);
            ClassicAssert.AreEqual("hi", ((JsonValue)expressions.Right).GetValue<string>());
        }

        [Test]
        public void SinglePropertyAndFilterWithEscapeQuote()
        {
            JsonPath path = new JsonPath(@"Blah[ ?( @.name=='h\'i' ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.Equals, expressions.Operator);
            ClassicAssert.AreEqual("h'i", ((JsonValue)expressions.Right).GetValue<string>());
        }

        [Test]
        public void SinglePropertyAndFilterWithDoubleEscape()
        {
            JsonPath path = new JsonPath(@"Blah[ ?( @.name=='h\\i' ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.Equals, expressions.Operator);
            ClassicAssert.AreEqual("h\\i", ((JsonValue)expressions.Right).GetValue<string>());
        }

        [Test]
        public void SinglePropertyAndFilterWithRegexAndOptions()
        {
            JsonPath path = new JsonPath("Blah[ ?( @.name=~/hi/i ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.RegexEquals, expressions.Operator);
            ClassicAssert.AreEqual("/hi/i", ((JsonValue)expressions.Right).GetValue<string>());
        }

        [Test]
        public void SinglePropertyAndFilterWithRegex()
        {
            JsonPath path = new JsonPath("Blah[?(@.title =~ /^.*Sword.*$/)]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.RegexEquals, expressions.Operator);
            ClassicAssert.AreEqual("/^.*Sword.*$/", ((JsonValue)expressions.Right).GetValue<string>());
        }

        [Test]
        public void SinglePropertyAndFilterWithEscapedRegex()
        {
            JsonPath path = new JsonPath(@"Blah[?(@.title =~ /[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g)]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.RegexEquals, expressions.Operator);
            ClassicAssert.AreEqual(@"/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g",
                ((JsonValue)expressions.Right).GetValue<string>());
        }

        [Test]
        public void SinglePropertyAndFilterWithOpenRegex()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath(@"Blah[?(@.title =~ /[\"); },
                "Path ended with an open regex.");
        }

        [Test]
        public void SinglePropertyAndFilterWithUnknownEscape()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath(@"Blah[ ?( @.name=='h\i' ) ]"); },
                @"Unknown escape character: \i");
        }

        [Test]
        public void SinglePropertyAndFilterWithFalse()
        {
            JsonPath path = new JsonPath("Blah[ ?( @.name==false ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.Equals, expressions.Operator);
            ClassicAssert.AreEqual(false, ((JsonValue)expressions.Right).GetValue<bool>());
        }

        [Test]
        public void SinglePropertyAndFilterWithTrue()
        {
            JsonPath path = new JsonPath("Blah[ ?( @.name==true ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.Equals, expressions.Operator);
            ClassicAssert.AreEqual(true, ((JsonValue)expressions.Right).GetValue<bool>());
        }

        [Test]
        public void SinglePropertyAndFilterWithNull()
        {
            JsonPath path = new JsonPath("Blah[ ?( @.name==null ) ]");
            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.Equals, expressions.Operator);
            ClassicAssert.IsNull(expressions.Right);
        }

        [Test]
        public void FilterWithScan()
        {
            JsonPath path = new JsonPath("[?(@..name<>null)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            List<PathFilter> paths = (List<PathFilter>)expressions.Left;
            ClassicAssert.AreEqual("name", ((ScanFilter)paths[0]).Name);
        }

        [Test]
        public void FilterWithNotEquals()
        {
            JsonPath path = new JsonPath("[?(@.name<>null)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.NotEquals, expressions.Operator);
        }

        [Test]
        public void FilterWithNotEquals2()
        {
            JsonPath path = new JsonPath("[?(@.name!=null)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.NotEquals, expressions.Operator);
        }

        [Test]
        public void FilterWithLessThan()
        {
            JsonPath path = new JsonPath("[?(@.name<null)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.LessThan, expressions.Operator);
        }

        [Test]
        public void FilterWithLessThanOrEquals()
        {
            JsonPath path = new JsonPath("[?(@.name<=null)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.LessThanOrEquals, expressions.Operator);
        }

        [Test]
        public void FilterWithGreaterThan()
        {
            JsonPath path = new JsonPath("[?(@.name>null)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.GreaterThan, expressions.Operator);
        }

        [Test]
        public void FilterWithGreaterThanOrEquals()
        {
            JsonPath path = new JsonPath("[?(@.name>=null)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.GreaterThanOrEquals, expressions.Operator);
        }

        [Test]
        public void FilterWithInteger()
        {
            JsonPath path = new JsonPath("[?(@.name>=12)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(12, ((JsonValue)expressions.Right).GetValue<long>());
        }

        [Test]
        public void FilterWithNegativeInteger()
        {
            JsonPath path = new JsonPath("[?(@.name>=-12)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(-12, ((JsonValue)expressions.Right).GetValue<long>());
        }

        [Test]
        public void FilterWithFloat()
        {
            JsonPath path = new JsonPath("[?(@.name>=12.1)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(12.1d, ((JsonValue)expressions.Right).GetValue<double>());
        }

        [Test]
        public void FilterExistWithAnd()
        {
            JsonPath path = new JsonPath("[?(@.name&&@.title)]");
            CompositeExpression expressions = (CompositeExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.And, expressions.Operator);
            ClassicAssert.AreEqual(2, expressions.Expressions.Count);

            var first = (BooleanQueryExpression)expressions.Expressions[0];
            var firstPaths = (List<PathFilter>)first.Left;
            ClassicAssert.AreEqual("name", ((FieldFilter)firstPaths[0]).Name);
            ClassicAssert.AreEqual(QueryOperator.Exists, first.Operator);

            var second = (BooleanQueryExpression)expressions.Expressions[1];
            var secondPaths = (List<PathFilter>)second.Left;
            ClassicAssert.AreEqual("title", ((FieldFilter)secondPaths[0]).Name);
            ClassicAssert.AreEqual(QueryOperator.Exists, second.Operator);
        }

        [Test]
        public void FilterExistWithAndOr()
        {
            JsonPath path = new JsonPath("[?(@.name&&@.title||@.pie)]");
            CompositeExpression andExpression = (CompositeExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(QueryOperator.And, andExpression.Operator);
            ClassicAssert.AreEqual(2, andExpression.Expressions.Count);

            var first = (BooleanQueryExpression)andExpression.Expressions[0];
            var firstPaths = (List<PathFilter>)first.Left;
            ClassicAssert.AreEqual("name", ((FieldFilter)firstPaths[0]).Name);
            ClassicAssert.AreEqual(QueryOperator.Exists, first.Operator);

            CompositeExpression orExpression = (CompositeExpression)andExpression.Expressions[1];
            ClassicAssert.AreEqual(2, orExpression.Expressions.Count);

            var orFirst = (BooleanQueryExpression)orExpression.Expressions[0];
            var orFirstPaths = (List<PathFilter>)orFirst.Left;
            ClassicAssert.AreEqual("title", ((FieldFilter)orFirstPaths[0]).Name);
            ClassicAssert.AreEqual(QueryOperator.Exists, orFirst.Operator);

            var orSecond = (BooleanQueryExpression)orExpression.Expressions[1];
            var orSecondPaths = (List<PathFilter>)orSecond.Left;
            ClassicAssert.AreEqual("pie", ((FieldFilter)orSecondPaths[0]).Name);
            ClassicAssert.AreEqual(QueryOperator.Exists, orSecond.Operator);
        }

        [Test]
        public void FilterWithRoot()
        {
            JsonPath path = new JsonPath("[?($.name>=12.1)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            List<PathFilter> paths = (List<PathFilter>)expressions.Left;
            ClassicAssert.AreEqual(2, paths.Count);
            ClassicAssert.IsInstanceOf(typeof(RootFilter), paths[0]);
            ClassicAssert.IsInstanceOf(typeof(FieldFilter), paths[1]);
        }

        [Test]
        public void BadOr1()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name||)]"),
                "Unexpected character while parsing path query: )");
        }

        [Test]
        public void BaddOr2()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name|)]"),
                "Unexpected character while parsing path query: |");
        }

        [Test]
        public void BaddOr3()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name|"),
                "Unexpected character while parsing path query: |");
        }

        [Test]
        public void BaddOr4()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name||"), "Path ended with open query.");
        }

        [Test]
        public void NoAtAfterOr()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name||s"),
                "Unexpected character while parsing path query: s");
        }

        [Test]
        public void NoPathAfterAt()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name||@"), @"Path ended with open query.");
        }

        [Test]
        public void NoPathAfterDot()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name||@."),
                @"Unexpected end while parsing path.");
        }

        [Test]
        public void NoPathAfterDot2()
        {
            ClassicAssert.Throws<JsonException>(() => new JsonPath("[?(@.name||@.)]"),
                @"Unexpected end while parsing path.");
        }

        [Test]
        public void FilterWithFloatExp()
        {
            JsonPath path = new JsonPath("[?(@.name>=5.56789e+0)]");
            BooleanQueryExpression expressions = (BooleanQueryExpression)((QueryFilter)path.Filters[0]).Expression;
            ClassicAssert.AreEqual(5.56789e+0, ((JsonValue)expressions.Right).GetValue<double>());
        }

        [Test]
        public void MultiplePropertiesAndIndexers()
        {
            JsonPath path = new JsonPath("Blah[0]..Two.Three[1].Four");
            ClassicAssert.AreEqual(6, path.Filters.Count);
            ClassicAssert.AreEqual("Blah", ((FieldFilter)path.Filters[0]).Name);
            ClassicAssert.AreEqual(0, ((ArrayIndexFilter)path.Filters[1]).Index);
            ClassicAssert.AreEqual("Two", ((ScanFilter)path.Filters[2]).Name);
            ClassicAssert.AreEqual("Three", ((FieldFilter)path.Filters[3]).Name);
            ClassicAssert.AreEqual(1, ((ArrayIndexFilter)path.Filters[4]).Index);
            ClassicAssert.AreEqual("Four", ((FieldFilter)path.Filters[5]).Name);
        }

        [Test]
        public void BadCharactersInIndexer()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("Blah[[0]].Two.Three[1].Four"); },
                @"Unexpected character while parsing path indexer: [");
        }

        [Test]
        public void UnclosedIndexer()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("Blah[0"); }, @"Path ended with open indexer.");
        }

        [Test]
        public void IndexerOnly()
        {
            JsonPath path = new JsonPath("[111119990]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(111119990, ((ArrayIndexFilter)path.Filters[0]).Index);
        }

        [Test]
        public void IndexerOnlyWithWhitespace()
        {
            JsonPath path = new JsonPath("[  10  ]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(10, ((ArrayIndexFilter)path.Filters[0]).Index);
        }

        [Test]
        public void MultipleIndexes()
        {
            JsonPath path = new JsonPath("[111119990,3]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(2, ((ArrayMultipleIndexFilter)path.Filters[0]).Indexes.Count);
            ClassicAssert.AreEqual(111119990, ((ArrayMultipleIndexFilter)path.Filters[0]).Indexes[0]);
            ClassicAssert.AreEqual(3, ((ArrayMultipleIndexFilter)path.Filters[0]).Indexes[1]);
        }

        [Test]
        public void MultipleIndexesWithWhitespace()
        {
            JsonPath path = new JsonPath("[   111119990  ,   3   ]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(2, ((ArrayMultipleIndexFilter)path.Filters[0]).Indexes.Count);
            ClassicAssert.AreEqual(111119990, ((ArrayMultipleIndexFilter)path.Filters[0]).Indexes[0]);
            ClassicAssert.AreEqual(3, ((ArrayMultipleIndexFilter)path.Filters[0]).Indexes[1]);
        }

        [Test]
        public void MultipleQuotedIndexes()
        {
            JsonPath path = new JsonPath("['111119990','3']");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(2, ((FieldMultipleFilter)path.Filters[0]).Names.Count);
            ClassicAssert.AreEqual("111119990", ((FieldMultipleFilter)path.Filters[0]).Names[0]);
            ClassicAssert.AreEqual("3", ((FieldMultipleFilter)path.Filters[0]).Names[1]);
        }

        [Test]
        public void MultipleQuotedIndexesWithWhitespace()
        {
            JsonPath path = new JsonPath("[ '111119990' , '3' ]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(2, ((FieldMultipleFilter)path.Filters[0]).Names.Count);
            ClassicAssert.AreEqual("111119990", ((FieldMultipleFilter)path.Filters[0]).Names[0]);
            ClassicAssert.AreEqual("3", ((FieldMultipleFilter)path.Filters[0]).Names[1]);
        }

        [Test]
        public void SlicingIndexAll()
        {
            JsonPath path = new JsonPath("[111119990:3:2]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(111119990, ((ArraySliceFilter)path.Filters[0]).Start);
            ClassicAssert.AreEqual(3, ((ArraySliceFilter)path.Filters[0]).End);
            ClassicAssert.AreEqual(2, ((ArraySliceFilter)path.Filters[0]).Step);
        }

        [Test]
        public void SlicingIndex()
        {
            JsonPath path = new JsonPath("[111119990:3]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(111119990, ((ArraySliceFilter)path.Filters[0]).Start);
            ClassicAssert.AreEqual(3, ((ArraySliceFilter)path.Filters[0]).End);
            ClassicAssert.AreEqual(null, ((ArraySliceFilter)path.Filters[0]).Step);
        }

        [Test]
        public void SlicingIndexNegative()
        {
            JsonPath path = new JsonPath("[-111119990:-3:-2]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(-111119990, ((ArraySliceFilter)path.Filters[0]).Start);
            ClassicAssert.AreEqual(-3, ((ArraySliceFilter)path.Filters[0]).End);
            ClassicAssert.AreEqual(-2, ((ArraySliceFilter)path.Filters[0]).Step);
        }

        [Test]
        public void SlicingIndexEmptyStop()
        {
            JsonPath path = new JsonPath("[  -3  :  ]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(-3, ((ArraySliceFilter)path.Filters[0]).Start);
            ClassicAssert.AreEqual(null, ((ArraySliceFilter)path.Filters[0]).End);
            ClassicAssert.AreEqual(null, ((ArraySliceFilter)path.Filters[0]).Step);
        }

        [Test]
        public void SlicingIndexEmptyStart()
        {
            JsonPath path = new JsonPath("[ : 1 : ]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(null, ((ArraySliceFilter)path.Filters[0]).Start);
            ClassicAssert.AreEqual(1, ((ArraySliceFilter)path.Filters[0]).End);
            ClassicAssert.AreEqual(null, ((ArraySliceFilter)path.Filters[0]).Step);
        }

        [Test]
        public void SlicingIndexWhitespace()
        {
            JsonPath path = new JsonPath("[  -111119990  :  -3  :  -2  ]");
            ClassicAssert.AreEqual(1, path.Filters.Count);
            ClassicAssert.AreEqual(-111119990, ((ArraySliceFilter)path.Filters[0]).Start);
            ClassicAssert.AreEqual(-3, ((ArraySliceFilter)path.Filters[0]).End);
            ClassicAssert.AreEqual(-2, ((ArraySliceFilter)path.Filters[0]).Step);
        }

        [Test]
        public void EmptyIndexer()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("[]"); }, "Array index expected.");
        }

        [Test]
        public void IndexerCloseInProperty()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("]"); },
                "Unexpected character while parsing path: ]");
        }

        [Test]
        public void AdjacentIndexers()
        {
            JsonPath path = new JsonPath("[1][0][-4][" + int.MaxValue + "]");
            ClassicAssert.AreEqual(4, path.Filters.Count);
            ClassicAssert.AreEqual(1, ((ArrayIndexFilter)path.Filters[0]).Index);
            ClassicAssert.AreEqual(0, ((ArrayIndexFilter)path.Filters[1]).Index);
            ClassicAssert.AreEqual(-4, ((ArrayIndexFilter)path.Filters[2]).Index);
            ClassicAssert.AreEqual(Array.MaxLength, ((ArrayIndexFilter)path.Filters[3]).Index);
        }

        [Test]
        public void MissingDotAfterIndexer()
        {
            ClassicAssert.Throws<JsonException>(() => { new JsonPath("[1]Blah"); },
                "Unexpected character following indexer: B");
        }

        [Test]
        public void PropertyFollowingEscapedPropertyName()
        {
            JsonPath path = new JsonPath("frameworks.dnxcore50.dependencies.['System.Xml.ReaderWriter'].source");
            ClassicAssert.AreEqual(5, path.Filters.Count);

            ClassicAssert.AreEqual("frameworks", ((FieldFilter)path.Filters[0]).Name);
            ClassicAssert.AreEqual("dnxcore50", ((FieldFilter)path.Filters[1]).Name);
            ClassicAssert.AreEqual("dependencies", ((FieldFilter)path.Filters[2]).Name);
            ClassicAssert.AreEqual("System.Xml.ReaderWriter", ((FieldFilter)path.Filters[3]).Name);
            ClassicAssert.AreEqual("source", ((FieldFilter)path.Filters[4]).Name);
        }

        [Test]
        public void ArrayOfArrayValue()
        {
            JsonPath path = new JsonPath("$.a[?(@.a in [[1,2],[2,3]])]");

            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("a", ((FieldFilter)path.Filters[0]).Name);
            QueryExpression InExpression = ((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.In, InExpression.Operator);
            ClassicAssert.IsInstanceOf<BooleanQueryExpression>(InExpression);
            JsonArray Array = ((BooleanQueryExpression)InExpression).Right as JsonArray;
            ClassicAssert.AreEqual(2, Array?.Count);
            ClassicAssert.AreEqual(true, JsonNode.DeepEquals(Array?[0], JsonNode.Parse("[1,2]")));
        }

        [Test]
        public void ArrayOfArrayValueWithEscaping()
        {
            string hi_there_str = """
                                  [
                                  "\"hi\"",
                                  "there"
                                  ]
                                  """;

            JsonPath path = new JsonPath($"$.a[?(@.a in [{hi_there_str},[2,3]])]");

            ClassicAssert.AreEqual(2, path.Filters.Count);
            ClassicAssert.AreEqual("a", ((FieldFilter)path.Filters[0]).Name);
            QueryExpression InExpression = ((QueryFilter)path.Filters[1]).Expression;
            ClassicAssert.AreEqual(QueryOperator.In, InExpression.Operator);
            ClassicAssert.IsInstanceOf<BooleanQueryExpression>(InExpression);
            JsonArray Array = ((BooleanQueryExpression)InExpression).Right as JsonArray;
            ClassicAssert.AreEqual(2, Array?.Count);


            ClassicAssert.AreEqual(true, JsonNode.DeepEquals(Array?[0], JsonNode.Parse(hi_there_str)));
        }
    }
}