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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Allure.NUnit;
using GarnetJSON.JSONPath;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.JSONPath
{
    [AllureNUnit]
    [TestFixture]
    public class JsonPathExecuteTests : AllureTestBase
    {
        [Test]
        public void GreaterThanIssue1518()
        {
            string statusJson = @"{""usingmem"": ""214376""}"; //214,376
            var jObj = JsonNode.Parse(statusJson);

            var success = jObj.TrySelectNode("$..[?(@.usingmem>10)]", out var result); //found,10
            ClassicAssert.IsTrue(success);
            JsonAssert.AreEqual(jObj, result);

            success = jObj.TrySelectNode("$..[?(@.usingmem>27000)]", out result); //null, 27,000
            ClassicAssert.IsTrue(success);
            JsonAssert.AreEqual(jObj, result);

            success = jObj.TrySelectNode("$..[?(@.usingmem>21437)]", out result); //found, 21,437
            ClassicAssert.IsTrue(success);
            JsonAssert.AreEqual(jObj, result);

            success = jObj.TrySelectNode("$..[?(@.usingmem>21438)]", out result); //null,21,438
            ClassicAssert.IsTrue(success);
            JsonAssert.AreEqual(jObj, result);
        }

        [Test]
        public void BacktrackingRegex_SingleMatch_TimeoutRespected()
        {
            const string RegexBacktrackingPattern =
                "(?<a>(.*?))[|].*(?<b>(.*?))[|].*(?<c>(.*?))[|].*(?<d>[1-3])[|].*(?<e>(.*?))[|].*[|].*[|].*(?<f>(.*?))[|].*[|].*(?<g>(.*?))[|].*(?<h>(.*))";

            var jObj = JsonNode.Parse(
                @"[{""b"": ""15/04/2020 8:18:03 PM|1|System.String[]|3|Libero eligendi magnam ut inventore.. Quaerat et sit voluptatibus repellendus blanditiis aliquam ut.. Quidem qui ut sint in ex et tempore.|||.\\iste.cpp||46018|-1"" }]");

            ClassicAssert.Throws<RegexMatchTimeoutException>((() =>
            {
                jObj.SelectNodes(
                    $"[?(@.b =~ /{RegexBacktrackingPattern}/)]",
                    new JsonSelectSettings { RegexMatchTimeout = TimeSpan.FromSeconds(0.01) }).ToArray();
            }));
        }

        [Test]
        public void GreaterThanWithIntegerParameterAndStringValue()
        {
            string json = @"{
  ""persons"": [
    {
      ""name""  : ""John"",
      ""age"": ""26""
    },
    {
      ""name""  : ""Jane"",
      ""age"": ""2""
    }
  ]
}";

            var models = JsonNode.Parse(json);

            var results = models.SelectNodes("$.persons[?(@.age > 3)]").ToList();

            ClassicAssert.AreEqual(1, results.Count);
        }

        [Test]
        public void GreaterThanWithStringParameterAndIntegerValue()
        {
            string json = @"{
              ""persons"": [
                {
                  ""name""  : ""John"",
                  ""age"": 26
                },
                {
                  ""name""  : ""Jane"",
                  ""age"": 2
                }
              ]
            }";

            var models = JsonNode.Parse(json);

            var results = models.SelectNodes("$.persons[?(@.age > '3')]").ToList();

            ClassicAssert.AreEqual(1, results.Count);
        }

        [Test]
        public void RecursiveWildcard()
        {
            string json = @"{
    ""a"": [
        {
            ""id"": 1
        }
    ],
    ""b"": [
        {
            ""id"": 2
        },
        {
            ""id"": 3,
            ""c"": {
                ""id"": 4
            }
        }
    ],
    ""d"": [
        {
            ""id"": 5
        }
    ]
}";

            var models = JsonNode.Parse(json);

            var results = models.SelectNodes("$.b..*.id").ToList();

            ClassicAssert.AreEqual(3, results.Count);
            ClassicAssert.AreEqual(2, results[0].GetValue<int>());
            ClassicAssert.AreEqual(3, results[1].GetValue<int>());
            ClassicAssert.AreEqual(4, results[2].GetValue<int>());
        }

        [Test]
        public void ScanFilter()
        {
            string json = @"{
  ""elements"": [
    {
      ""id"": ""A"",
      ""children"": [
        {
          ""id"": ""AA"",
          ""children"": [
            {
              ""id"": ""AAA""
            },
            {
              ""id"": ""AAB""
            }
          ]
        },
        {
          ""id"": ""AB""
        }
      ]
    },
    {
      ""id"": ""B"",
      ""children"": []
    }
  ]
}";

            var models = JsonNode.Parse(json);

            var results = models.SelectNodes("$.elements..[?(@.id=='AAA')]").ToList();

            ClassicAssert.AreEqual(1, results.Count);
            JsonAssert.AreEqual(models["elements"][0]["children"][0]["children"][0], results[0]);
        }

        [Test]
        public void FilterTrue()
        {
            string json = @"{
  ""elements"": [
    {
      ""id"": ""A"",
      ""children"": [
        {
          ""id"": ""AA"",
          ""children"": [
            {
              ""id"": ""AAA""
            },
            {
              ""id"": ""AAB""
            }
          ]
        },
        {
          ""id"": ""AB""
        }
      ]
    },
    {
      ""id"": ""B"",
      ""children"": []
    }
  ]
}";

            var models = JsonNode.Parse(json);

            var results = models.SelectNodes("$.elements[?(true)]").ToList();

            ClassicAssert.AreEqual(2, results.Count);
            JsonAssert.AreEqual(results[0], models["elements"][0]);
            JsonAssert.AreEqual(results[1], models["elements"][1]);
        }

        [Test]
        public void ScanFilterTrue()
        {
            string json = @"{
  ""elements"": [
    {
      ""id"": ""A"",
      ""children"": [
        {
          ""id"": ""AA"",
          ""children"": [
            {
              ""id"": ""AAA""
            },
            {
              ""id"": ""AAB""
            }
          ]
        },
        {
          ""id"": ""AB""
        }
      ]
    },
    {
      ""id"": ""B"",
      ""children"": []
    }
  ]
}";

            var models = JsonNode.Parse(json);

            var results = models.SelectNodes("$.elements..[?(true)]").ToList();

            // TODO: I think this should be 15, because results from online evaluators doesn't include the root/self element. Need to verify if changing the returning of self will affect other tests.
            ClassicAssert.AreEqual(16, results.Count);
        }

        [Test]
        public void ScanQuoted()
        {
            string json = @"{
    ""Node1"": {
        ""Child1"": {
            ""Name"": ""IsMe"",
            ""TargetNode"": {
                ""Prop1"": ""Val1"",
                ""Prop2"": ""Val2""
            }
        },
        ""My.Child.Node"": {
            ""TargetNode"": {
                ""Prop1"": ""Val1"",
                ""Prop2"": ""Val2""
            }
        }
    },
    ""Node2"": {
        ""TargetNode"": {
            ""Prop1"": ""Val1"",
            ""Prop2"": ""Val2""
        }
    }
}";

            var models = JsonNode.Parse(json);

            int result = models.SelectNodes("$..['My.Child.Node']").Count();
            ClassicAssert.AreEqual(1, result);

            result = models.SelectNodes("..['My.Child.Node']").Count();
            ClassicAssert.AreEqual(1, result);
        }

        [Test]
        public void ScanMultipleQuoted()
        {
            string json = @"{
    ""Node1"": {
        ""Child1"": {
            ""Name"": ""IsMe"",
            ""TargetNode"": {
                ""Prop1"": ""Val1"",
                ""Prop2"": ""Val2""
            }
        },
        ""My.Child.Node"": {
            ""TargetNode"": {
                ""Prop1"": ""Val3"",
                ""Prop2"": ""Val4""
            }
        }
    },
    ""Node2"": {
        ""TargetNode"": {
            ""Prop1"": ""Val5"",
            ""Prop2"": ""Val6""
        }
    }
}";

            var models = JsonNode.Parse(json);

            var results = models.SelectNodes("$..['My.Child.Node','Prop1','Prop2']").ToList();
            ClassicAssert.AreEqual("Val1", results[0].GetValue<string>());
            ClassicAssert.AreEqual("Val2", results[1].GetValue<string>());
            ClassicAssert.AreEqual(JsonValueKind.Object, results[2].GetValueKind());
            ClassicAssert.AreEqual("Val3", results[3].GetValue<string>());
            ClassicAssert.AreEqual("Val4", results[4].GetValue<string>());
            ClassicAssert.AreEqual("Val5", results[5].GetValue<string>());
            ClassicAssert.AreEqual("Val6", results[6].GetValue<string>());
        }

        [Test]
        public void ParseWithEmptyArrayContent()
        {
            var json = @"{
    ""controls"": [
        {
            ""messages"": {
                ""addSuggestion"": {
                    ""en-US"": ""Add""
                }
            }
        },
        {
            ""header"": {
                ""controls"": []
            },
            ""controls"": [
                {
                    ""controls"": [
                        {
                            ""defaultCaption"": {
                                ""en-US"": ""Sort by""
                            },
                            ""sortOptions"": [
                                {
                                    ""label"": {
                                        ""en-US"": ""Name""
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}";
            var jToken = JsonNode.Parse(json);
            var tokens = jToken.SelectNodes("$..en-US").ToList();

            ClassicAssert.AreEqual(3, tokens.Count);
            ClassicAssert.AreEqual("Add", tokens[0].ToString());
            ClassicAssert.AreEqual("Sort by", tokens[1].ToString());
            ClassicAssert.AreEqual("Name", tokens[2].ToString());
        }

        [Test]
        public void SelectTokenAfterEmptyContainer()
        {
            string json = @"{
    ""cont"": [],
    ""test"": ""no one will find me""
}";

            var o = JsonNode.Parse(json);

            var results = o.SelectNodes("$..test").ToList();

            ClassicAssert.AreEqual(1, results.Count);
            ClassicAssert.AreEqual("no one will find me", results[0].ToString());
        }

        [Test]
        public void EvaluatePropertyWithRequired()
        {
            string json = "{\"bookId\":\"1000\"}";
            var o = JsonNode.Parse(json);

            var bookId = o.TrySelectNode("bookId", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);

            ClassicAssert.IsTrue(bookId);
            ClassicAssert.AreEqual("1000", result.GetValue<string>());
        }

        [Test]
        public void EvaluateEmptyPropertyIndexer()
        {
            var o = JsonNode.Parse(@"{"""": 1}");

            var t = o.TrySelectNode("['']", out var result);
            ClassicAssert.IsTrue(t);
            ClassicAssert.AreEqual(1, result.GetValue<int>());
        }

        [Test]
        public void EvaluateEmptyString()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var success = o.TrySelectNode("", out var result);
            ClassicAssert.IsTrue(success);
            JsonAssert.AreEqual(o, result);

            success = o.TrySelectNode("['']", out result);
            ClassicAssert.IsFalse(success);
            ClassicAssert.IsNull(result);
        }

        [Test]
        public void EvaluateEmptyStringWithMatchingEmptyProperty()
        {
            var o = JsonNode.Parse(@"{"" "": 1}");

            var success = o.TrySelectNode("[' ']", out var result);
            ClassicAssert.IsTrue(success);
            ClassicAssert.AreEqual(1, result.GetValue<int>());
        }

        [Test]
        public void EvaluateWhitespaceString()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var success = o.TrySelectNode(" ", out var result);
            ClassicAssert.IsTrue(success);
            JsonAssert.AreEqual(o, result);
        }

        [Test]
        public void EvaluateDollarString()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var success = o.TrySelectNode("$", out var result);
            ClassicAssert.IsTrue(success);
            JsonAssert.AreEqual(o, result);
        }

        [Test]
        public void EvaluateDollarTypeString()
        {
            var o = JsonNode.Parse(@"{""$values"": [1, 2, 3]}");

            var t = o.TrySelectNode("$values[1]", out var result);
            ClassicAssert.IsTrue(t);
            ClassicAssert.AreEqual(2, result.GetValue<int>());
        }

        [Test]
        public void EvaluateSingleProperty()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var t = o.TrySelectNode("Blah", out var result);
            ClassicAssert.IsTrue(t);
            ClassicAssert.AreEqual(JsonValueKind.Number, result.GetValueKind());
            ClassicAssert.AreEqual(1, result.GetValue<int>());
        }

        [Test]
        public void EvaluateWildcardProperty()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1, ""Blah2"": 2}");

            var t = o.SelectNodes("$.*").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetValue<int>());
            ClassicAssert.AreEqual(2, t[1].GetValue<int>());
        }

        [Test]
        public void QuoteName()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var t = o.TrySelectNode("['Blah']", out var result);
            ClassicAssert.IsTrue(t);
            ClassicAssert.AreEqual(JsonValueKind.Number, result.GetValueKind());
            ClassicAssert.AreEqual(1, result.GetValue<int>());
        }

        [Test]
        public void EvaluateMissingProperty()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var success = o.TrySelectNode("Missing[1]", out var result);
            ClassicAssert.IsFalse(success);
            ClassicAssert.IsNull(result);
        }

        [Test]
        public void EvaluateIndexerOnObject()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var t = o.TrySelectNode("[1]", out var result);
            ClassicAssert.IsFalse(t);
        }

        [Test]
        public void EvaluateIndexerOnObjectWithError()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(
                () => { o.TrySelectNode("[1]", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result); },
                @"Index 1 not valid on JObject.");
        }


        [Test]
        public void EvaluateSliceOnObjectWithError()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(
                () => { o.TrySelectNode("[:]", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result); },
                @"Array slice is not valid on JObject.");
        }

        [Test]
        public void EvaluatePropertyOnArray()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            var t = a.TrySelectNode("BlahBlah", out var result);
            ClassicAssert.IsFalse(t);
        }

        [Test]
        public void EvaluateMultipleResultsError()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(() => { a.TrySelectNode("[0, 1]", out var result); },
                @"Path returned multiple tokens.");
        }

        [Test]
        public void EvaluatePropertyOnArrayWithError()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    a.TrySelectNode("BlahBlah", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);
                }, @"Property 'BlahBlah' not valid on JArray.");
        }

        [Test]
        public void EvaluateNoResultsWithMultipleArrayIndexes()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    a.TrySelectNode("[9,10]", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);
                }, @"Index 9 outside the bounds of JArray.");
        }

        [Test]
        public void EvaluateMissingPropertyWithError()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    o.TrySelectNode("Missing", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);
                }, "Property 'Missing' does not exist on JObject.");
        }

        [Test]
        public void EvaluatePropertyWithoutError()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            var v = o.TrySelectNode("Blah", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);
            ClassicAssert.IsTrue(v);
            ClassicAssert.AreEqual(1, result.GetValue<int>());
        }

        [Test]
        public void EvaluateMissingPropertyIndexWithError()
        {
            var o = JsonNode.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    o.TrySelectNode("['Missing','Missing2']", new JsonSelectSettings { ErrorWhenNoMatch = true },
                        out var result);
                }, "Property 'Missing' does not exist on JObject.");
        }

        [Test]
        public void EvaluateMultiPropertyIndexOnArrayWithError()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    a.TrySelectNode("['Missing','Missing2']", new JsonSelectSettings { ErrorWhenNoMatch = true },
                        out var result);
                }, "Properties 'Missing', 'Missing2' not valid on JArray.");
        }

        [Test]
        public void EvaluateArraySliceWithError()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(
                () => { a.TrySelectNode("[99:]", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result); },
                "Array slice of 99 to * returned no results.");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    a.TrySelectNode("[1:-19]", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);
                }, "Array slice of 1 to -19 returned no results.");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    a.TrySelectNode("[:-19]", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);
                }, "Array slice of * to -19 returned no results.");

            a = JsonNode.Parse(@"[]");

            ClassicAssert.Throws<JsonException>(
                () => { a.TrySelectNode("[:]", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result); },
                "Array slice of * to * returned no results.");
        }

        [Test]
        public void EvaluateOutOfBoundsIndxer()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            var t = a.TrySelectNode("[1000].Ha", out var result);
            ClassicAssert.IsFalse(t);
        }

        [Test]
        public void EvaluateArrayOutOfBoundsIndxerWithError()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(
                () =>
                {
                    a.TrySelectNode("[1000].Ha", new JsonSelectSettings { ErrorWhenNoMatch = true }, out var result);
                }, "Index 1000 outside the bounds of JArray.");
        }

        [Test]
        public void EvaluateArray()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4]");

            var t = a.TrySelectNode("[1]", out var result);
            ClassicAssert.IsTrue(t);
            ClassicAssert.AreEqual(JsonValueKind.Number, result.GetValueKind());
            ClassicAssert.AreEqual(2, result.GetValue<int>());
        }

        [Test]
        public void EvaluateArraySlice()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4, 5, 6, 7, 8, 9]");

            var t = a.SelectNodes("[-3:]").ToList();
            ClassicAssert.AreEqual(3, t.Count);
            ClassicAssert.AreEqual(7, t[0].GetValue<int>());
            ClassicAssert.AreEqual(8, t[1].GetValue<int>());
            ClassicAssert.AreEqual(9, t[2].GetValue<int>());

            t = [.. a.SelectNodes("[-1:-2:-1]")];
            ClassicAssert.AreEqual(1, t.Count);
            ClassicAssert.AreEqual(9, t[0].GetValue<int>());

            t = [.. a.SelectNodes("[-2:-1]")];
            ClassicAssert.AreEqual(1, t.Count);
            ClassicAssert.AreEqual(8, t[0].GetValue<int>());

            t = [.. a.SelectNodes("[1:1]")];
            ClassicAssert.AreEqual(0, t.Count);

            t = [.. a.SelectNodes("[1:2]")];
            ClassicAssert.AreEqual(1, t.Count);
            ClassicAssert.AreEqual(2, t[0].GetValue<int>());

            t = [.. a.SelectNodes("[::-1]")];
            ClassicAssert.AreEqual(9, t.Count);
            ClassicAssert.AreEqual(9, t[0].GetValue<int>());
            ClassicAssert.AreEqual(8, t[1].GetValue<int>());
            ClassicAssert.AreEqual(7, t[2].GetValue<int>());
            ClassicAssert.AreEqual(6, t[3].GetValue<int>());
            ClassicAssert.AreEqual(5, t[4].GetValue<int>());
            ClassicAssert.AreEqual(4, t[5].GetValue<int>());
            ClassicAssert.AreEqual(3, t[6].GetValue<int>());
            ClassicAssert.AreEqual(2, t[7].GetValue<int>());
            ClassicAssert.AreEqual(1, t[8].GetValue<int>());

            t = [.. a.SelectNodes("[::-2]")];
            ClassicAssert.AreEqual(5, t.Count);
            ClassicAssert.AreEqual(9, t[0].GetValue<int>());
            ClassicAssert.AreEqual(7, t[1].GetValue<int>());
            ClassicAssert.AreEqual(5, t[2].GetValue<int>());
            ClassicAssert.AreEqual(3, t[3].GetValue<int>());
            ClassicAssert.AreEqual(1, t[4].GetValue<int>());
        }

        [Test]
        public void EvaluateWildcardArray()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4]");

            var t = a.SelectNodes("[*]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(4, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetValue<int>());
            ClassicAssert.AreEqual(2, t[1].GetValue<int>());
            ClassicAssert.AreEqual(3, t[2].GetValue<int>());
            ClassicAssert.AreEqual(4, t[3].GetValue<int>());
        }

        [Test]
        public void EvaluateArrayMultipleIndexes()
        {
            var a = JsonNode.Parse(@"[1, 2, 3, 4]");

            var t = a.SelectNodes("[1,2,0]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(3, t.Count());
            ClassicAssert.AreEqual(2, t.ElementAt(0).GetValue<int>());
            ClassicAssert.AreEqual(3, t.ElementAt(1).GetValue<int>());
            ClassicAssert.AreEqual(1, t.ElementAt(2).GetValue<int>());
        }

        [Test]
        public void EvaluateScan()
        {
            var o1 = JsonNode.Parse(@"{""Name"": 1}");
            var o2 = JsonNode.Parse(@"{""Name"": 2}");
            var a = JsonNode.Parse(@"[" + o1.ToJsonString() + "," + o2.ToJsonString() + "]");

            var t = a.SelectNodes("$..Name").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetValue<int>());
            ClassicAssert.AreEqual(2, t[1].GetValue<int>());
        }

        [Test]
        public void EvaluateWildcardScan()
        {
            string json = @"[
                { ""Name"": 1 },
                { ""Name"": 2 }
            ]";
            var a = JsonNode.Parse(json);

            IList<JsonNode> t = [.. a.SelectNodes("$..*")];
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(4, t.Count);
            JsonAssert.AreEqual(a[0], t[0]);
            JsonAssert.AreEqual(a[1], t[2]);
        }

        [Test]
        public void EvaluateScanNestResults()
        {
            string json = @"[
                { ""Name"": 1 },
                { ""Name"": 2 },
                { ""Name"": { ""Name"": [3] } }
            ]";
            var a = JsonNode.Parse(json);

            IList<JsonNode> t = [.. a.SelectNodes("$..Name")];
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(4, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetValue<int>());
            ClassicAssert.AreEqual(2, t[1].GetValue<int>());
            JsonAssert.AreEqual(a[2]["Name"], t[2]);
            JsonAssert.AreEqual(a[2]["Name"]["Name"], t[3]);
        }

        [Test]
        public void EvaluateWildcardScanNestResults()
        {
            string json = @"[
                { ""Name"": 1 },
                { ""Name"": 2 },
                { ""Name"": { ""Name"": [3] } }
            ]";
            var a = JsonNode.Parse(json);

            IList<JsonNode> t = [.. a.SelectNodes("$..*")];
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(8, t.Count);

            JsonAssert.AreEqual(a[0], t[0]);
            JsonAssert.AreEqual(a[1], t[2]);
            JsonAssert.AreEqual(a[2], t[4]);
        }

        [Test]
        public void EvaluateSinglePropertyReturningArray()
        {
            var json = @"{""Blah"": [1, 2, 3]}";
            var o = JsonNode.Parse(json);

            var t = o.TrySelectNode("Blah", out var result);
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(JsonValueKind.Array, result.GetValueKind());

            t = o.TrySelectNode("Blah[2]", out result);
            ClassicAssert.AreEqual(JsonValueKind.Number, result.GetValueKind());
            ClassicAssert.AreEqual(3, result.GetValue<int>());
        }

        [Test]
        public void EvaluateLastSingleCharacterProperty()
        {
            var json = @"{""People"":[{""N"":""Jeff""}]}";
            var o2 = JsonNode.Parse(json);

            var a2 = o2.TrySelectNode("People[0].N", out var result);

            ClassicAssert.AreEqual("Jeff", result.GetValue<string>());
        }

        [Test]
        public void ExistsQuery()
        {
            var json = @"[{""hi"": ""ho""}, {""hi2"": ""ha""}]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( @.hi ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(1, t.Count);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": ""ho""}"), t[0]);
        }

        [Test]
        public void EqualsQuery()
        {
            var json = @"[{""hi"": ""ho""}, {""hi"": ""ha""}]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( @.['hi'] == 'ha' ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(1, t.Count);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": ""ha""}"), t[0]);
        }

        [Test]
        public void NotEqualsQuery()
        {
            var json = @"[[{""hi"": ""ho""}], [{""hi"": ""ha""}]]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( @..hi <> 'ha' ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(1, t.Count);
            JsonAssert.AreEqual(JsonNode.Parse(@"[{""hi"": ""ho""}]"), t[0]);
        }

        [Test]
        public void NoPathQuery()
        {
            var json = @"[1, 2, 3]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( @ > 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual(2, t[0].GetValue<int>());
            ClassicAssert.AreEqual(3, t[1].GetValue<int>());
        }

        [Test]
        public void MultipleQueries()
        {
            var json = @"[1, 2, 3, 4, 5, 6, 7, 8, 9]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[?(@ <> 1)][?(@ <> 4)][?(@ < 7)]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(0, t.Count);
        }

        [Test]
        public void GreaterQuery()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 3}]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( @.hi > 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 2}"), t[0]);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 3}"), t[1]);
        }

        [Test]
        public void LesserQuery_ValueFirst()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 3}]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( 1 < @.hi ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 2}"), t[0]);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 3}"), t[1]);
        }

        [Test]
        public void GreaterQueryBigInteger()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 3}]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( @.hi > 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 2}"), t[0]);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 3}"), t[1]);
        }

        [Test]
        public void GreaterOrEqualQuery()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 2.0}, {""hi"": 3}]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[ ?( @.hi >= 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(4, t.Count);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 1}"), t[0]);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 2}"), t[1]);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 2.0}"), t[2]);
            JsonAssert.AreEqual(JsonNode.Parse(@"{""hi"": 3}"), t[3]);
        }

        [Test]
        public void NestedQuery()
        {
            var json = @"[
                { ""name"": ""Bad Boys"", ""cast"": [{ ""name"": ""Will Smith"" }] },
                { ""name"": ""Independence Day"", ""cast"": [{ ""name"": ""Will Smith"" }] },
                { ""name"": ""The Rock"", ""cast"": [{ ""name"": ""Nick Cage"" }] }
            ]";
            var a = JsonNode.Parse(json);

            var t = a.SelectNodes("[?(@.cast[?(@.name=='Will Smith')])].name").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual("Bad Boys", t[0].GetValue<string>());
            ClassicAssert.AreEqual("Independence Day", t[1].GetValue<string>());
        }

        [Test]
        public void PathWithConstructor()
        {
            var json = @"[
                { ""Property1"": [1, [[[]]]] },
                { ""Property2"": [null, [1]] }
            ]";
            var a = JsonNode.Parse(json);

            var v = a.TrySelectNode("[1].Property2[1][0]", out var result);
            ClassicAssert.AreEqual(1, result.GetValue<int>());
        }

        [Test]
        public void MultiplePaths()
        {
            var json = @"[
                { ""price"": 199, ""max_price"": 200 },
                { ""price"": 200, ""max_price"": 200 },
                { ""price"": 201, ""max_price"": 200 }
            ]";
            var a = JsonNode.Parse(json);

            var results = a.SelectNodes("[?(@.price > @.max_price)]").ToList();
            ClassicAssert.AreEqual(1, results.Count);
            JsonAssert.AreEqual(a[2], results[0]);
        }

        [Test]
        public void Exists_True()
        {
            var json = @"[
                { ""price"": 199, ""max_price"": 200 },
                { ""price"": 200, ""max_price"": 200 },
                { ""price"": 201, ""max_price"": 200 }
            ]";
            var a = JsonNode.Parse(json);

            var results = a.SelectNodes("[?(true)]").ToList();
            ClassicAssert.AreEqual(3, results.Count);
            JsonAssert.AreEqual(a[0], results[0]);
            JsonAssert.AreEqual(a[1], results[1]);
            JsonAssert.AreEqual(a[2], results[2]);
        }

        [Test]
        public void Exists_Null()
        {
            var json = @"[
                { ""price"": 199, ""max_price"": 200 },
                { ""price"": 200, ""max_price"": 200 },
                { ""price"": 201, ""max_price"": 200 }
            ]";
            var a = JsonNode.Parse(json);

            var results = a.SelectNodes("[?(true)]").ToList();
            ClassicAssert.AreEqual(3, results.Count);
            JsonAssert.AreEqual(a[0], results[0]);
            JsonAssert.AreEqual(a[1], results[1]);
            JsonAssert.AreEqual(a[2], results[2]);
        }

        [Test]
        public void WildcardWithProperty()
        {
            var json = @"{
                ""station"": 92000041000001,
                ""containers"": [
                    {
                        ""id"": 1,
                        ""text"": ""Sort system"",
                        ""containers"": [
                            { ""id"": ""2"", ""text"": ""Yard 11"" },
                            { ""id"": ""92000020100006"", ""text"": ""Sort yard 12"" },
                            { ""id"": ""92000020100005"", ""text"": ""Yard 13"" }
                        ]
                    },
                    { ""id"": ""92000020100011"", ""text"": ""TSP-1"" },
                    { ""id"":""92000020100007"", ""text"": ""Passenger 15"" }
                ]
            }";
            var o = JsonNode.Parse(json);

            var tokens = o.SelectNodes("$..*[?(@.text)]").ToList();
            int i = 0;
            ClassicAssert.AreEqual("Sort system", tokens[i++]["text"].GetValue<string>());
            ClassicAssert.AreEqual("TSP-1", tokens[i++]["text"].GetValue<string>());
            ClassicAssert.AreEqual("Passenger 15", tokens[i++]["text"].GetValue<string>());
            ClassicAssert.AreEqual("Yard 11", tokens[i++]["text"].GetValue<string>());
            ClassicAssert.AreEqual("Sort yard 12", tokens[i++]["text"].GetValue<string>());
            ClassicAssert.AreEqual("Yard 13", tokens[i++]["text"].GetValue<string>());
            ClassicAssert.AreEqual(6, tokens.Count);
        }

        [Test]
        public void QueryAgainstNonStringValues()
        {
            var json = @"{
                ""prop"": [
                    { ""childProp"": ""ff2dc672-6e15-4aa2-afb0-18f4f69596ad"" },
                    { ""childProp"": ""ff2dc672-6e15-4aa2-afb0-18f4f69596ad"" },
                    { ""childProp"": ""http://localhost"" },
                    { ""childProp"": ""http://localhost"" },
                    { ""childProp"": ""2000-12-05T05:07:59Z"" },
                    { ""childProp"": ""2000-12-05T05:07:59Z"" },
                    { ""childProp"": ""2000-12-05T05:07:59-10:00"" },
                    { ""childProp"": ""2000-12-05T05:07:59-10:00"" },
                    { ""childProp"": ""SGVsbG8gd29ybGQ="" },
                    { ""childProp"": ""SGVsbG8gd29ybGQ="" },
                    { ""childProp"": ""365.23:59:59"" },
                    { ""childProp"": ""365.23:59:59"" }
                ]
            }";
            var o = JsonNode.Parse(json);

            var t = o.SelectNodes("$.prop[?(@.childProp =='ff2dc672-6e15-4aa2-afb0-18f4f69596ad')]").ToList();
            ClassicAssert.AreEqual(2, t.Count);

            t = [.. o.SelectNodes("$.prop[?(@.childProp =='http://localhost')]")];
            ClassicAssert.AreEqual(2, t.Count);

            t = [.. o.SelectNodes("$.prop[?(@.childProp =='2000-12-05T05:07:59Z')]")];
            ClassicAssert.AreEqual(2, t.Count);

            t = [.. o.SelectNodes("$.prop[?(@.childProp =='2000-12-05T05:07:59-10:00')]")];
            ClassicAssert.AreEqual(2, t.Count);

            t = [.. o.SelectNodes("$.prop[?(@.childProp =='SGVsbG8gd29ybGQ=')]")];
            ClassicAssert.AreEqual(2, t.Count);

            t = [.. o.SelectNodes("$.prop[?(@.childProp =='365.23:59:59')]")];
            ClassicAssert.AreEqual(2, t.Count);
        }

        [Test]
        public void Example()
        {
            var json = @"{
                ""Stores"": [
                    ""Lambton Quay"",
                    ""Willis Street""
                ],
                ""Manufacturers"": [
                    {
                        ""Name"": ""Acme Co"",
                        ""Products"": [
                            {
                                ""Name"": ""Anvil"",
                                ""Price"": 50
                            }
                        ]
                    },
                    {
                        ""Name"": ""Contoso"",
                        ""Products"": [
                            {
                                ""Name"": ""Elbow Grease"",
                                ""Price"": 99.95
                            },
                            {
                                ""Name"": ""Headlight Fluid"",
                                ""Price"": 4
                            }
                        ]
                    }
                ]
            }";

            var o = JsonNode.Parse(json);

            o.TrySelectNode("Manufacturers[0].Name", out var nameNode);
            string name = nameNode.GetValue<string>();
            // Acme Co

            o.TrySelectNode("Manufacturers[0].Products[0].Price", out var priceNode);
            decimal productPrice = priceNode.GetValue<decimal>();
            // 50

            o.TrySelectNode("Manufacturers[1].Products[0].Name", out var productNameNode);
            string productName = productNameNode.GetValue<string>();
            // Elbow Grease

            ClassicAssert.AreEqual("Acme Co", name);
            ClassicAssert.AreEqual(50m, productPrice);
            ClassicAssert.AreEqual("Elbow Grease", productName);

            o.TrySelectNode("Stores", out var storesNode);
            IList<string> storeNames = [.. ((JsonArray)storesNode).Select(s => s.GetValue<string>())];
            // Lambton Quay
            // Willis Street

            var manufacturers = (JsonArray)o["Manufacturers"];
            IList<string> firstProductNames =
            [
                .. manufacturers.Select(m =>
                {
                    m.AsObject().TrySelectNode("Products[1].Name", out var node);
                    return node?.GetValue<string>();
                })
            ];
            // null
            // Headlight Fluid

            decimal totalPrice = manufacturers.Sum(m =>
            {
                m.AsObject().TrySelectNode("Products[0].Price", out var node);
                return node.GetValue<decimal>();
            });
            // 149.95

            ClassicAssert.AreEqual(2, storeNames.Count);
            ClassicAssert.AreEqual("Lambton Quay", storeNames[0]);
            ClassicAssert.AreEqual("Willis Street", storeNames[1]);
            ClassicAssert.AreEqual(2, firstProductNames.Count);
            ClassicAssert.AreEqual(null, firstProductNames[0]);
            ClassicAssert.AreEqual("Headlight Fluid", firstProductNames[1]);
            ClassicAssert.AreEqual(149.95m, totalPrice);
        }

        [Test]
        public void NotEqualsAndNonPrimativeValues()
        {
            string json = @"[
        {
        ""name"": ""string"",
        ""value"": ""aString""
        },
        {
        ""name"": ""number"",
        ""value"": 123
        },
        {
        ""name"": ""array"",
        ""value"": [
        1,
        2,
        3,
        4
        ]
        },
        {
        ""name"": ""object"",
        ""value"": {
        ""1"": 1
        }
        }
        ]";

            var a = JsonNode.Parse(json);

            List<JsonNode> result = [.. a.SelectNodes("$.[?(@.value!=1)]")];
            ClassicAssert.AreEqual(4, result.Count);

            result = [.. a.SelectNodes("$.[?(@.value!='2000-12-05T05:07:59-10:00')]")];
            ClassicAssert.AreEqual(4, result.Count);

            result = [.. a.SelectNodes("$.[?(@.value!=null)]")];
            ClassicAssert.AreEqual(4, result.Count);

            result = [.. a.SelectNodes("$.[?(@.value!=123)]")];
            ClassicAssert.AreEqual(3, result.Count);

            result = [.. a.SelectNodes("$.[?(@.value)]")];
            ClassicAssert.AreEqual(4, result.Count);
        }

        [Test]
        public void RootInFilter()
        {
            string json = @"[
        {
        ""store"" : {
         ""book"" : [
            {
               ""category"" : ""reference"",
               ""author"" : ""Nigel Rees"",
               ""title"" : ""Sayings of the Century"",
               ""price"" : 8.95
            },
            {
               ""category"" : ""fiction"",
               ""author"" : ""Evelyn Waugh"",
               ""title"" : ""Sword of Honour"",
               ""price"" : 12.99
            },
            {
               ""category"" : ""fiction"",
               ""author"" : ""Herman Melville"",
               ""title"" : ""Moby Dick"",
               ""isbn"" : ""0-553-21311-3"",
               ""price"" : 8.99
            },
            {
               ""category"" : ""fiction"",
               ""author"" : ""J. R. R. Tolkien"",
               ""title"" : ""The Lord of the Rings"",
               ""isbn"" : ""0-395-19395-8"",
               ""price"" : 22.99
            }
         ],
         ""bicycle"" : {
            ""color"" : ""red"",
            ""price"" : 19.95
         }
        },
        ""expensive"" : 10
        }
        ]";

            var a = JsonNode.Parse(json);

            List<JsonNode> result = [.. a.SelectNodes("$.[?($.[0].store.bicycle.price < 20)]")];
            ClassicAssert.AreEqual(1, result.Count);

            result = [.. a.SelectNodes("$.[?($.[0].store.bicycle.price < 10)]")];
            ClassicAssert.AreEqual(0, result.Count);
        }

        [Test]
        public void RootInFilterWithRootObject()
        {
            string json = @"{
                ""store"" : {
                    ""book"" : [
                        {
                            ""category"" : ""reference"",
                            ""author"" : ""Nigel Rees"",
                            ""title"" : ""Sayings of the Century"",
                            ""price"" : 8.95
                        },
                        {
                            ""category"" : ""fiction"",
                            ""author"" : ""Evelyn Waugh"",
                            ""title"" : ""Sword of Honour"",
                            ""price"" : 12.99
                        },
                        {
                            ""category"" : ""fiction"",
                            ""author"" : ""Herman Melville"",
                            ""title"" : ""Moby Dick"",
                            ""isbn"" : ""0-553-21311-3"",
                            ""price"" : 8.99
                        },
                        {
                            ""category"" : ""fiction"",
                            ""author"" : ""J. R. R. Tolkien"",
                            ""title"" : ""The Lord of the Rings"",
                            ""isbn"" : ""0-395-19395-8"",
                            ""price"" : 22.99
                        }
                    ],
                    ""bicycle"" : [
                        {
                            ""color"" : ""red"",
                            ""price"" : 19.95
                        }
                    ]
                },
                ""expensive"" : 10
            }";

            var a = JsonNode.Parse(json);

            List<JsonNode> result = [.. a.SelectNodes("$..book[?(@.price <= $['expensive'])]")];
            ClassicAssert.AreEqual(2, result.Count);

            result = [.. a.SelectNodes("$.store..[?(@.price > $.expensive)]")];
            ClassicAssert.AreEqual(3, result.Count);
        }

        [Test]
        public void RootInFilterWithInitializers()
        {
            var json = @"{
                ""referenceDate"": ""0001-01-01T00:00:00Z"",
                ""dateObjectsArray"": [
                    { ""date"": ""0001-01-01T00:00:00Z"" },
                    { ""date"": ""9999-12-31T23:59:59.9999999Z"" },
                    { ""date"": ""2023-10-10T00:00:00Z"" },
                    { ""date"": ""0001-01-01T00:00:00Z"" }
                ]
            }";

            var rootObject = JsonNode.Parse(json);

            List<JsonNode> result = [.. rootObject.SelectNodes("$.dateObjectsArray[?(@.date == $.referenceDate)]")];
            ClassicAssert.AreEqual(2, result.Count);
        }

        [Test]
        public void IdentityOperator()
        {
            var json = @"{
             ""Values"": [{

                    ""Coercible"": 1,
                    ""Name"": ""Number""

                }, {
              ""Coercible"": ""1"",
              ""Name"": ""String""
             }]
            }";

            var o = JsonNode.Parse(json);

            // just to verify expected behavior hasn't changed
            IEnumerable<string> sanity1 =
                [.. o.SelectNodes("Values[?(@.Coercible == '1')].Name").Select(x => x.GetValue<string>())];
            IEnumerable<string> sanity2 =
                [.. o.SelectNodes("Values[?(@.Coercible != '1')].Name").Select(x => x.GetValue<string>())];
            // new behavior
            IEnumerable<string> mustBeNumber1 =
                [.. o.SelectNodes("Values[?(@.Coercible === 1)].Name").Select(x => x.GetValue<string>())];
            IEnumerable<string> mustBeString1 =
                [.. o.SelectNodes("Values[?(@.Coercible !== 1)].Name").Select(x => x.GetValue<string>())];
            IEnumerable<string> mustBeString2 =
                [.. o.SelectNodes("Values[?(@.Coercible === '1')].Name").Select(x => x.GetValue<string>())];
            IEnumerable<string> mustBeNumber2 =
                [.. o.SelectNodes("Values[?(@.Coercible !== '1')].Name").Select(x => x.GetValue<string>())];

            // FAILS-- JsonPath returns { "String" }
            //CollectionAssert.AreEquivalent(new[] { "Number", "String" }, sanity1);
            // FAILS-- JsonPath returns { "Number" }
            //Assert.IsTrue(!sanity2.Any());
            ClassicAssert.AreEqual("Number", mustBeNumber1.Single());
            ClassicAssert.AreEqual("String", mustBeString1.Single());
            ClassicAssert.AreEqual("Number", mustBeNumber2.Single());
            ClassicAssert.AreEqual("String", mustBeString2.Single());
        }

        [Test]
        public void QueryWithEscapedPath()
        {
            var json = @"{
        ""Property"": [
          {
            ""@Name"": ""x"",
            ""@Value"": ""y"",
            ""@Type"": ""FindMe""
          }
        ]
        }";

            var t = JsonNode.Parse(json);

            var tokens = t.SelectNodes("$..[?(@.['@Type'] == 'FindMe')]").ToList();
            ClassicAssert.AreEqual(1, tokens.Count);
        }

        [Test]
        public void Equals_FloatWithInt()
        {
            var json = @"{
        ""Values"": [
        {
        ""Property"": 1
        }
        ]
        }";

            var t = JsonNode.Parse(json);

            ClassicAssert.IsNotNull(t.SelectNodes(@"Values[?(@.Property == 1.0)]"));
        }

        [TestCaseSource(nameof(StrictMatchWithInverseTestData))]
        public void EqualsStrict(string value1, string value2, bool matchStrict)
        {
            value1 = value1.StartsWith('\'') ? $"\"{value1.Substring(1, value1.Length - 2)}\"" : value1;

            string completeJson = @"{
        ""Values"": [
        {
        ""Property"": " + value1 + @"
        }
        ]
        }";
            string completeEqualsStrictPath = "$.Values[?(@.Property === " + value2 + ")]";
            string completeNotEqualsStrictPath = "$.Values[?(@.Property !== " + value2 + ")]";

            var t = JsonNode.Parse(completeJson);

            bool hasEqualsStrict = t.SelectNodes(completeEqualsStrictPath).Any();
            ClassicAssert.AreEqual(
                matchStrict,
                hasEqualsStrict,
                $"Expected {value1} and {value2} to match: {matchStrict}"
                + Environment.NewLine + completeJson + Environment.NewLine + completeEqualsStrictPath);

            bool hasNotEqualsStrict = t.SelectNodes(completeNotEqualsStrictPath).Any();
            ClassicAssert.AreNotEqual(
                matchStrict,
                hasNotEqualsStrict,
                $"Expected {value1} and {value2} to match: {!matchStrict}"
                + Environment.NewLine + completeJson + Environment.NewLine + completeEqualsStrictPath);
        }

        public static IEnumerable<object[]> StrictMatchWithInverseTestData()
        {
            foreach (var item in StrictMatchTestData())
            {
                yield return new object[] { item[0], item[1], item[2] };

                if (!item[0].Equals(item[1]))
                {
                    // Test the inverse
                    yield return new object[] { item[1], item[0], item[2] };
                }
            }
        }

        private static IEnumerable<object[]> StrictMatchTestData()
        {
            yield return new object[] { "1", "1", true };
            yield return new object[] { "1", "1.0", true };
            yield return new object[] { "1", "true", false };
            yield return new object[] { "1", "'1'", false };
            yield return new object[] { "'1'", "'1'", true };
            yield return new object[] { "false", "false", true };
            yield return new object[] { "true", "false", false };
            yield return new object[] { "1", "1.1", false };
            yield return new object[] { "1", "null", false };
            yield return new object[] { "null", "null", true };
            yield return new object[] { "null", "'null'", false };
        }
    }
}