
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using GarnetJSON.JSONPath;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.JSONPath
{
    [TestFixture]
    public class JsonPathExecuteTests
    {
        [Test]
        public void GreaterThanIssue1518()
        {
            string statusJson = @"{""usingmem"": ""214376""}";//214,376
            var jObj = JsonDocument.Parse(statusJson);

            var aa = jObj.SelectElement("$..[?(@.usingmem>10)]");//found,10
            JsonAssert.AreEqual(jObj, aa);

            var bb = jObj.SelectElement("$..[?(@.usingmem>27000)]");//null, 27,000
            JsonAssert.AreEqual(jObj, bb);

            var cc = jObj.SelectElement("$..[?(@.usingmem>21437)]");//found, 21,437
            JsonAssert.AreEqual(jObj, cc);

            var dd = jObj.SelectElement("$..[?(@.usingmem>21438)]");//null,21,438
            JsonAssert.AreEqual(jObj, dd);
        }

        [Test]
        public void BacktrackingRegex_SingleMatch_TimeoutRespected()
        {
            const string RegexBacktrackingPattern = "(?<a>(.*?))[|].*(?<b>(.*?))[|].*(?<c>(.*?))[|].*(?<d>[1-3])[|].*(?<e>(.*?))[|].*[|].*[|].*(?<f>(.*?))[|].*[|].*(?<g>(.*?))[|].*(?<h>(.*))";
            
            var jObj = JsonDocument.Parse(@"[{""b"": ""15/04/2020 8:18:03 PM|1|System.String[]|3|Libero eligendi magnam ut inventore.. Quaerat et sit voluptatibus repellendus blanditiis aliquam ut.. Quidem qui ut sint in ex et tempore.|||.\\iste.cpp||46018|-1"" }]");

            ClassicAssert.Throws<RegexMatchTimeoutException>((() =>
            {
                Enumerable.ToArray<JsonElement>(jObj.SelectElements(
                    $"[?(@.b =~ /{RegexBacktrackingPattern}/)]",
                    new JsonSelectSettings
                    {
                        RegexMatchTimeout = TimeSpan.FromSeconds(0.01)
                    }));
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

            var models = JsonDocument.Parse(json);

            var results = models.SelectElements("$.persons[?(@.age > 3)]").ToList();

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

            var models = JsonDocument.Parse(json);

            var results = models.SelectElements("$.persons[?(@.age > '3')]").ToList();

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

            var models = JsonDocument.Parse(json);

            var results = models.SelectElements("$.b..*.id").ToList();

            ClassicAssert.AreEqual(3, results.Count);
            ClassicAssert.AreEqual(2, results[0].GetInt32());
            ClassicAssert.AreEqual(3, results[1].GetInt32());
            ClassicAssert.AreEqual(4, results[2].GetInt32());
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

            var models = JsonDocument.Parse(json);

            var results = models.SelectElements("$.elements..[?(@.id=='AAA')]").ToList();

            ClassicAssert.AreEqual(1, results.Count);
            JsonAssert.AreEqual(models.RootElement.GetProperty("elements")[0].GetProperty("children")[0].GetProperty("children")[0], results[0]);
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

            var models = JsonDocument.Parse(json);

            var results = models.SelectElements("$.elements[?(true)]").ToList();

            ClassicAssert.AreEqual(2, results.Count);
            JsonAssert.AreEqual(results[0], models.RootElement.GetProperty("elements")[0]);
            JsonAssert.AreEqual(results[1], models.RootElement.GetProperty("elements")[1]);
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

            var models = JsonDocument.Parse(json);

            var results = models.SelectElements("$.elements..[?(true)]").ToList();

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

            var models = JsonDocument.Parse(json);

            int result = models.SelectElements("$..['My.Child.Node']").Count();
            ClassicAssert.AreEqual(1, result);

            result = models.SelectElements("..['My.Child.Node']").Count();
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

            var models = JsonDocument.Parse(json);

            var results = models.SelectElements("$..['My.Child.Node','Prop1','Prop2']").ToList();
            ClassicAssert.AreEqual("Val1", results[0].GetString());
            ClassicAssert.AreEqual("Val2", results[1].GetString());
            ClassicAssert.AreEqual(JsonValueKind.Object, results[2].ValueKind);
            ClassicAssert.AreEqual("Val3", results[3].GetString());
            ClassicAssert.AreEqual("Val4", results[4].GetString());
            ClassicAssert.AreEqual("Val5", results[5].GetString());
            ClassicAssert.AreEqual("Val6", results[6].GetString());
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
            var jToken = JsonDocument.Parse(json);
            var tokens = jToken.SelectElements("$..en-US").ToList();

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

            var o = JsonDocument.Parse(json);

            var results = o.SelectElements("$..test").ToList();

            ClassicAssert.AreEqual(1, results.Count);
            ClassicAssert.AreEqual("no one will find me", results[0].ToString());
        }

        [Test]
        public void EvaluatePropertyWithRequired()
        {
            string json = "{\"bookId\":\"1000\"}";
            var o = JsonDocument.Parse(json);

            var bookId = o.SelectElement("bookId", new JsonSelectSettings { ErrorWhenNoMatch = true });

            ClassicAssert.IsTrue(bookId.HasValue);
            ClassicAssert.AreEqual("1000", bookId.Value.GetString());
        }

        [Test]
        public void EvaluateEmptyPropertyIndexer()
        {
            var o = JsonDocument.Parse(@"{"""": 1}");

            var t = o.SelectElement("['']");
            ClassicAssert.IsTrue(t.HasValue);
            ClassicAssert.AreEqual(1, t.Value.GetInt32());
        }

        [Test]
        public void EvaluateEmptyString()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var t = o.SelectElement("");
            JsonAssert.AreEqual(o, t);

            t = o.SelectElement("['']");
            ClassicAssert.IsNull(t);
        }

        [Test]
        public void EvaluateEmptyStringWithMatchingEmptyProperty()
        {
            var o = JsonDocument.Parse(@"{"" "": 1}");

            var t = o.SelectElement("[' ']");
            ClassicAssert.IsTrue(t.HasValue);
            ClassicAssert.AreEqual(1, t.Value.GetInt32());
        }

        [Test]
        public void EvaluateWhitespaceString()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var t = o.SelectElement(" ");
            JsonAssert.AreEqual(o, t);
        }

        [Test]
        public void EvaluateDollarString()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var t = o.SelectElement("$");
            JsonAssert.AreEqual(o, t);
        }

        [Test]
        public void EvaluateDollarTypeString()
        {
            var o = JsonDocument.Parse(@"{""$values"": [1, 2, 3]}");

            var t = o.SelectElement("$values[1]");
            ClassicAssert.IsTrue(t.HasValue);
            ClassicAssert.AreEqual(2, t.Value.GetInt32());
        }

        [Test]
        public void EvaluateSingleProperty()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var t = o.SelectElement("Blah");
            ClassicAssert.IsTrue(t.HasValue);
            ClassicAssert.AreEqual(JsonValueKind.Number, t.Value.ValueKind);
            ClassicAssert.AreEqual(1, t.Value.GetInt32());
        }

        [Test]
        public void EvaluateWildcardProperty()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1, ""Blah2"": 2}");

            var t = o.SelectElements("$.*").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetInt32());
            ClassicAssert.AreEqual(2, t[1].GetInt32());
        }

        [Test]
        public void QuoteName()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var t = o.SelectElement("['Blah']");
            ClassicAssert.IsTrue(t.HasValue);
            ClassicAssert.AreEqual(JsonValueKind.Number, t.Value.ValueKind);
            ClassicAssert.AreEqual(1, t.Value.GetInt32());
        }

        [Test]
        public void EvaluateMissingProperty()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var t = o.SelectElement("Missing[1]");
            ClassicAssert.IsNull(t);
        }

        [Test]
        public void EvaluateIndexerOnObject()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var t = o.SelectElement("[1]");
            ClassicAssert.IsNull(t);
        }

        [Test]
        public void EvaluateIndexerOnObjectWithError()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(() => { o.SelectElement("[1]", true); }, @"Index 1 not valid on JObject.");
        }

        [Test]
        public void EvaluateWildcardIndexOnObjectWithError()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(() => { o.SelectElement("[*]", true); }, @"Index * not valid on JObject.");
        }

        [Test]
        public void EvaluateSliceOnObjectWithError()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(() => { o.SelectElement("[:]", true); }, @"Array slice is not valid on JObject.");
        }

        [Test]
        public void EvaluatePropertyOnArray()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            var t = a.SelectElement("BlahBlah");
            ClassicAssert.IsNull(t);
        }

        [Test]
        public void EvaluateMultipleResultsError()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("[0, 1]"); }, @"Path returned multiple tokens.");
        }

        [Test]
        public void EvaluatePropertyOnArrayWithError()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("BlahBlah", true); }, @"Property 'BlahBlah' not valid on JArray.");
        }

        [Test]
        public void EvaluateNoResultsWithMultipleArrayIndexes()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("[9,10]", true); }, @"Index 9 outside the bounds of JArray.");
        }

        [Test]
        public void EvaluateMissingPropertyWithError()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(() => { o.SelectElement("Missing", true); }, "Property 'Missing' does not exist on JObject.");
        }

        [Test]
        public void EvaluatePropertyWithoutError()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            var v = o.SelectElement("Blah", true);
            ClassicAssert.IsTrue(v.HasValue);
            ClassicAssert.AreEqual(1, v.Value.GetInt32());
        }

        [Test]
        public void EvaluateMissingPropertyIndexWithError()
        {
            var o = JsonDocument.Parse(@"{""Blah"": 1}");

            ClassicAssert.Throws<JsonException>(() => { o.SelectElement("['Missing','Missing2']", true); }, "Property 'Missing' does not exist on JObject.");
        }

        [Test]
        public void EvaluateMultiPropertyIndexOnArrayWithError()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("['Missing','Missing2']", true); }, "Properties 'Missing', 'Missing2' not valid on JArray.");
        }

        [Test]
        public void EvaluateArraySliceWithError()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("[99:]", true); }, "Array slice of 99 to * returned no results.");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("[1:-19]", true); }, "Array slice of 1 to -19 returned no results.");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("[:-19]", true); }, "Array slice of * to -19 returned no results.");

            a = JsonDocument.Parse(@"[]");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("[:]", true); }, "Array slice of * to * returned no results.");
        }

        [Test]
        public void EvaluateOutOfBoundsIndxer()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            var t = a.SelectElement("[1000].Ha");
            ClassicAssert.IsNull(t);
        }

        [Test]
        public void EvaluateArrayOutOfBoundsIndxerWithError()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5]");

            ClassicAssert.Throws<JsonException>(() => { a.SelectElement("[1000].Ha", true); }, "Index 1000 outside the bounds of JArray.");
        }

        [Test]
        public void EvaluateArray()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4]");

            var t = a.SelectElement("[1]");
            ClassicAssert.IsTrue(t.HasValue);
            ClassicAssert.AreEqual(JsonValueKind.Number, t.Value.ValueKind);
            ClassicAssert.AreEqual(2, t.Value.GetInt32());
        }

        [Test]
        public void EvaluateArraySlice()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4, 5, 6, 7, 8, 9]");

            var t = a.SelectElements("[-3:]").ToList();
            ClassicAssert.AreEqual(3, t.Count);
            ClassicAssert.AreEqual(7, t[0].GetInt32());
            ClassicAssert.AreEqual(8, t[1].GetInt32());
            ClassicAssert.AreEqual(9, t[2].GetInt32());

            t = a.SelectElements("[-1:-2:-1]").ToList();
            ClassicAssert.AreEqual(1, t.Count);
            ClassicAssert.AreEqual(9, t[0].GetInt32());

            t = a.SelectElements("[-2:-1]").ToList();
            ClassicAssert.AreEqual(1, t.Count);
            ClassicAssert.AreEqual(8, t[0].GetInt32());

            t = a.SelectElements("[1:1]").ToList();
            ClassicAssert.AreEqual(0, t.Count);

            t = a.SelectElements("[1:2]").ToList();
            ClassicAssert.AreEqual(1, t.Count);
            ClassicAssert.AreEqual(2, t[0].GetInt32());

            t = a.SelectElements("[::-1]").ToList();
            ClassicAssert.AreEqual(9, t.Count);
            ClassicAssert.AreEqual(9, t[0].GetInt32());
            ClassicAssert.AreEqual(8, t[1].GetInt32());
            ClassicAssert.AreEqual(7, t[2].GetInt32());
            ClassicAssert.AreEqual(6, t[3].GetInt32());
            ClassicAssert.AreEqual(5, t[4].GetInt32());
            ClassicAssert.AreEqual(4, t[5].GetInt32());
            ClassicAssert.AreEqual(3, t[6].GetInt32());
            ClassicAssert.AreEqual(2, t[7].GetInt32());
            ClassicAssert.AreEqual(1, t[8].GetInt32());

            t = a.SelectElements("[::-2]").ToList();
            ClassicAssert.AreEqual(5, t.Count);
            ClassicAssert.AreEqual(9, t[0].GetInt32());
            ClassicAssert.AreEqual(7, t[1].GetInt32());
            ClassicAssert.AreEqual(5, t[2].GetInt32());
            ClassicAssert.AreEqual(3, t[3].GetInt32());
            ClassicAssert.AreEqual(1, t[4].GetInt32());
        }

        [Test]
        public void EvaluateWildcardArray()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4]");

            var t = a.SelectElements("[*]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(4, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetInt32());
            ClassicAssert.AreEqual(2, t[1].GetInt32());
            ClassicAssert.AreEqual(3, t[2].GetInt32());
            ClassicAssert.AreEqual(4, t[3].GetInt32());
        }

        [Test]
        public void EvaluateArrayMultipleIndexes()
        {
            var a = JsonDocument.Parse(@"[1, 2, 3, 4]");

            var t = a.SelectElements("[1,2,0]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(3, t.Count());
            ClassicAssert.AreEqual(2, t.ElementAt(0).GetInt32());
            ClassicAssert.AreEqual(3, t.ElementAt(1).GetInt32());
            ClassicAssert.AreEqual(1, t.ElementAt(2).GetInt32());
        }

        [Test]
        public void EvaluateScan()
        {
            var o1 = JsonDocument.Parse(@"{""Name"": 1}").RootElement;
            var o2 = JsonDocument.Parse(@"{""Name"": 2}").RootElement;
            var a = JsonDocument.Parse(@"[" + o1.GetRawText() + "," + o2.GetRawText() + "]").RootElement;

            var t = a.SelectElements("$..Name").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetInt32());
            ClassicAssert.AreEqual(2, t[1].GetInt32());
        }

        [Test]
        public void EvaluateWildcardScan()
        {
            string json = @"[
                { ""Name"": 1 },
                { ""Name"": 2 }
            ]";
            var a = JsonDocument.Parse(json);

            IList<JsonElement> t = a.SelectElements("$..*").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(5, t.Count);
            JsonAssert.AreEqual(a, t[0]);
            JsonAssert.AreEqual(a.RootElement[0], t[1]);
            JsonAssert.AreEqual(a.RootElement[1], t[3]);
        }

        [Test]
        public void EvaluateScanNestResults()
        {
            string json = @"[
                { ""Name"": 1 },
                { ""Name"": 2 },
                { ""Name"": { ""Name"": [3] } }
            ]";
            var a = JsonDocument.Parse(json);

            IList<JsonElement> t = a.SelectElements("$..Name").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(4, t.Count);
            ClassicAssert.AreEqual(1, t[0].GetInt32());
            ClassicAssert.AreEqual(2, t[1].GetInt32());
            JsonAssert.AreEqual(a.RootElement[2].GetProperty("Name"), t[2]);
            JsonAssert.AreEqual(a.RootElement[2].GetProperty("Name").GetProperty("Name"), t[3]);
        }

        [Test]
        public void EvaluateWildcardScanNestResults()
        {
            string json = @"[
                { ""Name"": 1 },
                { ""Name"": 2 },
                { ""Name"": { ""Name"": [3] } }
            ]";
            var a = JsonDocument.Parse(json);

            IList<JsonElement> t = a.SelectElements("$..*").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(9, t.Count);

            JsonAssert.AreEqual(a, t[0]);
            JsonAssert.AreEqual(a.RootElement[0], t[1]);
            JsonAssert.AreEqual(a.RootElement[1], t[3]);
            JsonAssert.AreEqual(a.RootElement[2], t[5]);
        }

        [Test]
        public void EvaluateSinglePropertyReturningArray()
        {
            var json = @"{""Blah"": [1, 2, 3]}";
            var o = JsonDocument.Parse(json);

            var t = o.SelectElement("Blah");
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(JsonValueKind.Array, t.Value.ValueKind);

            t = o.SelectElement("Blah[2]");
            ClassicAssert.AreEqual(JsonValueKind.Number, t.Value.ValueKind);
            ClassicAssert.AreEqual(3, t.Value.GetInt32());
        }

        [Test]
        public void EvaluateLastSingleCharacterProperty()
        {
            var json = @"{""People"":[{""N"":""Jeff""}]}";
            var o2 = JsonDocument.Parse(json);

            var a2 = o2.SelectElement("People[0].N").Value.GetString();

            ClassicAssert.AreEqual("Jeff", a2);
        }

        [Test]
        public void ExistsQuery()
        {
            var json = @"[{""hi"": ""ho""}, {""hi2"": ""ha""}]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( @.hi ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(1, t.Count);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": ""ho""}"), t[0]);
        }

        [Test]
        public void EqualsQuery()
        {
            var json = @"[{""hi"": ""ho""}, {""hi"": ""ha""}]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( @.['hi'] == 'ha' ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(1, t.Count);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": ""ha""}"), t[0]);
        }

        [Test]
        public void NotEqualsQuery()
        {
            var json = @"[[{""hi"": ""ho""}], [{""hi"": ""ha""}]]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( @..hi <> 'ha' ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(1, t.Count);
            JsonAssert.AreEqual(JsonDocument.Parse(@"[{""hi"": ""ho""}]"), t[0]);
        }

        [Test]
        public void NoPathQuery()
        {
            var json = @"[1, 2, 3]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( @ > 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual(2, t[0].GetInt32());
            ClassicAssert.AreEqual(3, t[1].GetInt32());
        }

        [Test]
        public void MultipleQueries()
        {
            var json = @"[1, 2, 3, 4, 5, 6, 7, 8, 9]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[?(@ <> 1)][?(@ <> 4)][?(@ < 7)]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(0, t.Count);
        }

        [Test]
        public void GreaterQuery()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 3}]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( @.hi > 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 2}"), t[0]);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 3}"), t[1]);
        }

        [Test]
        public void LesserQuery_ValueFirst()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 3}]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( 1 < @.hi ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 2}"), t[0]);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 3}"), t[1]);
        }

        [Test]
        public void GreaterQueryBigInteger()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 3}]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( @.hi > 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 2}"), t[0]);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 3}"), t[1]);
        }

        [Test]
        public void GreaterOrEqualQuery()
        {
            var json = @"[{""hi"": 1}, {""hi"": 2}, {""hi"": 2.0}, {""hi"": 3}]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[ ?( @.hi >= 1 ) ]").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(4, t.Count);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 1}"), t[0]);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 2}"), t[1]);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 2.0}"), t[2]);
            JsonAssert.AreEqual(JsonDocument.Parse(@"{""hi"": 3}"), t[3]);
        }

        [Test]
        public void NestedQuery()
        {
            var json = @"[
                { ""name"": ""Bad Boys"", ""cast"": [{ ""name"": ""Will Smith"" }] },
                { ""name"": ""Independence Day"", ""cast"": [{ ""name"": ""Will Smith"" }] },
                { ""name"": ""The Rock"", ""cast"": [{ ""name"": ""Nick Cage"" }] }
            ]";
            var a = JsonDocument.Parse(json);

            var t = a.SelectElements("[?(@.cast[?(@.name=='Will Smith')])].name").ToList();
            ClassicAssert.IsNotNull(t);
            ClassicAssert.AreEqual(2, t.Count);
            ClassicAssert.AreEqual("Bad Boys", t[0].GetString());
            ClassicAssert.AreEqual("Independence Day", t[1].GetString());
        }

        [Test]
        public void PathWithConstructor()
        {
            var json = @"[
                { ""Property1"": [1, [[[]]]] },
                { ""Property2"": [null, [1]] }
            ]";
            var a = JsonDocument.Parse(json);

            var v = a.SelectElement("[1].Property2[1][0]").Value;
            ClassicAssert.AreEqual(1, v.GetInt32());
        }

        [Test]
        public void MultiplePaths()
        {
            var json = @"[
                { ""price"": 199, ""max_price"": 200 },
                { ""price"": 200, ""max_price"": 200 },
                { ""price"": 201, ""max_price"": 200 }
            ]";
            var a = JsonDocument.Parse(json);

            var results = a.SelectElements("[?(@.price > @.max_price)]").ToList();
            ClassicAssert.AreEqual(1, results.Count);
            JsonAssert.AreEqual(a.RootElement[2], results[0]);
        }

        [Test]
        public void Exists_True()
        {
            var json = @"[
                { ""price"": 199, ""max_price"": 200 },
                { ""price"": 200, ""max_price"": 200 },
                { ""price"": 201, ""max_price"": 200 }
            ]";
            var a = JsonDocument.Parse(json);

            var results = a.SelectElements("[?(true)]").ToList();
            ClassicAssert.AreEqual(3, results.Count);
            JsonAssert.AreEqual(a.RootElement[0], results[0]);
            JsonAssert.AreEqual(a.RootElement[1], results[1]);
            JsonAssert.AreEqual(a.RootElement[2], results[2]);
        }

        [Test]
        public void Exists_Null()
        {
            var json = @"[
                { ""price"": 199, ""max_price"": 200 },
                { ""price"": 200, ""max_price"": 200 },
                { ""price"": 201, ""max_price"": 200 }
            ]";
            var a = JsonDocument.Parse(json);

            var results = a.SelectElements("[?(true)]").ToList();
            ClassicAssert.AreEqual(3, results.Count);
            JsonAssert.AreEqual(a.RootElement[0], results[0]);
            JsonAssert.AreEqual(a.RootElement[1], results[1]);
            JsonAssert.AreEqual(a.RootElement[2], results[2]);
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
            var o = JsonDocument.Parse(json);

            var tokens = o.SelectElements("$..*[?(@.text)]").ToList();
            int i = 0;
            ClassicAssert.AreEqual("Sort system", tokens[i++].GetProperty("text").GetString());
            ClassicAssert.AreEqual("TSP-1", tokens[i++].GetProperty("text").GetString());
            ClassicAssert.AreEqual("Passenger 15", tokens[i++].GetProperty("text").GetString());
            ClassicAssert.AreEqual("Yard 11", tokens[i++].GetProperty("text").GetString());
            ClassicAssert.AreEqual("Sort yard 12", tokens[i++].GetProperty("text").GetString());
            ClassicAssert.AreEqual("Yard 13", tokens[i++].GetProperty("text").GetString());
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
            var o = JsonDocument.Parse(json);

            var t = o.SelectElements("$.prop[?(@.childProp =='ff2dc672-6e15-4aa2-afb0-18f4f69596ad')]").ToList();
            ClassicAssert.AreEqual(2, t.Count);

            t = o.SelectElements("$.prop[?(@.childProp =='http://localhost')]").ToList();
            ClassicAssert.AreEqual(2, t.Count);

            t = o.SelectElements("$.prop[?(@.childProp =='2000-12-05T05:07:59Z')]").ToList();
            ClassicAssert.AreEqual(2, t.Count);

            t = o.SelectElements("$.prop[?(@.childProp =='2000-12-05T05:07:59-10:00')]").ToList();
            ClassicAssert.AreEqual(2, t.Count);

            t = o.SelectElements("$.prop[?(@.childProp =='SGVsbG8gd29ybGQ=')]").ToList();
            ClassicAssert.AreEqual(2, t.Count);

            t = o.SelectElements("$.prop[?(@.childProp =='365.23:59:59')]").ToList();
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

            var o = JsonDocument.Parse(json);

            string name = o.SelectElement("Manufacturers[0].Name").Value.GetString();
            // Acme Co

            decimal productPrice = o.SelectElement("Manufacturers[0].Products[0].Price").Value.GetDecimal();
            // 50

            string productName = o.SelectElement("Manufacturers[1].Products[0].Name").Value.GetString();
            // Elbow Grease

            ClassicAssert.AreEqual("Acme Co", name);
            ClassicAssert.AreEqual(50m, productPrice);
            ClassicAssert.AreEqual("Elbow Grease", productName);

            IList<string> storeNames = o.SelectElement("Stores").Value.EnumerateArray().Select(s => s.GetString()).ToList();
            // Lambton Quay
            // Willis Street

            IList<string> firstProductNames = o.RootElement.GetProperty("Manufacturers").EnumerateArray().Select(m => m.SelectElement("Products[1].Name") is JsonElement jsonElement ? jsonElement.GetString() : null).ToList();
            // null
            // Headlight Fluid

            decimal totalPrice = o.RootElement.GetProperty("Manufacturers").EnumerateArray().Sum(m => m.SelectElement("Products[0].Price").Value.GetDecimal());
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

            var a = JsonDocument.Parse(json);

            List<JsonElement> result = a.SelectElements("$.[?(@.value!=1)]").ToList();
            ClassicAssert.AreEqual(4, result.Count);

            result = a.SelectElements("$.[?(@.value!='2000-12-05T05:07:59-10:00')]").ToList();
            ClassicAssert.AreEqual(4, result.Count);

            result = a.SelectElements("$.[?(@.value!=null)]").ToList();
            ClassicAssert.AreEqual(4, result.Count);

            result = a.SelectElements("$.[?(@.value!=123)]").ToList();
            ClassicAssert.AreEqual(3, result.Count);

            result = a.SelectElements("$.[?(@.value)]").ToList();
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

            var a = JsonDocument.Parse(json);

            List<JsonElement> result = a.SelectElements("$.[?($.[0].store.bicycle.price < 20)]").ToList();
            ClassicAssert.AreEqual(1, result.Count);

            result = a.SelectElements("$.[?($.[0].store.bicycle.price < 10)]").ToList();
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

            var a = JsonDocument.Parse(json);

            List<JsonElement> result = a.SelectElements("$..book[?(@.price <= $['expensive'])]").ToList();
            ClassicAssert.AreEqual(2, result.Count);

            result = a.SelectElements("$.store..[?(@.price > $.expensive)]").ToList();
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

            var rootObject = JsonDocument.Parse(json);

            List<JsonElement> result = rootObject.SelectElements("$.dateObjectsArray[?(@.date == $.referenceDate)]").ToList();
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

            var o = JsonDocument.Parse(json);

            // just to verify expected behavior hasn't changed
            IEnumerable<string> sanity1 = o.SelectElements("Values[?(@.Coercible == '1')].Name").Select(x => x.GetString()).ToList();
            IEnumerable<string> sanity2 = o.SelectElements("Values[?(@.Coercible != '1')].Name").Select(x => x.GetString()).ToList();
            // new behavior
            IEnumerable<string> mustBeNumber1 = o.SelectElements("Values[?(@.Coercible === 1)].Name").Select(x => x.GetString()).ToList();
            IEnumerable<string> mustBeString1 = o.SelectElements("Values[?(@.Coercible !== 1)].Name").Select(x => x.GetString()).ToList();
            IEnumerable<string> mustBeString2 = o.SelectElements("Values[?(@.Coercible === '1')].Name").Select(x => x.GetString()).ToList();
            IEnumerable<string> mustBeNumber2 = o.SelectElements("Values[?(@.Coercible !== '1')].Name").Select(x => x.GetString()).ToList();

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

            var t = JsonDocument.Parse(json);

            var tokens = t.SelectElements("$..[?(@.['@Type'] == 'FindMe')]").ToList();
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

            var t = JsonDocument.Parse(json);

            ClassicAssert.IsNotNull(t.SelectElements(@"Values[?(@.Property == 1.0)]"));
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

            var t = JsonDocument.Parse(completeJson);

            bool hasEqualsStrict = t.SelectElements(completeEqualsStrictPath).Any();
            ClassicAssert.AreEqual(
                matchStrict,
                hasEqualsStrict,
                $"Expected {value1} and {value2} to match: {matchStrict}"
                + Environment.NewLine + completeJson + Environment.NewLine + completeEqualsStrictPath);

            bool hasNotEqualsStrict = t.SelectElements(completeNotEqualsStrictPath).Any();
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