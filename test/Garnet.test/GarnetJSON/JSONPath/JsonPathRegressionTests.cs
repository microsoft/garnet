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

#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using GarnetJSON.JSONPath;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.JSONPath
{
    [TestFixture]
    public class JsonPathRegressionTests
    {
        public class RegressionTestQuery
        {
            public override string ToString() => id;

            public required string id { get; set; }
            public required string selector { get; set; }

            public bool? ordered { get; set; }
            public required JsonNode document { get; set; }
            public JsonNode? consensus { get; set; }

            [JsonPropertyName("not-found-consensus")]
            public string? not_found_consensus { get; set; }

            [JsonPropertyName("scalar-consensus")]
            public JsonNode? scalar_consensus { get; set; }

            [JsonPropertyName("scalar-not-found-consensus")]
            public string? scalar_not_found_consensus { get; set; }


            public JsonNode? ScalarOrConsensus => consensus ?? scalar_consensus;
        }

        internal record RegressionTestHolder(RegressionTestQuery[] queries);


        internal static IEnumerable<TestCaseData> LoadCases()
        {
            // JSON adapted from https://github.com/cburgmer/json-path-comparison/blob/master/regression_suite/regression_suite.yaml
            var holder = JsonSerializer.Deserialize<RegressionTestHolder>(
                File.OpenRead("GarnetJSON/JSONPath/RegressionSuite.json"));
            return holder!.queries.Select(x => new TestCaseData(x).SetArgDisplayNames(x.id, x.selector));
        }

        [Test, TestCaseSource(nameof(LoadCases))]
        public void TestRegression(RegressionTestQuery query)
        {
            try
            {
                JsonPath path = new JsonPath(query.selector);
                if (query.consensus == null && query.scalar_consensus == null && query.not_found_consensus == null &&
                    query.scalar_not_found_consensus == null)
                {
                    // no consensus, skipping
                    return;
                }

                var result = path.Evaluate(query.document, query.document, null).ToArray();
                if (result.Length == 0)
                {
                    ClassicAssert.IsTrue(
                        (query.consensus != null && query.consensus is JsonArray arr && arr.Count == 0) ||
                        query.not_found_consensus != null || query.scalar_not_found_consensus != null, query.id);
                }
                else if (result.Length == 1 && query.scalar_consensus != null)
                {
                    ClassicAssert.IsTrue(JsonNode.DeepEquals(query.scalar_consensus, result[0]));
                }
                else
                {
                    if (query.consensus is JsonArray array && array.Count == result.Length)
                    {
                        if (query.ordered ?? false)
                        {
                            ClassicAssert.IsTrue(
                                result.Select((node, idx) => (node, idx))
                                    .All(i => JsonNode.DeepEquals(i.node, array[i.idx])),
                                query.id);
                        }
                        else
                        {
                            var all_results_in_consensus =
                                result.All(x => array.Any(a => JsonNode.DeepEquals(x, a)));
                            var all_consensus_in_result =
                                array.All(x => result.Any(a => JsonNode.DeepEquals(x, a)));
                            ClassicAssert.IsTrue(all_results_in_consensus && all_consensus_in_result, query.id);
                        }
                    }
                    else if (query.consensus is JsonValue jv && jv.GetValue<string>() == "NOT_SUPPORTED")
                    {
                        Assert.Ignore();
                    }
                    else
                    {
                        ClassicAssert.IsTrue(false, query.id);
                    }
                }
            }
            catch (Exception ex)
            {
                if (query.consensus is JsonValue jv && jv.GetValue<string>() == "NOT_SUPPORTED")
                {
                    ClassicAssert.IsTrue(true, "{0} NOT SUPPORTED & Errors", query.id);
                }
                else if (query.consensus is null)
                {
                    Assert.Inconclusive($"{query.id} threw {ex.Message} but no consensus.");
                }
                else
                {
                    throw;
                }
            }
        }
    }
}