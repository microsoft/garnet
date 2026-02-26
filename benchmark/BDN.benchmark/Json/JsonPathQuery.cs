// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using GarnetJSON.JSONPath;

namespace BDN.benchmark.Json;

[MemoryDiagnoser]
public class JsonPathQuery
{
    private JsonNode _jsonNode;

    private readonly Consumer _consumer = new Consumer();

    [Params(
        "$.store.book[0].title",
        "$.store.book[*].author",
        "$.store.book[?(@.price < 10)].title",
        "$.store.bicycle.color",
        "$.store.book[*]", // all books
        "$.store..price", // all prices using recursive descent
        "$..author", // all authors using recursive descent
        "$.store.book[?(@.price > 10 && @.price < 20)]", // filtered by price range
        "$.store.book[?(@.category == 'fiction')]", // filtered by category
        "$.store.book[-1:]", // last book
        "$.store.book[:2]", // first two books
        "$.store.book[?(@.author =~ /.*Waugh/)]", // regex match on author
        "$..book[0,1]", // union of array indices
        "$..*", // recursive descent all nodes
        "$..['bicycle','price']", // recursive descent specfic node with name match
        "$..[?(@.price < 10)]", // recursive descent specfic node with conditionally match
        "$.store.book[?(@.author && @.title)]", // existence check
        "$.store.*" // wildcard child
    )]
    public string JsonPath { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var jsonString = """
        {
            "store": {
                "book": [
                    {
                        "category": "reference",
                        "author": "Nigel Rees",
                        "title": "Sayings of the Century",
                        "price": 8.95
                    },
                    {
                        "category": "fiction",
                        "author": "Evelyn Waugh",
                        "title": "Sword of Honour",
                        "price": 12.99
                    }
                ],
                "bicycle": {
                    "color": "red",
                    "price": 19.95
                }
            }
        }
        """;

        _jsonNode = JsonNode.Parse(jsonString);
    }

    [Benchmark]
    public void SelectNodes()
    {
        var result = _jsonNode.SelectNodes(JsonPath);
        result.Consume(_consumer);
    }
}