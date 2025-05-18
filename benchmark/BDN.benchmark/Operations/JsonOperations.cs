// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for ModuleOperations
    /// </summary>
    [MemoryDiagnoser]
    public class JsonOperations : OperationsBase
    {
        static ReadOnlySpan<byte> JSONGETCMD => "*3\r\n$8\r\nJSON.GET\r\n$2\r\nk3\r\n$1\r\n$\r\n"u8;
        static ReadOnlySpan<byte> JSONSETCMD => "*4\r\n$8\r\nJSON.SET\r\n$2\r\nk3\r\n$4\r\n$.f2\r\n$1\r\n2\r\n"u8;
        static ReadOnlySpan<byte> JSONGET_DEEP => "*3\r\n$8\r\nJSON.GET\r\n$4\r\nbig1\r\n$12\r\n$.data[0].id\r\n"u8;
        static ReadOnlySpan<byte> JSONGET_ARRAY => "*3\r\n$8\r\nJSON.GET\r\n$4\r\nbig1\r\n$13\r\n$.data[*]\r\n"u8;
        static ReadOnlySpan<byte> JSONGET_ARRAY_ELEMENTS => "*3\r\n$8\r\nJSON.GET\r\n$4\r\nbig1\r\n$13\r\n$.data[*].id\r\n"u8;
        static ReadOnlySpan<byte> JSONGET_FILTER => "*3\r\n$8\r\nJSON.GET\r\n$4\r\nbig1\r\n$29\r\n$.data[?(@.active==true)]\r\n"u8;
        static ReadOnlySpan<byte> JSONGET_RECURSIVE => "*3\r\n$8\r\nJSON.GET\r\n$4\r\nbig1\r\n$4\r\n$..*\r\n"u8;

        Request jsonGetCmd;
        Request jsonSetCmd;
        Request jsonGetDeepCmd;
        Request jsonGetArrayCmd;
        Request jsonGetArrayElementsCmd;
        Request jsonGetFilterCmd;
        Request jsonGetRecursiveCmd;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            RegisterModules();

            SetupOperation(ref jsonGetCmd, JSONGETCMD);
            SetupOperation(ref jsonSetCmd, JSONSETCMD);
            SetupOperation(ref jsonGetDeepCmd, JSONGET_DEEP);
            SetupOperation(ref jsonGetArrayCmd, JSONGET_ARRAY);
            SetupOperation(ref jsonGetArrayElementsCmd, JSONGET_ARRAY_ELEMENTS);
            SetupOperation(ref jsonGetFilterCmd, JSONGET_FILTER);
            SetupOperation(ref jsonGetRecursiveCmd, JSONGET_RECURSIVE);

            // Setup test data
            var largeJson = GenerateLargeJson(20);
            SlowConsumeMessage(Encoding.UTF8.GetBytes($"*4\r\n$8\r\nJSON.SET\r\n$4\r\nbig1\r\n$1\r\n$\r\n${largeJson.Length}\r\n{largeJson}\r\n"));

            // Existing setup
            SlowConsumeMessage("*4\r\n$8\r\nJSON.SET\r\n$2\r\nk3\r\n$1\r\n$\r\n$14\r\n{\"f1\":{\"a\":1}}\r\n"u8);
            SlowConsumeMessage(JSONGETCMD);
            SlowConsumeMessage(JSONSETCMD);
        }

        [Benchmark]
        public void ModuleJsonGetCommand()
        {
            Send(jsonGetCmd);
        }

        [Benchmark]
        public void ModuleJsonSetCommand()
        {
            Send(jsonSetCmd);
        }

        [Benchmark]
        public void ModuleJsonGetDeepPath()
        {
            Send(jsonGetDeepCmd);
        }

        [Benchmark]
        public void ModuleJsonGetArrayPath()
        {
            Send(jsonGetArrayCmd);
        }

        [Benchmark]
        public void ModuleJsonGetArrayElementsPath()
        {
            Send(jsonGetArrayElementsCmd);
        }

        [Benchmark]
        public void ModuleJsonGetFilterPath()
        {
            Send(jsonGetFilterCmd);
        }

        [Benchmark]
        public void ModuleJsonGetRecursive()
        {
            Send(jsonGetRecursiveCmd);
        }

        private static string GenerateLargeJson(int items)
        {
            var data = new StringBuilder();
            data.Append("{\"data\":[");

            for (int i = 0; i < items; i++)
            {
                if (i > 0) data.Append(',');
                data.Append($$"""
                    {
                        "id": {{i}},
                        "name": "Item{{i}}",
                        "active": {{(i % 2 == 0).ToString().ToLower()}},
                        "value": {{i * 100}},
                        "nested": {
                            "level1": {
                                "level2": {
                                    "value": {{i}}
                                }
                            }
                        }
                    }
                    """);
            }

            data.Append("]}");
            return data.ToString();
        }

        private void RegisterModules()
        {
            server.Register.NewModule(new GarnetJSON.JsonModule(), [], out _);
        }
    }
}