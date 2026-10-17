// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Garnet.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class GarnetAssertsTests : TestBase
    {
        private static readonly MetadataReference[] References = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES"))
            .Split(Path.PathSeparator).Append(typeof(GarnetAsserts).Assembly.Location).Distinct()
            .Select(path => MetadataReference.CreateFromFile(path)).ToArray();

        [TestCase("DEBUG", 4)]
        [TestCase("GARNET_ASSERT", 2)]
        [TestCase("", 0)]
        public async Task ConditionalCallsAndInterpolation(string symbol, int expectedCalls)
        {
            var source = """
                using System;
                using static Garnet.Diagnostics.GarnetAsserts;
                int calls = 0;
                bool Condition() { calls++; return true; }
                string Unexpected() => throw new Exception("Successful assertions must not evaluate interpolation");
                Assert(Condition(), $"{Unexpected()}");
                DebugOnlyAssert(Condition(), $"{Unexpected()}");
                Assert(Condition(), $"{Unexpected()}", $"{Unexpected()}");
                DebugOnlyAssert(Condition(), $"{Unexpected()}", $"{Unexpected()}");
                #if DEBUG
                DebugOnlyAssert(DebugState.Value == 1);
                #endif
                Console.WriteLine(calls);
                #if DEBUG
                static class DebugState { public static int Value = 1; }
                #endif
                """;
            var result = await RunChild(source, symbol);
            Assert.That(result.ExitCode, Is.Zero, result.Error);
            Assert.That(result.Output.Trim(), Is.EqualTo(expectedCalls.ToString()));
        }

        [TestCase("DEBUG", "Assert(false, \"fatal-marker\", \"detail-marker\");")]
        [TestCase("GARNET_ASSERT", "Assert(false, \"fatal-marker\", \"detail-marker\");")]
        [TestCase("GARNET_ASSERT", "Assert(false, $\"fatal-{suffix} detail-{suffix}\");")]
        [TestCase("GARNET_ASSERT", "Assert(false, $\"fatal-{suffix}\", \"detail-marker\");")]
        [TestCase("DEBUG", "DebugOnlyAssert(false, \"fatal-marker\", \"detail-marker\");")]
        [TestCase("DEBUG", "DebugOnlyAssert(false, $\"fatal-{suffix} detail-{suffix}\");")]
        [TestCase("DEBUG", "DebugOnlyAssert(false, $\"fatal-{suffix}\", $\"detail-{suffix}\");")]
        [TestCase("GARNET_ASSERT", "Assert(false, $\"fatal-{suffix}\", $\"detail-{suffix}\");")]
        [TestCase("DEBUG", "Task.Run(() => Assert(false, $\"fatal-{suffix}\", $\"detail-{suffix}\")).GetAwaiter().GetResult();")]
        [TestCase("GARNET_ASSERT", "Task.Run(() => Assert(false, $\"fatal-{suffix}\", $\"detail-{suffix}\")).GetAwaiter().GetResult();")]
        public async Task FailureTerminatesEvenInsideExceptionHandlers(string symbol, string assertion)
        {
            var source = $$"""
                using System;
                using System.Threading.Tasks;
                using static Garnet.Diagnostics.GarnetAsserts;
                var suffix = "marker";
                try { {{assertion}} }
                catch { Console.WriteLine("caught"); }
                finally { Console.WriteLine("finally"); }
                Console.WriteLine("survived");
                """;
            var result = await RunChild(source, symbol);
            Assert.That(result.ExitCode, Is.Not.Zero);
            Assert.That(result.Error, Does.Contain("Garnet assertion failed: false").And.Contain("fatal-marker").And.Contain("detail-marker").And.Contain("AssertionProbe.cs:"));
            Assert.That(result.Output, Does.Not.Contain("caught").And.Not.Contain("finally").And.Not.Contain("survived"));
        }

        [TestCase("DEBUG")]
        [TestCase("")]
        public async Task BannedAnalyzerRejectsEveryDebugAssertOverloadAndAlias(string symbol)
        {
            var source = """
                using System.Diagnostics;
                using Alias = System.Diagnostics.Debug;
                using static System.Diagnostics.Debug;
                Debug.Assert(true);
                Alias.Assert(true, "message");
                System.Diagnostics.Debug.Assert(true, "message", "details");
                Assert(true, "message", "{0}", new object[] { 1 });
                Debug.Assert(true, $"message {1}");
                Debug.Assert(true, $"message {1}", $"details {2}");
                """;
            var compilation = Compile(source, symbol);
            Assert.That(compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error), Is.Empty);
            var assembly = Assembly.LoadFrom(Path.Combine(TestContext.CurrentContext.TestDirectory, "Microsoft.CodeAnalysis.CSharp.BannedApiAnalyzers.dll"));
            var analyzers = assembly.GetTypes().Where(t => !t.IsAbstract && typeof(DiagnosticAnalyzer).IsAssignableFrom(t))
                .Select(t => (DiagnosticAnalyzer)Activator.CreateInstance(t));
            var options = new AnalyzerOptions([new BannedSymbols()]);
            var diagnostics = await compilation.WithAnalyzers([.. analyzers], options).GetAnalyzerDiagnosticsAsync();
            Assert.That(diagnostics.Count(d => d.Id == "RS0030"), Is.EqualTo(6), string.Join(Environment.NewLine, diagnostics));
            Assert.That(diagnostics.Where(d => d.Id != "RS0030"), Is.Empty);
        }

        private static CSharpCompilation Compile(string source, string symbol) => CSharpCompilation.Create("AssertionProbe",
            [CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(preprocessorSymbols: symbol.Length == 0 ? [] : [symbol]), path: "AssertionProbe.cs")],
            References, new CSharpCompilationOptions(OutputKind.ConsoleApplication, optimizationLevel: OptimizationLevel.Release));

        private static async Task<(int ExitCode, string Output, string Error)> RunChild(string source, string symbol)
        {
            var directory = Directory.CreateTempSubdirectory("garnet-asserts-");
            try
            {
                var dll = Path.Combine(directory.FullName, "AssertionProbe.dll");
                var emitted = Compile(source, symbol).Emit(dll);
                Assert.That(emitted.Success, Is.True, string.Join(Environment.NewLine, emitted.Diagnostics));
                File.Copy(typeof(GarnetAsserts).Assembly.Location, Path.Combine(directory.FullName, "Garnet.Diagnostics.dll"));
                File.WriteAllText(Path.ChangeExtension(dll, ".runtimeconfig.json"),
                    $$"""{"runtimeOptions":{"tfm":"net{{Environment.Version.Major}}.0","framework":{"name":"Microsoft.NETCore.App","version":"{{Environment.Version.Major}}.{{Environment.Version.Minor}}.0"} } }""");
                var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
                start.ArgumentList.Add(dll);
                start.Environment["DOTNET_DbgEnableMiniDump"] = "0";
                start.Environment["COMPlus_DbgEnableMiniDump"] = "0";
                using var process = Process.Start(start);
                try
                {
                    var output = process.StandardOutput.ReadToEndAsync();
                    var error = process.StandardError.ReadToEndAsync();
                    using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(1));
                    await process.WaitForExitAsync(timeout.Token);
                    return (process.ExitCode, await output, await error);
                }
                finally
                {
                    if (!process.HasExited)
                    {
                        process.Kill(entireProcessTree: true);
                        await process.WaitForExitAsync();
                    }
                }
            }
            finally
            {
                directory.Delete(recursive: true);
            }
        }

        private sealed class BannedSymbols : AdditionalText
        {
            public override string Path => System.IO.Path.Combine(TestContext.CurrentContext.TestDirectory, "BannedSymbols.txt");
            public override SourceText GetText(CancellationToken cancellationToken = default) => SourceText.From(File.ReadAllText(Path));
        }
    }
}