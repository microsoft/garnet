// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Garnet.analyzers
{
    /// <summary>
    /// Analyzer that flags test classes (annotated with [TestFixture]) that do not inherit from TestBase.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class TestBaseMissingAnalyzer : DiagnosticAnalyzer
    {
        private static DiagnosticDescriptor MissingTestBase { get; } = new(
            "GARNET0010",
            "Test class must inherit from TestBase",
            "Test class '{0}' should inherit from TestBase to enable running-test diagnostics",
            "Testing",
            DiagnosticSeverity.Warning,
            isEnabledByDefault: true);

        /// <inheritdoc/>
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
            [MissingTestBase];

        /// <inheritdoc/>
        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(
                compilationStartContext =>
                {
                    var testFixtureAttr = compilationStartContext.Compilation.GetTypeByMetadataName("NUnit.Framework.TestFixtureAttribute");
                    var testBaseType = compilationStartContext.Compilation.GetTypeByMetadataName("Garnet.test.TestBase");

                    if (testFixtureAttr is null || testBaseType is null)
                    {
                        return;
                    }

                    compilationStartContext.RegisterSymbolAction(
                        symbolContext => AnalyzeNamedType(symbolContext, testFixtureAttr, testBaseType),
                        SymbolKind.NamedType);
                });
        }

        private static void AnalyzeNamedType(
            SymbolAnalysisContext context,
            INamedTypeSymbol testFixtureAttr,
            INamedTypeSymbol testBaseType)
        {
            if (context.Symbol is not INamedTypeSymbol namedType)
            {
                return;
            }

            if (namedType.TypeKind != TypeKind.Class || namedType.IsAbstract)
            {
                return;
            }

            // Check if this class has [TestFixture]
            var hasTestFixture = namedType.GetAttributes()
                .Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, testFixtureAttr));

            if (!hasTestFixture)
            {
                return;
            }

            // Walk the base type chain to see if TestBase is anywhere in it
            var baseType = namedType.BaseType;
            while (baseType is not null)
            {
                if (SymbolEqualityComparer.Default.Equals(baseType, testBaseType))
                {
                    return;
                }

                baseType = baseType.BaseType;
            }

            // TestBase not found in hierarchy — report
            var diag = Diagnostic.Create(MissingTestBase, namedType.Locations[0], namedType.Name);
            context.ReportDiagnostic(diag);
        }
    }
}
