// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Garnet.analyzers
{
    /// <summary>
    /// Analyzer which flags uses of RespWriteUtils outside of the RespServerSessionOutput helper methods.
    /// 
    /// The intent is to force consistency in output for handling large outputs, error tracking, constant use, etc.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class AvoidRespWriteUtilsAnalyzer : DiagnosticAnalyzer
    {
        private static DiagnosticDescriptor AvoidRespWriteUtils { get; } = new("GARNET0001", "Avoid direct use of RespWriteUtils", "If possible use RespServerSession.{0} instead of RespWriteUtils.{1}", "Correctness", DiagnosticSeverity.Warning, isEnabledByDefault: true);

        /// <inheritdoc/>
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [AvoidRespWriteUtils];

        /// <inheritdoc/>
        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.ReportDiagnostics | GeneratedCodeAnalysisFlags.Analyze);
            context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(
                static compilationStartContext =>
                {
                    var respWriteUtilsType = compilationStartContext.Compilation.GetTypeByMetadataName("Garnet.common.RespWriteUtils");
                    var respServerSessionType = compilationStartContext.Compilation.GetTypeByMetadataName("Garnet.server.RespServerSession");

                    if (respWriteUtilsType is not null && respServerSessionType is not null)
                    {
                        var suggestionLookup = BuildLookup(respWriteUtilsType, respServerSessionType);

                        compilationStartContext.RegisterSyntaxNodeAction(syntaxNodeContext => AnalyzeCaller(syntaxNodeContext, respWriteUtilsType, suggestionLookup), SyntaxKind.InvocationExpression);
                    }
                }
            );

            // Build a map of TryWriteXXX methods on RespWriteUtils -> WriteXXX methods on RespServerSession
            static Dictionary<string, string> BuildLookup(INamedTypeSymbol respWriteUtilsType, INamedTypeSymbol respServerSessionType)
            {
                var tryWriteMethods = new HashSet<string>();

                foreach (var member in respWriteUtilsType.GetMembers())
                {
                    if (member is not IMethodSymbol mtdSymbol)
                    {
                        continue;
                    }

                    if (!member.Name.StartsWith("TryWrite"))
                    {
                        continue;
                    }

                    _ = tryWriteMethods.Add(member.Name);
                }

                var lookup = new Dictionary<string, string>();
                foreach (var member in respServerSessionType.GetMembers())
                {
                    if (member is not IMethodSymbol mtdSymbol)
                    {
                        continue;
                    }

                    // Check that method is declared in RespServerSessionOutput.cs, otherwise we don't consider it a candidate for flagging
                    if (!member.DeclaringSyntaxReferences.Any(static decl => Path.GetFileName(decl.SyntaxTree.FilePath) == "RespServerSessionOutput.cs"))
                    {
                        continue;
                    }

                    if (!member.Name.StartsWith("Write"))
                    {
                        continue;
                    }

                    var tryEquivalent = $"Try{member.Name}";
                    if (!tryWriteMethods.Contains(tryEquivalent))
                    {
                        continue;
                    }

                    lookup[tryEquivalent] = member.Name;
                }

                return lookup;
            }
        }

        /// <summary>
        /// Flag all calls to methods looking like RespWriteUtils.TryXXX
        /// </summary>
        private static void AnalyzeCaller(SyntaxNodeAnalysisContext context, INamedTypeSymbol respWriteUtilsType, Dictionary<string, string> candidateLookup)
        {
            if (context.Node is not InvocationExpressionSyntax invoke)
            {
                return;
            }

            if (invoke.Expression is not MemberAccessExpressionSyntax memberAccess)
            {
                return;
            }

            if (memberAccess.Name is not IdentifierNameSyntax methodName)
            {
                return;
            }

            // Quickly filter out anything that doesn't look like a TryXXX method
            if (string.IsNullOrEmpty(methodName.Identifier.Text) || !methodName.Identifier.Text.StartsWith("Try"))
            {
                return;
            }

            // Filter out anything in RespServerSessionOutput.cs, as it's intended to use these methods
            if (Path.GetFileName(context.Node.SyntaxTree.FilePath) == "RespServerSessionOutput.cs")
            {
                return;
            }

            // Ignore anything that isn't a call to RespWriteUtils
            var leftHandType = context.SemanticModel.GetSymbolInfo(memberAccess.Expression);
            if (leftHandType.Symbol is null || !SymbolEqualityComparer.Default.Equals(leftHandType.Symbol, respWriteUtilsType))
            {
                return;
            }

            // Lookup equivalent method, and raise diagnostic
            if (!candidateLookup.TryGetValue(methodName.Identifier.Text, out var rewriteTo))
            {
                return;
            }

            // Raise the actual diagnostic
            var diag = Diagnostic.Create(AvoidRespWriteUtils, context.Node.GetLocation(), rewriteTo, methodName.Identifier.Text);
            context.ReportDiagnostic(diag);
        }
    }
}
