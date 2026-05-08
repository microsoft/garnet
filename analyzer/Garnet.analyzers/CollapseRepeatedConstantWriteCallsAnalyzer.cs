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
using Microsoft.CodeAnalysis.Text;

namespace Garnet.analyzers
{
    /// <summary>
    /// Flag cases where two or more sequential calls to WriteXXX helpers (defined in RespServerSession) where the arguments are constants.
    /// 
    /// IE.
    ///   WriteArrayLength(2)
    ///   WriteBulkString(CmdStrings.Foo)
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class CollapseRepeatedConstantWriteCallsAnalyzer : DiagnosticAnalyzer
    {
        private static DiagnosticDescriptor CollapseRepeatedConstantWriteCalls { get; } = new("GARNET0007", "Collapse Repeated Constant Write Calls", "Define a CmdString constant for these {0} consecutive WriteXXX helper calls", "Correctness", DiagnosticSeverity.Warning, isEnabledByDefault: true);

        /// <inheritdoc/>
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [CollapseRepeatedConstantWriteCalls];

        /// <inheritdoc/>
        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.ReportDiagnostics | GeneratedCodeAnalysisFlags.Analyze);
            context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(
                compilationStartContext =>
                {
                    var respServerSessionType = compilationStartContext.Compilation.GetTypeByMetadataName("Garnet.server.RespServerSession");
                    var cmdStringsType = compilationStartContext.Compilation.GetTypeByMetadataName("Garnet.server.CmdStrings");

                    if (respServerSessionType is not null && cmdStringsType is not null)
                    {
                        var lookup = BuildLookup(respServerSessionType);

                        compilationStartContext.RegisterSyntaxNodeAction(
                            syntaxNodeContext => AnalyzeCodeBlockForRepeatedConstantWriteCalls(syntaxNodeContext, cmdStringsType, lookup),
                            SyntaxKind.Block
                        );
                    }
                }
            );

            // Lookup all WriteXXX methods declared in RespServerSessionOutput.cs
            static HashSet<IMethodSymbol> BuildLookup(INamedTypeSymbol respServerSessionType)
            {
                var lookup = new HashSet<IMethodSymbol>(SymbolEqualityComparer.Default);
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

                    _ = lookup.Add(mtdSymbol);
                }

                return lookup;
            }
        }

        /// <summary>
        /// Raise the <see cref="CollapseRepeatedConstantWriteCalls"/> if 2 or more calls to methods in <paramref name="writeMethods"/> appear in a row with constant arguments.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("MicrosoftCodeAnalysisCorrectness", "RS1024:Symbols should be compared for equality", Justification = "writeMethods is constructed with the appropriate comparer")]
        private static void AnalyzeCodeBlockForRepeatedConstantWriteCalls(SyntaxNodeAnalysisContext blockAnalyzeContext, INamedTypeSymbol cmdStringsType, HashSet<IMethodSymbol> writeMethods)
        {
            if (blockAnalyzeContext.Node is not BlockSyntax blockSyntax)
            {
                return;
            }

            if (blockSyntax.Statements.Count <= 1)
            {
                return;
            }

            // Walk through all statements and find runs of 2+ constant calls to methods in writeMethods
            var constantWriteCalls = new List<StatementSyntax>();
            foreach (var statement in blockSyntax.Statements)
            {
                // Skip trivia
                if (statement.IsStructuredTrivia)
                {
                    continue;
                }

                // Only look at statements of the form:
                //    WriteXXX();
                if (statement is ExpressionStatementSyntax expressionSyntax && expressionSyntax.Expression is InvocationExpressionSyntax invocationSyntax && invocationSyntax.Expression is IdentifierNameSyntax mtdNameSyntax && mtdNameSyntax.Identifier.Text.StartsWith("Write"))
                {
                    var methodType = blockAnalyzeContext.SemanticModel.GetSymbolInfo(mtdNameSyntax, blockAnalyzeContext.CancellationToken);
                    if (methodType.Symbol is not null && writeMethods.Contains(methodType.Symbol) && IsConstantMethodCall(blockAnalyzeContext, invocationSyntax, cmdStringsType))
                    {
                        // This is a constant, remember it and move on
                        constantWriteCalls.Add(expressionSyntax);
                        continue;
                    }
                }

                // If we encountered something we _didn't_ remember then we need to raise diagnostics for the previously found statements
                if (constantWriteCalls.Count > 1)
                {
                    RaiseDiagnostic(blockAnalyzeContext, constantWriteCalls);
                }

                // A single statement cannot be collapsed
                if (constantWriteCalls.Count != 0)
                {
                    constantWriteCalls.Clear();
                }
            }

            // At the end of the block, flush any previously found statements
            if (constantWriteCalls.Count > 1)
            {
                RaiseDiagnostic(blockAnalyzeContext, constantWriteCalls);
            }

            // Determine if a call to the given WriteXXX method should be considered a constant call
            static bool IsConstantMethodCall(SyntaxNodeAnalysisContext blockAnalyzeContext, InvocationExpressionSyntax invocationSyntax, INamedTypeSymbol cmdStringsType)
            {
                foreach (var arg in invocationSyntax.ArgumentList.Arguments)
                {
                    if (arg.Expression is MemberAccessExpressionSyntax memberAccess)
                    {
                        var beingAccessed = blockAnalyzeContext.SemanticModel.GetSymbolInfo(memberAccess.Expression, blockAnalyzeContext.CancellationToken);
                        if (beingAccessed.Symbol is not null && SymbolEqualityComparer.Default.Equals(beingAccessed.Symbol, cmdStringsType))
                        {
                            continue;
                        }
                    }

                    var constantValue = blockAnalyzeContext.SemanticModel.GetConstantValue(arg.Expression);
                    if (constantValue.HasValue)
                    {
                        continue;
                    }

                    // Non-constant, can't collapse
                    return false;
                }

                // All constants (including no argument), we can collapse
                return true;
            }

            // Raise a diagnostic that covers all the consecutive statements passed
            static void RaiseDiagnostic(SyntaxNodeAnalysisContext blockAnalyzeContext, List<StatementSyntax> toFlag)
            {
                var start = toFlag[0].GetLocation();
                var end = toFlag[toFlag.Count - 1].GetLocation();

                var wholeSpan = new TextSpan(start.SourceSpan.Start, end.SourceSpan.End - start.SourceSpan.Start);
                var location = Location.Create(start.SourceTree, wholeSpan);

                var diag = Diagnostic.Create(CollapseRepeatedConstantWriteCalls, location, toFlag.Count);
                blockAnalyzeContext.ReportDiagnostic(diag);
            }
        }
    }
}
