// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Garnet.analyzers
{
    /// <summary>
    /// A number of analyzers in here to keep us honest about large responses and using CmdStrings
    ///  - Flag any call to WriteXXX methods in RespServerSessionOutput.cs which are variable size UNLESS they call a method with Large in its name
    ///  - Flag constants pass to WriteXXX methods which are not placed in CmdStrings classes
    ///  - Flag constants which are larger than minimum send buffer sizes
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class UseLargeOrConstantsForRespWritesAnalyzer : DiagnosticAnalyzer
    {
        private static DiagnosticDescriptor UseLargeOverridesWithVariableSizeResponses { get; } = new("GARNET0002", "Use Large Overrides With Variable Size Responses", "Use {0} instead of {1} for variable size responses", "Correctness", DiagnosticSeverity.Warning, isEnabledByDefault: true);
        private static DiagnosticDescriptor AddLargeOverridesForVariableSizeResponses { get; } = new("GARNET0003", "Add Large Override For Variable Size Responses", "Add and use {0} to RespServerSessionOutput.cs in place of using {1} for variable size responses", "Correctness", DiagnosticSeverity.Warning, isEnabledByDefault: true);
        private static DiagnosticDescriptor MoveOutputConstantsToCmdStrings { get; } = new("GARNET0004", "Move Output Constants To CmdStrings", "Add {0} to CmdStrings and use it in place of an inline literal", "Correctness", DiagnosticSeverity.Warning, isEnabledByDefault: true);
        private static DiagnosticDescriptor UseExistingConstantInCmdStrings { get; } = new("GARNET0005", "Use Existing Constants In CmdStrings", "Use CmdStrings.{0} instead of inline literal", "Correctness", DiagnosticSeverity.Warning, isEnabledByDefault: true);

        /// <inheritdoc/>
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [UseLargeOverridesWithVariableSizeResponses, AddLargeOverridesForVariableSizeResponses, MoveOutputConstantsToCmdStrings, UseExistingConstantInCmdStrings];

#pragma warning disable RS1026
        /// <inheritdoc/>
        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.ReportDiagnostics | GeneratedCodeAnalysisFlags.Analyze);
            //context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(
                static compilationStartContext =>
                {
                    var respServerSessionType = compilationStartContext.Compilation.GetTypeByMetadataName("Garnet.server.RespServerSession");
                    var cmdStringType = compilationStartContext.Compilation.GetTypeByMetadataName("Garnet.server.CmdStrings");

                    var byteType = compilationStartContext.Compilation.GetSpecialType(SpecialType.System_Byte);
                    var charType = compilationStartContext.Compilation.GetSpecialType(SpecialType.System_Char);
                    var stringType = compilationStartContext.Compilation.GetSpecialType(SpecialType.System_String);

                    var spanType = compilationStartContext.Compilation.GetTypeByMetadataName("System.Span`1");
                    var readOnlySpanType = compilationStartContext.Compilation.GetTypeByMetadataName("System.ReadOnlySpan`1");

                    var spanByteType = spanType.Construct(byteType);
                    var spanCharType = spanType.Construct(charType);

                    var readOnlySpanByteType = readOnlySpanType.Construct(byteType);
                    var readOnlySpanCharType = readOnlySpanType.Construct(charType);

                    var byteArrayType = compilationStartContext.Compilation.CreateArrayTypeSymbol(byteType);
                    var charArrayType = compilationStartContext.Compilation.CreateArrayTypeSymbol(charType);

                    HashSet<ITypeSymbol> variableLengthTypes =
                        new(SymbolEqualityComparer.Default)
                        {
                            spanByteType,
                            spanCharType,
                            readOnlySpanByteType,
                            readOnlySpanCharType,
                            stringType,
                            byteArrayType,
                            charArrayType,
                        };

                    if (respServerSessionType is not null && cmdStringType is not null)
                    {
                        var suggestionLookup = BuildWriteLargeLookup(respServerSessionType);

                        compilationStartContext.RegisterSyntaxNodeAction(
                            syntaxNodeContext =>
                            {
                                AnalyzeCallerForVariableSizeViolations(syntaxNodeContext, cmdStringType, variableLengthTypes, suggestionLookup, syntaxNodeContext.CancellationToken);
                            },
                            SyntaxKind.InvocationExpression
                        );

                        var knownConstants = BuildKnownConstantsLookup(cmdStringType, compilationStartContext.CancellationToken);

                        compilationStartContext.RegisterSyntaxNodeAction(
                            syntaxNodeAction =>
                            {
                                AnalyzeCallerForNonCmdStringsConstants(syntaxNodeAction, cmdStringType, respServerSessionType, knownConstants, syntaxNodeAction.CancellationToken);
                            },
                            SyntaxKind.InvocationExpression
                        );
                    }
                }
            );

            // Build a map of WriteXXX -> WriteLargeXXX methods on RespServerSession
            static Dictionary<string, string> BuildWriteLargeLookup(INamedTypeSymbol respServerSessionType)
            {
                var writeMethods = new HashSet<string>();
                var writeLargeMethods = new HashSet<string>();
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

                    if (member.Name.StartsWith("WriteLarge"))
                    {
                        _ = writeLargeMethods.Add(member.Name);
                    }
                    else if (member.Name.StartsWith("Write"))
                    {
                        _ = writeMethods.Add(member.Name);
                    }
                }

                var lookup = new Dictionary<string, string>();

                foreach (var largeMtd in writeLargeMethods)
                {
                    var writeEquiv = $"Write{largeMtd.Substring("WriteLarge".Length)}";

                    if (writeMethods.Contains(writeEquiv))
                    {
                        lookup[writeEquiv] = largeMtd;
                    }
                }

                return lookup;
            }

            // Build a map of literals ("foo" and "bar"u8) to the corresponding properties on CmdStrings
            static Dictionary<string, string> BuildKnownConstantsLookup(INamedTypeSymbol cmdStringsType, CancellationToken cancellation)
            {
                var lookup = new Dictionary<string, string>();

                foreach (var prop in cmdStringsType.GetMembers().OfType<IPropertySymbol>())
                {
                    if (prop.GetMethod is null || prop.SetMethod is not null)
                    {
                        continue;
                    }

                    var getDecl = prop.GetMethod.DeclaringSyntaxReferences.SingleOrDefault();
                    if (getDecl == null)
                    {
                        continue;
                    }

                    var getDeclSyntax = getDecl.GetSyntax(cancellation);
                    if (getDeclSyntax is not ArrowExpressionClauseSyntax arrowDecl)
                    {
                        continue;
                    }

                    if (arrowDecl.Expression is not LiteralExpressionSyntax literal)
                    {
                        continue;
                    }

                    lookup[literal.Token.Text] = prop.Name;
                }

                return lookup;
            }
        }

        /// <summary>
        /// Raises <see cref="UseLargeOverridesWithVariableSizeResponses"/> and <see cref="AddLargeOverridesForVariableSizeResponses"/>.
        /// 
        /// Finds any calls to WriteXXX where the input is variable length type AND not a constant, suggests uing WriteLargeXXX instead (or implementing it if it's not avaiable).
        /// </summary>
        private static void AnalyzeCallerForVariableSizeViolations(SyntaxNodeAnalysisContext context, INamedTypeSymbol cmdStringsType, HashSet<ITypeSymbol> variableLengthTypes, Dictionary<string, string> largeEquivLookup, CancellationToken cancellation)
        {
            if (context.Node is not InvocationExpressionSyntax invoke)
            {
                return;
            }

            if (invoke.Expression is not IdentifierNameSyntax methodName)
            {
                return;
            }

            // Quickly filter out anything that doesn't look like a plain Write method
            if (string.IsNullOrEmpty(methodName.Identifier.Text) || !methodName.Identifier.Text.StartsWith("Write") || methodName.Identifier.Text.StartsWith("WriteLarge"))
            {
                return;
            }

            // Ignore anything that isn't a call to a method in RespServerSessionOutput.cs
            var method = context.SemanticModel.GetSymbolInfo(methodName);
            if (method.Symbol is not IMethodSymbol methodSymbol || !method.Symbol.DeclaringSyntaxReferences.Any(static decl => Path.GetFileName(decl.SyntaxTree.FilePath) == "RespServerSessionOutput.cs"))
            {
                return;
            }

            // Skip anything without parameters or arguments
            if (methodSymbol.Parameters.Length == 0 || invoke.ArgumentList.Arguments.Count == 0)
            {
                return;
            }

            // Skip anything where the "to write" parameter isn't a variable length type
            var param0 = methodSymbol.Parameters[0];
            var isVariableLengthType = variableLengthTypes.Contains(param0.Type);
            if (!isVariableLengthType)
            {
                return;
            }

            // If the syntax tree or semantic thinks is a constant, just roll with it - other analyzers will refine this further
            var arg0 = invoke.ArgumentList.Arguments[0];
            var isConstant = arg0.Expression is LiteralExpressionSyntax || context.SemanticModel.GetConstantValue(arg0, cancellation).HasValue;
            if (isConstant)
            {
                return;
            }

            // Skip anything that references a CmdStrings property
            if (arg0.Expression is MemberAccessExpressionSyntax argMemberAccess)
            {
                var leftType = context.SemanticModel.GetSymbolInfo(argMemberAccess.Expression);
                var rightType = context.SemanticModel.GetSymbolInfo(argMemberAccess.Name);
                if (leftType.Symbol is INamedTypeSymbol type && SymbolEqualityComparer.Default.Equals(type, cmdStringsType) && rightType.Symbol is IPropertySymbol)
                {
                    return;
                }
            }

            // Raise the diagnostic, based on whether we have an existing override to use
            Diagnostic diag;
            if (largeEquivLookup.TryGetValue(methodName.Identifier.Text, out var largeMtdToUse))
            {
                diag = Diagnostic.Create(UseLargeOverridesWithVariableSizeResponses, methodName.GetLocation(), largeMtdToUse, methodName.Identifier.Text);
            }
            else
            {
                var largeEquivMtdName = $"WriteLarge{methodName.Identifier.Text.Substring("Write".Length)}";

                diag = Diagnostic.Create(AddLargeOverridesForVariableSizeResponses, methodName.GetLocation(), largeEquivMtdName, methodName.Identifier.Text);
            }

            context.ReportDiagnostic(diag);
        }

        /// <summary>
        /// Raises <see cref="MoveOutputConstantsToCmdStrings"/> and <see cref="UseExistingConstantInCmdStrings"/> if constants are used in WriteXXX methods which aren't on CmdStrings.
        /// </summary>
        private static void AnalyzeCallerForNonCmdStringsConstants(SyntaxNodeAnalysisContext context, INamedTypeSymbol cmdStringsType, INamedTypeSymbol respServerSession, Dictionary<string, string> knownConstants, CancellationToken cancellation)
        {
            if (context.Node is not InvocationExpressionSyntax invoke)
            {
                return;
            }

            if (invoke.Expression is not IdentifierNameSyntax methodName)
            {
                return;
            }

            // Quickly filter out anything that doesn't look like a plain Write method
            if (string.IsNullOrEmpty(methodName.Identifier.Text) || !methodName.Identifier.Text.StartsWith("Write"))
            {
                return;
            }

            // Ignore anything that isn't a call to a method in RespServerSessionOutput.cs
            var method = context.SemanticModel.GetSymbolInfo(methodName);
            if (method.Symbol is not IMethodSymbol methodSymbol || !method.Symbol.DeclaringSyntaxReferences.Any(static decl => Path.GetFileName(decl.SyntaxTree.FilePath) == "RespServerSessionOutput.cs"))
            {
                return;
            }

            // Skip anything without parameters or arguments
            if (methodSymbol.Parameters.Length == 0 || invoke.ArgumentList.Arguments.Count == 0)
            {
                return;
            }

            // If the first argument isn't a constant, ignore it
            string literalStr;
            var arg0 = invoke.ArgumentList.Arguments[0];
            if (arg0.Expression is LiteralExpressionSyntax literal)
            {
                literalStr = literal.Token.Text;
            }
            else
            {
                var inferredConstant = context.SemanticModel.GetConstantValue(arg0.Expression, cancellation);
                if (inferredConstant.HasValue && inferredConstant.Value is string constStr)
                {
                    literalStr = $"\"{constStr}\"u8";
                }
                else
                {
                    return;
                }
            }

            Diagnostic diag;
            if (knownConstants.TryGetValue(literalStr, out var propertyName))
            {
                diag = Diagnostic.Create(UseExistingConstantInCmdStrings, arg0.GetLocation(), propertyName);
            }
            else
            {
                diag = Diagnostic.Create(MoveOutputConstantsToCmdStrings, arg0.GetLocation(), literalStr);
            }

            context.ReportDiagnostic(diag);
        }
    }
}
