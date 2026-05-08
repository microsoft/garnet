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
        private static DiagnosticDescriptor CmdStringsConstantTooLarge { get; } = new("GARNET0006", "CmdStrings Constant Too Large", "CmdStrings.{0} estimated serialization length of {1} exceeds minimum send buffer size of {2}", "Correctness", DiagnosticSeverity.Warning, isEnabledByDefault: true);

        /// <inheritdoc/>
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [UseLargeOverridesWithVariableSizeResponses, AddLargeOverridesForVariableSizeResponses, MoveOutputConstantsToCmdStrings, UseExistingConstantInCmdStrings, CmdStringsConstantTooLarge];

        /// <inheritdoc/>
        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.ReportDiagnostics | GeneratedCodeAnalysisFlags.Analyze);
            context.EnableConcurrentExecution();

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

                        var knownConstants = BuildKnownConstantsLookup(cmdStringType, readOnlySpanByteType, compilationStartContext.CancellationToken);

                        compilationStartContext.RegisterSyntaxNodeAction(
                            syntaxNodeAction =>
                            {
                                AnalyzeCallerForNonCmdStringsConstants(syntaxNodeAction, cmdStringType, respServerSessionType, knownConstants, syntaxNodeAction.CancellationToken);
                            },
                            SyntaxKind.InvocationExpression
                        );

                        compilationStartContext.RegisterSyntaxNodeAction(
                            syntaxNodeAction =>
                            {
                                AnalyzeDeclarationForTooLargeCmdStringConstants(syntaxNodeAction, cmdStringType, knownConstants);
                            },
                            SyntaxKind.ClassDeclaration
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
            static Dictionary<string, string> BuildKnownConstantsLookup(INamedTypeSymbol cmdStringsType, INamedTypeSymbol readOnlySpanByteType, CancellationToken cancellation)
            {
                var lookup = new Dictionary<string, string>();

                foreach (var prop in cmdStringsType.GetMembers().OfType<IPropertySymbol>())
                {
                    if (prop.GetMethod is null || prop.SetMethod is not null)
                    {
                        continue;
                    }

                    if (!SymbolEqualityComparer.Default.Equals(prop.Type, readOnlySpanByteType))
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
            if (arg0.Expression is LiteralExpressionSyntax literal && literal.Kind() is SyntaxKind.StringLiteralExpression or SyntaxKind.Utf8StringLiteralExpression)
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

        /// <summary>
        /// Checks for constants in CmdStrings that exceed CmdStrings.MaximumConstantSize in bytes.
        /// 
        /// Constants that start with a RESP sigil are assumed to be literal, everything else is treated like a Bulk String.
        /// </summary>
        private static void AnalyzeDeclarationForTooLargeCmdStringConstants(SyntaxNodeAnalysisContext context, INamedTypeSymbol cmdStringsType, Dictionary<string, string> constantLookup)
        {
            if (context.Node is not ClassDeclarationSyntax classDecl)
            {
                return;
            }

            // Quick check for CmdStrings name
            if (classDecl.Identifier.Text != "CmdStrings")
            {
                return;
            }

            // Check for CmdStrings type
            var declType = context.SemanticModel.GetDeclaredSymbol(classDecl);
            if (declType is null || !SymbolEqualityComparer.Default.Equals(cmdStringsType, declType))
            {
                return;
            }

            // Get the MaximumConstantSize property
            var maximumConstantSizeDecl = classDecl.Members.OfType<PropertyDeclarationSyntax>().SingleOrDefault(static propDecl => propDecl.Identifier.Text == "MaximumConstantSize");
            if (maximumConstantSizeDecl is null)
            {
                return;
            }

            // Get MaximumConstantSize literal value
            var getDeclSyntax = maximumConstantSizeDecl.ExpressionBody;
            if (getDeclSyntax is not ArrowExpressionClauseSyntax arrowDecl)
            {
                return;
            }

            if (arrowDecl.Expression is not LiteralExpressionSyntax literal)
            {
                return;
            }

            var maximumSizeLit = context.SemanticModel.GetConstantValue(literal);
            if (!maximumSizeLit.HasValue)
            {
                return;
            }

            var maximumSize = (int)maximumSizeLit.Value;

            // Consider all found constaints and calculate their effective length
            foreach (var kv in constantLookup)
            {
                var lit = kv.Key;
                var prop = kv.Value;

                if (!string.IsNullOrEmpty(lit) && lit[0] == '"')
                {
                    lit = lit.Substring(1);
                }

                if (!string.IsNullOrEmpty(lit) && lit.EndsWith("u8"))
                {
                    lit = lit.Substring(0, lit.Length - 2);
                }

                if (!string.IsNullOrEmpty(lit) && lit[lit.Length - 1] == '"')
                {
                    lit = lit.Substring(0, lit.Length - 1);
                }

                // Skip empty
                if (string.IsNullOrEmpty(lit))
                {
                    continue;
                }

                var maybeSigil = lit[0];
                var isRespEncoded = maybeSigil is '+' or '-' or ':' or '$' or '*' or '_' or '#' or ',' or '(' or '!' or '=' or '%' or '|' or '~' or '>';

                int effectiveLength;
                if (isRespEncoded)
                {
                    effectiveLength = lit.Replace("\\", "").Length;
                }
                else
                {
                    // Assume Bulk String: $<len>\r\n<value>\r\n
                    var rawLength = lit.Replace("\\", "").Length;
                    effectiveLength = 1 + CountDigits(rawLength) + 2 + rawLength + 2;
                }

                if (effectiveLength > maximumSize)
                {
                    var decl = classDecl.Members.OfType<PropertyDeclarationSyntax>().FirstOrDefault(p => p.Identifier.Text == prop);

                    if (decl is not null)
                    {
                        var diag = Diagnostic.Create(CmdStringsConstantTooLarge, decl.GetLocation(), prop, effectiveLength, maximumSize);
                        context.ReportDiagnostic(diag);
                    }
                }
            }

            // Figure out space needed to serialize a Bulk String length
            static int CountDigits(int value)
            {
                value = value < 0 ? ((~value) + 1) : value;

                if (value < 10) return 1;
                if (value < 100) return 2;
                if (value < 1000) return 3;
                if (value < 100000000L)
                {
                    if (value < 1000000)
                    {
                        if (value < 10000) return 4;
                        return 5 + (value >= 100000 ? 1 : 0);
                    }
                    return 7 + (value >= 10000000L ? 1 : 0);
                }
                return 9 + (value >= 1000000000L ? 1 : 0);
            }
        }
    }
}
