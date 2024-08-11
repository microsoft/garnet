﻿using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Garnet;

[Generator]
public class EnumsSourceGenerator : ISourceGenerator
{
    const string GeneratedClassName = "EnumUtils";
    public void Initialize(GeneratorInitializationContext context)
    {
        context.RegisterForSyntaxNotifications(() => new EnumsSyntaxReceiver());
    }

    public void Execute(GeneratorExecutionContext context)
    {
        var syntaxReceiver = context.SyntaxReceiver as EnumsSyntaxReceiver;
        if (syntaxReceiver is null) return;

        foreach (var enumDeclaration in syntaxReceiver.Enums)
        {
            var enumName = enumDeclaration.Identifier.Text;

            var values = enumDeclaration.Members
                .Select(m => (
                    m.Identifier.Text,
                    Value: m.EqualsValue?.Value?.ToString(),
                    Description: m.AttributeLists
                        .SelectMany(al => al.Attributes).Where(a => a.Name.ToString() == "Description")
                        .SingleOrDefault()
                ))
                .ToList();

            var classBuilder = new StringBuilder();
            classBuilder.AppendLine("// <auto-generated>");
            classBuilder.AppendLine($"// This code was generated by the {nameof(EnumsSourceGenerator)} source generator.");

            classBuilder.AppendLine("using System;");
            classBuilder.AppendLine("using System.ComponentModel;");
            classBuilder.AppendLine("using System.Numerics;");
            classBuilder.AppendLine();
            classBuilder.AppendLine("namespace namespace Garnet;");
            classBuilder.AppendLine();
            classBuilder.AppendLine($"public static partial class {GeneratedClassName}");
            classBuilder.AppendLine("{");
            classBuilder.AppendLine(GenerateTryParseEnumFromDescriptionMethod(enumName, values));
            classBuilder.AppendLine();
            classBuilder.AppendLine(GenerateGetEnumDescriptionsMethod(enumName, values));
            classBuilder.AppendLine("}");
            var classSource = classBuilder.ToString();
            context.AddSource($"{GeneratedClassName}.{enumName}.g.cs", classSource);
        }
    }

    private static string GenerateTryParseEnumFromDescriptionMethod(string enumName, List<(string Name, string? Value, AttributeSyntax Description)> values)
    {
        var method = new StringBuilder();
        method.AppendLine($"    public static bool TryParse{enumName}FromDescription(string description, out {enumName} result)");
        method.AppendLine("    {");
        method.AppendLine("        result = default;");
        method.AppendLine("        switch (description)");
        method.AppendLine("        {");
        foreach (var (name, _, description) in values)
        {
            bool hasDescription = false;
            var descriptionValue = description?.ArgumentList?.Arguments.FirstOrDefault()?.ToString();
            if (descriptionValue is not null)
            {
                hasDescription = true;
                method.AppendLine($"            case {descriptionValue}:");
            }

            if (!hasDescription) continue;
            method.AppendLine($"                result = {enumName}.{name};");
            method.AppendLine("                return true;");
        }
        method.AppendLine("        }");
        method.AppendLine();
        method.AppendLine("        return false;");
        method.AppendLine("    }");
        method.AppendLine();

        return method.ToString();
    }
    private static string GenerateGetEnumDescriptionsMethod(string enumName, List<(string Name, string? Value, AttributeSyntax Description)> values)
    {
        var method = new StringBuilder();
        method.AppendLine($"    public static string[] Get{enumName}Descriptions({enumName} value)");
        method.AppendLine("    {");
        method.AppendLine("        var setFlags = BitOperations.PopCount((uint)value);");
        method.AppendLine("        if (setFlags == 0) return Array.Empty<string>();");
        method.AppendLine("        if (setFlags == 1)");
        method.AppendLine("        {");
        method.AppendLine("            return value switch");
        method.AppendLine("            {");
        foreach (var (name, _, description) in values)
        {
            if (description is null) continue;
            method.AppendLine($"                {enumName}.{name} => [ {description?.ArgumentList?.Arguments.FirstOrDefault()?.ToString()} ],");
        }
        method.AppendLine("                _ => Array.Empty<string>(),");
        method.AppendLine("            };");
        method.AppendLine("        }");
        method.AppendLine();
        method.AppendLine("        var descriptions = new string[setFlags];");
        method.AppendLine("        var index = 0;");
        foreach (var (name, _, description) in values)
        {
            if (description is null) continue;
            method.AppendLine($"        if ((value & {enumName}.{name}) != 0) descriptions[index++] = {description.ArgumentList?.Arguments.FirstOrDefault()?.ToString()};");
        }
        method.AppendLine();
        method.AppendLine("        return descriptions;");
        method.AppendLine("    }");

        return method.ToString();
    }

    private class EnumsSyntaxReceiver : ISyntaxReceiver
    {
        public List<EnumDeclarationSyntax> Enums { get; } = new();

        public void OnVisitSyntaxNode(SyntaxNode syntaxNode)
        {
            if (syntaxNode is EnumDeclarationSyntax enumDeclarationSyntax
                && enumDeclarationSyntax.AttributeLists.Any(al => al.Attributes.Any(a => a.Name.ToString() == "GenerateEnumUtils")))
            {
                Enums.Add(enumDeclarationSyntax);
            }
        }
    }
}