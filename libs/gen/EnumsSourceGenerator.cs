﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.CodeDom.Compiler;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Garnet;

[Generator]
public class EnumsSourceGenerator : IIncrementalGenerator
{
    const string GeneratedClassName = "EnumUtils";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var enumDetails = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                "Garnet.common.GenerateEnumDescriptionUtilsAttribute",
                predicate: static (_, _) => true,
                transform: static (ctx, _) => TransformEnumDetails((EnumDeclarationSyntax)ctx.TargetNode, ctx.SemanticModel)
            );
        var enumUtils = enumDetails.Select(static (details, _) => Execute(details));

        context.RegisterSourceOutput(enumUtils, (ctx, source) => ctx.AddSource($"{GeneratedClassName}.{source.EnumName}.g.cs", source.ClassSource));
    }

    private static EnumDetails TransformEnumDetails(EnumDeclarationSyntax enumDeclaration, SemanticModel semanticModel)
    {
        var namespaceDeclaration = enumDeclaration.FirstAncestorOrSelf<NamespaceDeclarationSyntax>();
        var flags = enumDeclaration.AttributeLists.Where(al => al.Attributes.Any(a => a?.Name?.ToString() == "Flags")).Any();
        var enumName = enumDeclaration.Identifier.Text;
        var values = enumDeclaration.Members
            .Select(m => (
                m.Identifier.Text,
                Value: m.EqualsValue is not null ? semanticModel.GetOperation(m.EqualsValue.Value)!.ConstantValue.Value : null,
                Description: m.AttributeLists
                    .SelectMany(al => al.Attributes).Where(a => a.Name.ToString() == "Description")
                    .SingleOrDefault()?.ArgumentList?.Arguments.Single().ToString()
            ))
            .ToArray();

        return new EnumDetails(namespaceDeclaration!.Name.ToString(), enumName, flags, new EquatableArray<(string Name, object? Value, string? Description)>(values));
    }

    private static (string EnumName, string ClassSource) Execute(EnumDetails details)
    {
        using var classWriter = new IndentedTextWriter(new StringWriter());
        classWriter.WriteLine("// <auto-generated>");
        classWriter.WriteLine($"// This code was generated by the {nameof(EnumsSourceGenerator)} source generator.");
        classWriter.WriteLine("// </auto-generated>");

        classWriter.WriteLine("using System;");
        classWriter.WriteLine("using System.ComponentModel;");
        classWriter.WriteLine("using System.Numerics;");
        classWriter.WriteLine();
        classWriter.WriteLine($"namespace {details.Namespace};");
        classWriter.WriteLine();
        classWriter.WriteLine("/// <summary>");
        classWriter.WriteLine($"/// Utility methods for enums.");
        classWriter.WriteLine("/// </summary>");
        classWriter.WriteLine($"public static partial class {GeneratedClassName}");
        classWriter.WriteLine("{");
        classWriter.Indent++;
        GenerateTryParseEnumFromDescriptionMethod(classWriter, details);
        classWriter.WriteLine();
        GenerateGetEnumDescriptionsMethod(classWriter, details);
        classWriter.Indent--;
        classWriter.WriteLine("}");

        return (details.EnumName, classWriter.InnerWriter.ToString()!);
    }

    private static void GenerateTryParseEnumFromDescriptionMethod(IndentedTextWriter classWriter, EnumDetails details)
    {
        classWriter.WriteLine("/// <summary>");
        classWriter.WriteLine("/// Tries to parse the enum value from the description.");
        classWriter.WriteLine("/// </summary>");
        classWriter.WriteLine("/// <param name=\"description\">Enum description.</param>");
        classWriter.WriteLine("/// <param name=\"result\">Enum value.</param>");
        classWriter.WriteLine("/// <returns>True if successful.</returns>");
        classWriter.WriteLine($"public static bool TryParse{details.EnumName}FromDescription(string description, out {details.EnumName} result)");
        classWriter.WriteLine("{");
        classWriter.Indent++;
        classWriter.WriteLine("result = default;");
        classWriter.WriteLine("switch (description)");
        classWriter.WriteLine("{");
        classWriter.Indent++;
        foreach (var (name, _, description) in details.Values)
        {
            bool hasDescription = false;
            if (description is not null)
            {
                hasDescription = true;
                classWriter.WriteLine($"case {description}:");
            }

            if (!hasDescription) continue;
            classWriter.Indent++;
            classWriter.WriteLine($"result = {details.EnumName}.{name};");
            classWriter.WriteLine("return true;");
            classWriter.Indent--;
        }
        classWriter.Indent--;
        classWriter.WriteLine("}");
        classWriter.WriteLine();
        classWriter.WriteLine("return false;");
        classWriter.Indent--;
        classWriter.WriteLine("}");
        classWriter.WriteLine();
    }

    private static void GenerateGetEnumDescriptionsMethod(IndentedTextWriter classWriter, EnumDetails details)
    {
        classWriter.WriteLine("/// <summary>");
        classWriter.WriteLine("/// Gets the descriptions of the set flags. Assumes the enum is a flags enum.");
        classWriter.WriteLine("/// If no description exists, returns the ToString() value of the input value.");
        classWriter.WriteLine("/// </summary>");
        classWriter.WriteLine("/// <param name=\"value\">Enum value.</param>");
        classWriter.WriteLine("/// <returns>Array of descriptions.</returns>");
        classWriter.WriteLine($"public static string[] Get{details.EnumName}Descriptions({details.EnumName} value)");
        classWriter.WriteLine("{");
        classWriter.Indent++;
        if (details.Flags)
        {
            foreach (var (name, value, description) in details.Values)
            {
                if (!IsPow2(value))
                {
                    var toStringValue = description ?? $"\"{name}\"";
                    classWriter.WriteLine($"if (value is {details.EnumName}.{name}) return [{toStringValue}];");
                }
            }

            classWriter.WriteLine("var setFlags = BitOperations.PopCount((ulong)value);");
            classWriter.WriteLine("if (setFlags is 1)");
            classWriter.WriteLine("{");
            classWriter.Indent++;
        }

        classWriter.WriteLine("return value switch");
        classWriter.WriteLine("{");
        classWriter.Indent++;
        foreach (var (name, value, description) in details.Values)
        {
            if (description is null) continue;
            if (details.Flags && !IsPow2(value)) continue;
            classWriter.WriteLine($"{details.EnumName}.{name} => [{description}],");
        }
        classWriter.WriteLine($"_ => [value.ToString()],");
        classWriter.Indent--;
        classWriter.WriteLine("};");
        classWriter.Indent--;
        classWriter.WriteLine("}");
        classWriter.WriteLine();

        if (!details.Flags)
        {
            return;
        }

        classWriter.WriteLine("var descriptions = new string[setFlags];");
        classWriter.WriteLine("var index = 0;");
        foreach (var (name, value, description) in details.Values)
        {
            if (description is null) continue;
            if (!IsPow2(value)) continue;
            classWriter.WriteLine($"if ((value & {details.EnumName}.{name}) is not 0) descriptions[index++] = {description};");
        }
        classWriter.WriteLine();
        classWriter.WriteLine("return descriptions;");
        classWriter.Indent--;
        classWriter.WriteLine("}");
    }

    static bool IsPow2(object? value)
    {
        if (value is int x) return x != 0 && (x & (x - 1)) == 0;
        return false;
    }

    private record struct EnumDetails(string Namespace, string EnumName, bool Flags, EquatableArray<(string Name, object? Value, string? Description)> Values);
}