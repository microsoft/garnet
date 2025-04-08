// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using CommandLine;
using CommandLine.Text;
using Garnet.fuzz;
using Garnet.fuzz.Targets;

public static class Program
{
    private const int ErrOptionsParse = -1;
    private const int ErrTypeNotFound = -2;
    private const int ErrMethodNotFound = -3;

    // Equivalent to Action<ReadOnlySpan<byte>>, but we can't write that in current C#
    private delegate void FuzzTargetDelegate(ReadOnlySpan<byte> input);

    /// <summary>
    /// This entry point is for validation and debugging purposes, fuzzing directly invokes the relevant classes.
    /// </summary>
    public static void Main(string[] args)
    {
        var parser = new Parser(settings =>
        {
            settings.AutoHelp = false;
            settings.CaseInsensitiveEnumValues = true;
            settings.GetoptMode = true;
        });


        var res = parser.ParseArguments<FuzzOptions>(args);

        if (res.Tag == ParserResultType.Parsed)
        {
            RunFuzzExample(res.Value);
        }
        else
        {
            var helpText = HelpText.AutoBuild(res);
            helpText.Heading = "Garnet.fuzz";
            helpText.Copyright = "Copyright (c) Microsoft Corporation";

            Console.Write(helpText);
            Environment.Exit(ErrOptionsParse);
        }
    }

    /// <summary>
    /// Run the fuzz example.
    /// </summary>
    private static void RunFuzzExample(FuzzOptions opts)
    {
        var inputs = GetInput(opts);

        var target = GetTarget(opts);

        foreach (var input in inputs)
        {
            target(input);
        }

        // Obtain a callback to run a fuzz target.
        static FuzzTargetDelegate GetTarget(FuzzOptions opts)
        {
            var typeName = $"Garnet.fuzz.Targets.{opts.FuzzTarget}";
            var type = Type.GetType(typeName);

            if (type is null)
            {
                Console.Error.WriteLine($"Could not load type: {typeName}");
                Environment.Exit(ErrTypeNotFound);
            }

            if (!opts.Quiet)
            {
                Console.WriteLine($"Fuzz target class: {type.FullName}");
            }

            var mtd = type.GetMethod(nameof(IFuzzerTarget.Fuzz), BindingFlags.Public | BindingFlags.Static);

            if (mtd is null)
            {
                Console.Error.WriteLine($"Could not load method: {nameof(IFuzzerTarget.Fuzz)}");
                Environment.Exit(ErrMethodNotFound);
            }

            if (!opts.Quiet)
            {
                Console.WriteLine($"Fuzz target method: {mtd.Name}");
            }

            var del = (FuzzTargetDelegate)Delegate.CreateDelegate(typeof(FuzzTargetDelegate), null, mtd!);

            FuzzTargetDelegate wrappedDel = (input) =>
            {
                try
                {
                    del(input);
                }
                catch (Exception e) when (e is not FuzzerValidationException)
                {
                    // Re-throw a wrapped error that captures input for ease of local debugging
                    //
                    // Centralized to DRY up IFuzzerTarget implementations
                    IFuzzerTarget.RaiseErrorForInput(e, input);
                }
            };

            return wrappedDel;
        }

        // Load the given input into an array.
        static IEnumerable<byte[]> GetInput(FuzzOptions opts)
        {
            if (opts.UseStandardIn)
            {
                if (!opts.Quiet)
                {
                    Console.WriteLine("Reading from standard in (end input with ^Z):");
                }
                yield return ReadSingleInput(Console.OpenStandardInput(), endOnCtrlZ: true, opts);
            }
            else if (opts.InputFile is not null)
            {
                if (!opts.Quiet)
                {
                    Console.WriteLine($"Reading file: {opts.InputFile.FullName}");
                }
                yield return ReadSingleInput(opts.InputFile.OpenRead(), endOnCtrlZ: false, opts);
            }
            else if (opts.InputDirectory is not null)
            {
                var files = opts.InputDirectory.GetFiles();
                var count = files.Length;
                var n = 1;

                foreach (var file in files)
                {
                    if (!opts.Quiet)
                    {
                        Console.WriteLine($"Reading file ({n:N0}/{count:N0}): {file.FullName}");
                    }

                    yield return ReadSingleInput(file.OpenRead(), endOnCtrlZ: false, opts);
                    n++;
                }
            }
            else
            {
                throw new InvalidOperationException("Unexpected input option configuration");
            }

            // Read input off of the given stream
            static byte[] ReadSingleInput(Stream stream, bool endOnCtrlZ, FuzzOptions opts)
            {
                var buff = new List<byte>(4 * 1_024);

                var into = new byte[1_024];

                int read;
                while ((read = stream.Read(into)) != 0)
                {
                    var readSpan = into.AsSpan()[..read];

                    // TODO: this is fairly Windows specific, how does Linux behave?
                    if (endOnCtrlZ && readSpan.Length >= 3 && readSpan[^3] == '\u001A')
                    {
                        // Trim the ^z\r\n
                        buff.AddRange(readSpan[..^3]);

                        break;
                    }

                    buff.AddRange(readSpan);
                }

                byte[] content = [.. buff];

                if (!opts.Quiet)
                {
                    Console.WriteLine($"{content.Length:N0} bytes read");
                }

                return content;
            }
        }
    }
}