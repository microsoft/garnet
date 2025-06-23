// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics.CodeAnalysis;
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
            settings.AutoHelp = true;
            settings.CaseInsensitiveEnumValues = true;
            settings.GetoptMode = true;
            settings.IgnoreUnknownArguments = false;
            settings.CaseInsensitiveEnumValues = true;
        });

        var res = parser.ParseArguments<FuzzOptions>(args);

        if (res.Tag == ParserResultType.Parsed)
        {
            RunFuzzExample(res.Value);
        }
        else
        {
            var helpText =
                HelpText.AutoBuild(
                    res,
                    err =>
                    {
                        err.Heading = "Garnet.fuzz";
                        err.Copyright = "Copyright (c) Microsoft Corporation";
                        err.AddEnumValuesToHelpText = true;
                        return err;
                    }
                );

            Console.Write(helpText);
            Environment.Exit(ErrOptionsParse);
        }
    }

    /// <summary>
    /// Log, if options allow it, a line to standard out.
    /// </summary>
    private static void WriteLine(FuzzOptions opts, string line)
    {
        if (opts.Quiet)
        {
            return;
        }

        Console.WriteLine(line);
    }

    /// <summary>
    /// Log, if options allow it, a character to standard out.
    /// </summary>
    private static void Write(FuzzOptions opts, char c)
    {
        if (opts.Quiet)
        {
            return;
        }

        Console.Write(c);
    }

    /// <summary>
    /// Log a message to standard error and exit the process.
    /// </summary>
    [DoesNotReturn]
    private static void FailAndExit(string message, int exitCode)
    {
        Console.Error.WriteLine(message);
        Environment.Exit(exitCode);
    }

    /// <summary>
    /// Run the fuzz example.
    /// </summary>
    private static void RunFuzzExample(FuzzOptions opts)
    {
        var inputs = GetInput(opts);

        var target = GetTarget(opts);

        var repeatCount = opts.RepeatCount is null or <= 0 ? 1 : opts.RepeatCount.Value;

        foreach (var input in inputs)
        {
            for (var i = 0; i < repeatCount; i++)
            {
                target(input);
                if (i != repeatCount - 1)
                {
                    // Do some random allocations to shift things around for the next invocation
                    GC.KeepAlive(GC.AllocateUninitializedArray<byte>(Random.Shared.Next(512), pinned: false));
                    GC.KeepAlive(GC.AllocateUninitializedArray<byte>(Random.Shared.Next(512), pinned: true));
                }

                if (repeatCount != 1)
                {
                    Write(opts, '#');
                }
            }

            if (repeatCount != 1)
            {
                WriteLine(opts, "");
            }
        }

        // Obtain a callback to run a fuzz target.
        static FuzzTargetDelegate GetTarget(FuzzOptions opts)
        {
            var typeName = $"Garnet.fuzz.Targets.{opts.FuzzTarget}";
            var type = Type.GetType(typeName);

            if (type is null)
            {
                FailAndExit($"Could not load type: {typeName}", ErrTypeNotFound);
            }

            WriteLine(opts, $"Fuzz target class: {type.FullName}");

            var mtd = type.GetMethod(nameof(IFuzzerTarget.Fuzz), BindingFlags.Public | BindingFlags.Static);

            if (mtd is null)
            {
                FailAndExit($"Could not load method: {nameof(IFuzzerTarget.Fuzz)}", ErrMethodNotFound);
            }

            WriteLine(opts, $"Fuzz target method: {mtd.Name}");

            var del = (FuzzTargetDelegate)Delegate.CreateDelegate(typeof(FuzzTargetDelegate), null, mtd!);

            void WrappedDel(ReadOnlySpan<byte> input)
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
            }

            return WrappedDel;
        }

        // Load the given input into an array.
        static IEnumerable<byte[]> GetInput(FuzzOptions opts)
        {
            if (opts.UseStandardIn)
            {
                WriteLine(opts, "Reading from standard in (end input with ^Z):");
                yield return ReadSingleInput(Console.OpenStandardInput(), endOnCtrlZ: true, opts);
            }
            else if (opts.InputFile is not null)
            {
                WriteLine(opts, $"Reading file: {opts.InputFile.FullName}");
                yield return ReadSingleInput(opts.InputFile.OpenRead(), endOnCtrlZ: false, opts);
            }
            else if (opts.InputDirectory is not null)
            {
                var files = opts.InputDirectory.GetFiles();
                var count = files.Length;
                var n = 1;

                foreach (var file in files)
                {
                    WriteLine(opts, $"Reading file ({n:N0}/{count:N0}): {file.FullName}");

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
                using (stream)
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

                    WriteLine(opts, $"{content.Length:N0} bytes read");

                    return content;
                }
            }
        }
    }
}