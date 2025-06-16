// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Garnet.fuzz.Targets
{
    /// <summary>
    /// Common interface for all fuzzer targets.
    /// </summary>
    internal interface IFuzzerTarget
    {
        /// <summary>
        /// Fuzzer entry point.
        /// 
        /// Crashes, exceptions, etc. should be allowed to bubble out.
        /// </summary>
        static abstract void Fuzz(ReadOnlySpan<byte> input);

        /// <summary>
        /// Move input onto the POH, and do any other work necessary for
        /// consistent fuzzing in Garnet.
        /// </summary>
        static void PrepareInput(ref ReadOnlySpan<byte> input)
        {
            var ret = GC.AllocateUninitializedArray<byte>(input.Length, pinned: true);
            input.CopyTo(ret.AsSpan());

            input = ret;
        }

        /// <summary>
        /// Helper for throwing an exception when some post-run validation failed.
        /// </summary>
        [DoesNotReturn]
        static void RaiseErrorForInput(string message, ReadOnlySpan<byte> input)
        {
            var inputAsText = Encoding.UTF8.GetString(input);
            var inputAsHex = Convert.ToHexString(input);

            throw new FuzzerValidationException($"{message}; length={input.Length}; text=\"{inputAsText}\"; hex={inputAsHex}");
        }

        /// <summary>
        /// Helper for throwing an exception when some post-run validation failed.
        /// </summary>
        [DoesNotReturn]
        static void RaiseErrorForInput(Exception e, ReadOnlySpan<byte> input)
        {
            var inputAsText = Encoding.UTF8.GetString(input);
            var inputAsHex = Convert.ToHexString(input);

            throw new FuzzerValidationException($"length={input.Length}; text=\"{inputAsText}\"; hex={inputAsHex}", e);
        }
    }

    /// <summary>
    /// Exception raised by helpers in <see cref="IFuzzerTarget"/>, just to enabling filtering elsewhere.
    /// </summary>
    internal sealed class FuzzerValidationException : Exception
    {
        internal FuzzerValidationException(string message) : this(message, null) { }

        internal FuzzerValidationException(string message, Exception? inner) : base(message, inner) { }
    }
}