// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Garnet.Diagnostics
{
    /// <summary>Fatal invariant checks enabled by DEBUG or GARNET_ASSERT at the call site.</summary>
    public static class GarnetAsserts
    {
        /// <summary>Breaks into an attached debugger, then terminates the process if the condition is false.</summary>
        [Conditional("DEBUG"), Conditional("GARNET_ASSERT")]
        public static void Assert([DoesNotReturnIf(false)] bool condition, string message = null, string detailMessage = null, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message, detailMessage, expression, file, line);
        }

        /// <summary>Checks an invariant without formatting its interpolated message on success.</summary>
        [Conditional("DEBUG"), Conditional("GARNET_ASSERT")]
        public static void Assert([DoesNotReturnIf(false)] bool condition, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler message, string detailMessage = null, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message.GetFormattedText(), detailMessage, expression, file, line);
        }

        /// <summary>Checks an invariant without formatting either interpolated message on success.</summary>
        [Conditional("DEBUG"), Conditional("GARNET_ASSERT")]
        public static void Assert([DoesNotReturnIf(false)] bool condition, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler message, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler detailMessage, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message.GetFormattedText(), detailMessage.GetFormattedText(), expression, file, line);
        }

        /// <summary>Checks DEBUG-only state; references to absent Release members still require #if DEBUG.</summary>
        [Conditional("DEBUG")]
        public static void DebugOnlyAssert([DoesNotReturnIf(false)] bool condition, string message = null, string detailMessage = null, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message, detailMessage, expression, file, line);
        }

        /// <summary>Checks DEBUG-only state without formatting its interpolated message on success.</summary>
        [Conditional("DEBUG")]
        public static void DebugOnlyAssert([DoesNotReturnIf(false)] bool condition, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler message, string detailMessage = null, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message.GetFormattedText(), detailMessage, expression, file, line);
        }

        /// <summary>Checks DEBUG-only state without formatting either interpolated message on success.</summary>
        [Conditional("DEBUG")]
        public static void DebugOnlyAssert([DoesNotReturnIf(false)] bool condition, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler message, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler detailMessage, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message.GetFormattedText(), detailMessage.GetFormattedText(), expression, file, line);
        }

        // Do not make this helper conditional: a Release-built library must serve DEBUG callers too.
        [MethodImpl(MethodImplOptions.NoInlining)]
        [DoesNotReturn]
        private static void Fail(string message, string detailMessage, string expression, string file, int line)
        {
            if (Debugger.IsAttached)
                Debugger.Break();

            Environment.FailFast($"Garnet assertion failed: {expression}\n{message}\n{detailMessage}\n{file}:{line}");
        }
    }

    /// <summary>Formats assertion messages only when the assertion fails.</summary>
    [InterpolatedStringHandler]
    public ref struct AssertMessageHandler
    {
        private DefaultInterpolatedStringHandler handler;

        /// <summary>Skips interpolation when the condition is true.</summary>
        public AssertMessageHandler(int literalLength, int formattedCount, bool condition, out bool shouldAppend)
        {
            shouldAppend = !condition;
            handler = shouldAppend ? new DefaultInterpolatedStringHandler(literalLength, formattedCount) : default;
        }

        /// <summary>Appends literal text.</summary>
        public void AppendLiteral(string value) => handler.AppendLiteral(value);

        /// <summary>Appends a formatted value.</summary>
        public void AppendFormatted<T>(T value) => handler.AppendFormatted(value);

        /// <summary>Appends a value with a format string.</summary>
        public void AppendFormatted<T>(T value, string format) => handler.AppendFormatted(value, format);

        /// <summary>Appends an aligned value.</summary>
        public void AppendFormatted<T>(T value, int alignment) => handler.AppendFormatted(value, alignment);

        /// <summary>Appends an aligned value with a format string.</summary>
        public void AppendFormatted<T>(T value, int alignment, string format) => handler.AppendFormatted(value, alignment, format);

        /// <summary>Appends a character span.</summary>
        public void AppendFormatted(ReadOnlySpan<char> value, int alignment = 0, string format = null) => handler.AppendFormatted(value, alignment, format);

        /// <summary>Appends text.</summary>
        public void AppendFormatted(string value) => handler.AppendFormatted(value);

        /// <summary>Appends aligned text with a format string.</summary>
        public void AppendFormatted(string value, int alignment = 0, string format = null) => handler.AppendFormatted(value, alignment, format);

        internal string GetFormattedText() => handler.ToStringAndClear();
    }
}