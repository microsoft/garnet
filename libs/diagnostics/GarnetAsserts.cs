// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Garnet.Diagnostics
{
    public static class GarnetAsserts
    {
        [Conditional("DEBUG"), Conditional("GARNET_ASSERT")]
        public static void Assert([DoesNotReturnIf(false)] bool condition, string message = null, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message, expression, file, line);
        }

        [Conditional("DEBUG"), Conditional("GARNET_ASSERT")]
        public static void Assert([DoesNotReturnIf(false)] bool condition, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler message, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message.GetFormattedText(), expression, file, line);
        }

        [Conditional("DEBUG")]
        public static void DebugOnlyAssert([DoesNotReturnIf(false)] bool condition, string message = null, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message, expression, file, line);
        }

        [Conditional("DEBUG")]
        public static void DebugOnlyAssert([DoesNotReturnIf(false)] bool condition, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler message, [CallerArgumentExpression(nameof(condition))] string expression = null, [CallerFilePath] string file = null, [CallerLineNumber] int line = 0)
        {
            if (!condition)
                Fail(message.GetFormattedText(), expression, file, line);
        }

        // Do not make this helper conditional: a Release-built library must serve DEBUG callers too.
        [MethodImpl(MethodImplOptions.NoInlining)]
        [DoesNotReturn]
        private static void Fail(string message, string expression, string file, int line)
        {
            if (Debugger.IsAttached)
                Debugger.Break();

            Environment.FailFast($"Garnet assertion failed: {expression}\n{message}\n{file}:{line}");
        }
    }
}