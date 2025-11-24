// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    internal static class CustomCommandUtils
    {
        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input">Object store input</param>
        /// <returns></returns>
        public static ReadOnlySpan<byte> GetFirstArg(ref ObjectInput input)
        {
            var idx = 0;
            return GetNextArg(ref input, ref idx);
        }

        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input">Main store input</param>
        /// <returns></returns>
        public static ReadOnlySpan<byte> GetFirstArg(ref StringInput input)
        {
            var idx = 0;
            return GetNextArg(ref input, ref idx);
        }

        /// <summary>
        /// Get argument from input, at specified index (starting from 0)
        /// </summary>
        /// <param name="input">Object store input</param>
        /// <param name="idx">Current argument index in input</param>
        /// <returns>Argument as a span</returns>
        public static ReadOnlySpan<byte> GetNextArg(ref ObjectInput input, scoped ref int idx)
        {
            var arg = idx < input.parseState.Count
                ? input.parseState.GetArgSliceByRef(idx).ReadOnlySpan
                : default;
            idx++;
            return arg;
        }

        /// <summary>
        /// Get argument from input, at specified index (starting from 0)
        /// </summary>
        /// <param name="input">Main store input</param>
        /// <param name="idx">Current argument index in input</param>
        /// <returns>Argument as a span</returns>
        public static ReadOnlySpan<byte> GetNextArg(ref StringInput input, scoped ref int idx)
        {
            var arg = idx < input.parseState.Count
                ? input.parseState.GetArgSliceByRef(idx).ReadOnlySpan
                : default;
            idx++;
            return arg;
        }
    }
}