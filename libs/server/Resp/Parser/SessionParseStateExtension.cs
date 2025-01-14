// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Extension methods for the SessionParseState struct.
    /// </summary>
    internal static class SessionParseStateExtension
    {
        /// <summary>
        /// Tries to extract keys from the key specifications in the given RespCommandsInfo.
        /// </summary>
        /// <param name="state">The SessionParseState instance.</param>
        /// <param name="keySpecs">The RespCommandKeySpecification array contains the key specification</param>
        /// <param name="keys">The list to store extracted keys.</param>
        /// <returns>True if keys were successfully extracted, otherwise false.</returns>
        internal static bool TryExtractKeysFromSpecs(this ref SessionParseState state, RespCommandKeySpecification[] keySpecs, out List<ArgSlice> keys)
        {
            keys = new();

            foreach (var spec in keySpecs)
            {
                if (!ExtractKeysFromSpec(ref state, keys, spec))
                {
                    return false;
                }
            }

            return true;
        }

        internal static bool TryExtractKeyandFlagsFromSpecs(this ref SessionParseState state, RespCommandKeySpecification[] keySpecs, out List<ArgSlice> keys, out List<string[]> flags)
        {
            keys = new();
            flags = new();

            foreach (var spec in keySpecs)
            {
                var prevKeyCount = keys.Count;
                if (!ExtractKeysFromSpec(ref state, keys, spec))
                {
                    return false;
                }

                var keyFlags = spec.RespFormatFlags;
                for (int i = prevKeyCount; i < keys.Count; i++)
                {
                    flags.Add(keyFlags);
                }
            }

            return true;
        }

        private static bool ExtractKeysFromSpec(ref SessionParseState state, List<ArgSlice> keys, RespCommandKeySpecification spec)
        {
            int startIndex = 0;

            if (spec.BeginSearch is BeginSearchIndex bsIndex)
            {
                startIndex = bsIndex.GetIndex(ref state);
            }
            else if (spec.BeginSearch is BeginSearchKeyword bsKeyword)
            {
                if (!bsKeyword.TryGetStartFrom(ref state, out startIndex))
                {
                    return false;
                }
            }

            if (startIndex < 0 || startIndex >= state.Count)
                return false;

            if (spec.FindKeys is FindKeysRange range)
            {
                range.ExtractKeys(ref state, startIndex, keys);
            }
            else if (spec.FindKeys is FindKeysKeyNum keyNum)
            {
                keyNum.ExtractKeys(ref state, startIndex, keys);
            }

            return true;
        }
    }
}