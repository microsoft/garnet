// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server.Custom
{
    /// <summary>
    /// Provides extension methods for handling object input.
    /// </summary>
    public static class ObjectInputExtensions
    {
        /// <summary>
        /// Tries to get the expire option from the input.
        /// </summary>
        /// <param name="input">The input object containing the command arguments.</param>
        /// <param name="offset">The current offset in the input arguments.</param>
        /// <param name="value">The parsed expire option if successful, otherwise <see cref="ExistOptions.None"/>.</param>
        /// <returns>True if the expire option was successfully parsed, otherwise false.</returns>
        public static bool TryGetExistOption(this ref ObjectInput input, scoped ref int offset, out ExistOptions value)
        {
            value = ExistOptions.None;
            var existOptionStr = CustomCommandUtils.GetNextArg(ref input, ref offset);
            if (existOptionStr.EqualsUpperCaseSpanIgnoringCase(CmdStrings.NX))
            {
                value = ExistOptions.NX;
            }
            else if (existOptionStr.EqualsUpperCaseSpanIgnoringCase(CmdStrings.XX))
            {
                value = ExistOptions.XX;
            }
            else
            {
                return false;
            }

            return true;
        }
    }
}