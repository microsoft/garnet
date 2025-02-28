// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using StackExchange.Redis;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Test utility for accessing and inspecting segments of a response to an ACL GETUSER command.
    /// </summary>
    internal class UserAclResult
    {
        private const int CommandResultArrayLength = 6;

        /*
         * Use ordinal values when retrieving command results to ensure consistency of client contract.
         * Changes requiring modifications to these ordinal values are likely breaking for clients.
        */
        private const int FlagsPropertyNameIndex = 0;

        private const int FlagsPropertyValueIndex = 1;

        private const int PasswordPropertyNameIndex = 2;

        private const int PasswordPropertyValueIndex = 3;

        private const int CommandsPropertyNameIndex = 4;

        private const int CommandsPropertyValueIndex = 5;

        private readonly RedisResult rawResult;

        public bool IsEnabled { get; init; }

        public RedisResult[] PasswordResult { get; }

        public string PermittedCommands { get; }

        public UserAclResult(RedisResult result)
        {
            rawResult = result;
            IsEnabled = rawResult[FlagsPropertyValueIndex][0].ToString() == "on";
            PasswordResult = (RedisResult[])rawResult[PasswordPropertyValueIndex];
            PermittedCommands = rawResult[CommandsPropertyValueIndex].ToString();
        }

        /// <summary>
        /// Verifies the structure of the response of the ACL GETUSER command.
        /// </summary>
        /// <returns>true if the response is well structured; otherwise false</returns>
        public bool IsWellStructured()
        {
            return IsArray(rawResult)
                && rawResult.Length == CommandResultArrayLength
                && rawResult[FlagsPropertyNameIndex].ToString() == "flags"
                && rawResult[PasswordPropertyNameIndex].ToString() == "passwords"
                && rawResult[CommandsPropertyNameIndex].ToString() == "commands"
                && IsArray(rawResult[FlagsPropertyValueIndex])
                && IsValue(rawResult[FlagsPropertyValueIndex][0])
                && IsArray(rawResult[PasswordPropertyValueIndex])
                && IsValue(rawResult[CommandsPropertyValueIndex]);
        }

        /// <summary>
        /// Verifies that the provided password is present within the response of the ACL GETUSER command.
        /// </summary>
        /// <param name="expectedPasswordHash">The password has expected to be present within the result.</param>
        /// <returns>true if the provided password is present; otherwise false.</returns>
        public bool ContainsPasswordHash(string expectedPasswordHash)
        {
            bool found = false;
            foreach (RedisResult hash in PasswordResult)
            {
                if (hash.ToString() == expectedPasswordHash)
                {
                    found = true;
                }
            }

            return found;
        }

        private bool IsArray(RedisResult result)
        {
            return result.Length != -1;
        }

        private bool IsValue(RedisResult result)
        {
            return result.Length == -1;
        }
    }
}