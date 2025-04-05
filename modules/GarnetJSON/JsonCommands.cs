// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using Garnet.common;
using Garnet.server;
using Garnet.server.Custom;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace GarnetJSON
{
    /// <summary>
    /// Represents a custom function to set JSON values in the Garnet object store.
    /// </summary>
    public class JsonSET : CustomObjectFunctions
    {
        private ILogger? logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSET"/> class.
        /// </summary>
        /// <param name="logger">The logger instance to use for logging.</param>
        public JsonSET(ILogger? logger = null) => this.logger = logger;

        /// <summary>
        /// Determines whether an initial update is needed.
        /// </summary>
        /// <param name="key">The key of the object.</param>
        /// <param name="input">The input data.</param>
        /// <param name="output">The output data.</param>
        /// <returns>Always returns true.</returns>
        public override bool NeedInitialUpdate(ReadOnlyMemory<byte> key, ref ObjectInput input, ref (IMemoryOwner<byte>, int) output) => true;

        /// <summary>
        /// Updates the JSON object with the specified key and input.
        /// </summary>
        /// <param name="key">The key of the object.</param>
        /// <param name="input">The input data.</param>
        /// <param name="jsonObject">The JSON object to update.</param>
        /// <param name="output">The output data.</param>
        /// <param name="rmwInfo">Additional information for the update.</param>
        /// <returns>True if the update is successful, otherwise false.</returns>
        public override bool Updater(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject jsonObject, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(jsonObject is GarnetJsonObject);

            var parseState = input.parseState;
            if (parseState.Count is not (2 or 3))
            {
                return AbortWithWrongNumberOfArguments(ref output, "json.set");
            }

            int offset = 0;
            var path = GetNextArg(ref input, ref offset);
            var value = GetNextArg(ref input, ref offset);
            var existOptions = ExistOptions.None;

            if (parseState.Count is 4 && !input.TryGetExistOption(ref offset, out existOptions))
            {
                return AbortWithSyntaxError(ref output);
            }

            var garnetJsonObject = jsonObject as GarnetJsonObject;
            Debug.Assert(garnetJsonObject is not null);

            var result = garnetJsonObject.Set(path, value, existOptions, out var errorMessage);

            switch (result)
            {
                case SetResult.Success:
                    return true;
                case SetResult.ConditionNotMet:
                    WriteNullBulkString(ref output);
                    break;
                default:
                    AbortWithErrorMessage(ref output, errorMessage);
                    break;
            }

            return true;
        }
    }

    /// <summary>
    /// Represents a custom function to get JSON values from the Garnet object store.
    /// </summary>
    public class JsonGET : CustomObjectFunctions
    {
        private ILogger? logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonGET"/> class.
        /// </summary>
        /// <param name="logger">The logger instance to use for logging.</param>
        public JsonGET(ILogger? logger = null) => this.logger = logger;

        /// <summary>
        /// Reads the JSON object with the specified key and input.
        /// </summary>
        /// <param name="key">The key of the object.</param>
        /// <param name="input">The input data.</param>
        /// <param name="jsonObject">The JSON object to read.</param>
        /// <param name="output">The output data.</param>
        /// <param name="readInfo">Additional information for the read operation.</param>
        /// <returns>True if the read is successful, otherwise false.</returns>
        public override bool Reader(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject jsonObject, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            Debug.Assert(jsonObject is GarnetJsonObject);
            var garnetJsonObject = jsonObject as GarnetJsonObject;
            Debug.Assert(garnetJsonObject is not null);

            var parseState = input.parseState;

            var outputArr = new List<byte[]>();
            var isSuccess = false;
            ReadOnlySpan<byte> errorMessage = default;
            if (parseState.Count == 0)
            {
                ReadOnlySpan<byte> path = default;
                isSuccess = garnetJsonObject.TryGet(path, outputArr, out errorMessage);
            }
            else
            {
                ReadOnlySpan<ArgSlice> paths = default;
                var offset = 0;
                string? indent = null;
                string? newLine = null;
                string? space = null;
                while (true)
                {
                    var option = GetNextArg(ref input, ref offset);
                    if (option.EqualsUpperCaseSpanIgnoringCase(JsonCmdStrings.INDENT) && offset < parseState.Count)
                    {
                        indent = GetNextString(ref input, ref offset);
                        continue;
                    }
                    else if (option.EqualsUpperCaseSpanIgnoringCase(JsonCmdStrings.NEWLINE) && offset < parseState.Count)
                    {
                        newLine = GetNextString(ref input, ref offset);
                        continue;
                    }
                    else if (option.EqualsUpperCaseSpanIgnoringCase(JsonCmdStrings.SPACE) && offset < parseState.Count)
                    {
                        space = GetNextString(ref input, ref offset);
                        continue;
                    }

                    if (offset > parseState.Count)
                    {
                        return AbortWithWrongNumberOfArguments(ref output, "json.get");
                    }
                    else
                    {
                        // If the code reached here then it means the current offset is a path, not an option
                        paths = parseState.Parameters.Slice(--offset);
                        break;
                    }
                }

                isSuccess = garnetJsonObject.TryGet(paths, outputArr, out errorMessage, indent, newLine, space);
            }

            if (!isSuccess)
            {
                AbortWithErrorMessage(ref output, errorMessage);
                return true;
            }

            if (outputArr.Count == 0)
            {
                WriteNullBulkString(ref output);
            }
            else
            {
                WriteBulkString(ref output, outputArr);
            }
            return true;
        }
    }
}