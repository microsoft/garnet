// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using System.Text;
using Garnet.common;
using Garnet.server;
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
                return output.AbortWithWrongNumberOfArguments("json.set");
            }

            int offset = 0;
            var path = CustomCommandUtils.GetNextArg(ref input, ref offset);
            var value = CustomCommandUtils.GetNextArg(ref input, ref offset);
            var existOptions = ExistOptions.None;

            if (parseState.Count is 4)
            {
                var existOptionStr = CustomCommandUtils.GetNextArg(ref input, ref offset);
                if (existOptionStr.EqualsUpperCaseSpanIgnoringCase(CmdStrings.NX))
                {
                    existOptions = ExistOptions.NX;
                }
                else if (existOptionStr.EqualsUpperCaseSpanIgnoringCase(CmdStrings.XX))
                {
                    existOptions = ExistOptions.XX;
                }
                else
                {
                    return output.AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }
            var result = ((GarnetJsonObject)jsonObject).Set(path, value, existOptions, out var errorMessage);

            switch (result)
            {
                case SetResult.Success:
                    WriteDirect(ref output, CmdStrings.RESP_OK);
                    break;
                case SetResult.ConditionNotMet:
                    WriteNullBulkString(ref output);
                    break;
                default:
                    output.AbortWithErrorMessage(errorMessage);
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
        /// <param name="value">The JSON object to read.</param>
        /// <param name="output">The output data.</param>
        /// <param name="readInfo">Additional information for the read operation.</param>
        /// <returns>True if the read is successful, otherwise false.</returns>
        public override bool Reader(ReadOnlyMemory<byte> key, ref ObjectInput input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo)
        {
            Debug.Assert(value is GarnetJsonObject);

            var parseState = input.parseState;

            using var outputStream = new MemoryStream();
            var isSuccess = false;
            ReadOnlySpan<byte> errorMessage = default;
            if (parseState.Count == 0)
            {
                ReadOnlySpan<byte> path = default;
                isSuccess = ((GarnetJsonObject)value).TryGet(path, outputStream, out errorMessage);
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
                    var option = CustomCommandUtils.GetNextArg(ref input, ref offset);
                    if (option.EqualsUpperCaseSpanIgnoringCase(JsonCmdStrings.INDENT) && offset < parseState.Count)
                    {
                        indent = Encoding.UTF8.GetString(CustomCommandUtils.GetNextArg(ref input, ref offset));
                        continue;
                    }
                    else if (option.EqualsUpperCaseSpanIgnoringCase(JsonCmdStrings.NEWLINE) && offset < parseState.Count)
                    {
                        newLine = Encoding.UTF8.GetString(CustomCommandUtils.GetNextArg(ref input, ref offset));
                        continue;
                    }
                    else if (option.EqualsUpperCaseSpanIgnoringCase(JsonCmdStrings.SPACE) && offset < parseState.Count)
                    {
                        space = Encoding.UTF8.GetString(CustomCommandUtils.GetNextArg(ref input, ref offset));
                        continue;
                    }

                    if (offset > parseState.Count)
                    {
                        return output.AbortWithWrongNumberOfArguments("json.get");
                    }
                    else
                    {
                        // If the code reached here then it means the current offset is a path, not an option
                        paths = parseState.Parameters.Slice(--offset);
                        break;
                    }
                }

                isSuccess = ((GarnetJsonObject)value).TryGet(paths, outputStream, out errorMessage, indent, newLine, space);
            }

            if (!isSuccess)
            {
                output.AbortWithErrorMessage(errorMessage);
                return true;
            }

            if (outputStream.Length == 0)
            {
                CustomCommandUtils.WriteNullBulkString(ref output);
            }
            else
            {
                CustomCommandUtils.WriteBulkString(ref output, outputStream.GetBuffer().AsSpan(0, (int)outputStream.Length));
            }
            return true;
        }
    }
}