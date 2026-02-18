// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Info related to the meta-command enveloping the RESP command (if exists)
    /// </summary>
    public struct MetaCommandInfo
    {
        public MetaCommandInfo(RespMetaCommand metaCommand, SessionParseState metaCommandParseState)
            : this(metaCommand, metaCommandParseState, -1)
        {
        }

        public MetaCommandInfo(RespMetaCommand metaCommand, SessionParseState metaCommandParseState, long arg1)
        {
            MetaCommand = metaCommand;
            Arg1 = arg1;
            MetaCommandParseState = metaCommandParseState;
        }

        public void Initialize(int argCount = 0)
        {
            MetaCommand = RespMetaCommand.None;
            Arg1 = -1;
            MetaCommandParseState.Initialize(argCount);
        }

        /// <summary>
        /// Meta Command
        /// </summary>
        public RespMetaCommand MetaCommand;

        /// <summary>
        /// Argument for generic usage by command implementation
        /// For etag-related meta-commands, this holds the etag comparand, if exists
        /// </summary>
        public long Arg1 = -1;

        /// <summary>
        /// Meta command parse state
        /// </summary>
        public SessionParseState MetaCommandParseState;

        /// <summary>
        /// Get serialized length of <see cref="MetaCommandInfo"/>
        /// </summary>
        /// <returns>The serialized length</returns>
        public int GetSerializedLength()
        {
            var serializedLength = sizeof(byte); // Meta command
            if (MetaCommand != RespMetaCommand.None)
            {
                serializedLength += sizeof(long); // Arg1

                if (!MetaCommand.SkipMetaParseStateSerialization())
                {
                    serializedLength += MetaCommandParseState.GetSerializedLength(); // Meta parse state
                }
            }

            return serializedLength;
        }

        /// <summary>
        /// Serialize <see cref="MetaCommandInfo"/> to memory buffer
        /// </summary>
        /// <param name="dest">The memory buffer to serialize into (of size at least SerializedLength(firstIdx) bytes)</param>
        /// <param name="length">Length of buffer to serialize into.</param>
        /// <returns>Total serialized bytes</returns>
        public unsafe int SerializeTo(byte* dest, int length)
        {
            var curr = dest;

            // Serialize meta command
            *curr = (byte)MetaCommand;
            curr += sizeof(byte);

            if (MetaCommand != RespMetaCommand.None)
            {
                // Serialize arg1
                *(long*)curr = Arg1;
                curr += sizeof(long);

                if (!MetaCommand.SkipMetaParseStateSerialization())
                {
                    // Serialize meta command parse state
                    var remainingLength = length - (int)(curr - dest);
                    var parseStateLength = MetaCommandParseState.SerializeTo(curr, remainingLength);
                    curr += parseStateLength;
                }
            }

            return (int)(curr - dest);
        }

        /// <summary>
        /// Deserialize <see cref="MetaCommandInfo"/> from memory buffer into current struct
        /// </summary>
        /// <param name="src">Memory buffer to deserialize from</param>
        /// <returns>Number of deserialized bytes</returns>
        public unsafe int DeserializeFrom(byte* src)
        {
            var curr = src;

            // Deserialize meta command
            MetaCommand = (RespMetaCommand)(*curr);
            curr += sizeof(byte);

            if (MetaCommand != RespMetaCommand.None)
            {
                // Deserialize arg1
                Arg1 = *(long*)curr;
                curr += sizeof(long);

                if (!MetaCommand.SkipMetaParseStateSerialization())
                {
                    // Deserialize meta command parse state
                    var parseStateLength = MetaCommandParseState.DeserializeFrom(curr);
                    curr += parseStateLength;
                }
            }

            return (int)(curr - src);
        }

        /// <summary>
        /// Check whether an operation should execute based on the current meta command and the current record's etag.
        /// </summary>
        /// <param name="currEtag">Current etag</param>
        /// <param name="updatedEtag">Etag value that should be assigned to the record, should the operation run and succeed</param>
        /// <param name="initContext">True if method called from initial updater context</param>
        /// <param name="readOnlyContext">True if method called from read-only context</param>
        /// <returns>True if operation should execute</returns>
        public bool CheckConditionalExecution(long currEtag, out long updatedEtag, bool initContext = false, bool readOnlyContext = false)
        {
            updatedEtag = currEtag;

            // If there is no meta-command or current record does not have an etag - nothing to check
            if (MetaCommand == RespMetaCommand.None && currEtag == LogRecord.NoETag)
                return true;

            var execCmd = true;
            long inputEtag = LogRecord.NoETag;

            // If current meta-command is a conditional-execution command, check the condition against the input etag.
            if (MetaCommand.IsEtagCondExecCommand())
            {
                inputEtag = Arg1;

                // If called from initial updater context, operation should execute regardless
                if (!initContext)
                    execCmd = MetaCommand.CheckConditionalExecution(currEtag, inputEtag);
            }

            // If operation should execute and not called from read-only context, 
            // update the etag value that should get assigned should the operation succeed.
            if (execCmd && !readOnlyContext)
            {
                updatedEtag = MetaCommand switch
                {
                    RespMetaCommand.None or RespMetaCommand.ExecWithEtag => currEtag + 1,
                    RespMetaCommand.ExecIfMatch => inputEtag + 1,
                    RespMetaCommand.ExecIfGreater => inputEtag,
                    _ => throw new ArgumentException($"Unexpected meta command: {MetaCommand}", nameof(MetaCommand)),
                };
            }

            return execCmd;
        }
    }
}