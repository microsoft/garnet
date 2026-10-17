// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Answers a blocking command that may not wait (<see cref="DenyBlocking"/>) with what <paramref name="keys"/>
        /// hold now, as though its timeout had already expired.
        /// </summary>
        /// <param name="command">The command, with BRPOPLPUSH given as BLMOVE</param>
        /// <param name="keys">Keys to try, in order; the source key alone for BLMOVE</param>
        /// <param name="cmdArgs">Additional arguments for command</param>
        bool BlockingCommandNow(RespCommand command, byte[][] keys, CollectionItemArgs cmdArgs)
        {
            // A consume that cannot park waits a device read out on this thread instead; see CanSuspend.
            var attempt = storeWrapper.itemBroker.TryGetCollectionItemNowAsync(command, keys, storageSession, cmdArgs, waitInPlace: !CanSuspend);
            if (attempt.IsCompletedSuccessfully)
            {
                WriteBlockingCommandResult(command, attempt.GetAwaiter().GetResultGuarded());
                return true;
            }

            // Keeps the batch scope: a BLMOVE pushes the element it popped, which is in the scratch buffers, and the
            // push may wait on the device.
            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope: true))
                body = BlockingCommandNowBodyAsync(command, attempt);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask BlockingCommandNowBodyAsync(RespCommand command, ValueTask<CollectionItemResult> attempt)
            => WriteBlockingCommandResult(command, await attempt.ConfigureAwait(false));

        void WriteBlockingCommandResult(RespCommand command, CollectionItemResult result)
        {
            switch (command)
            {
                case RespCommand.BLPOP:
                case RespCommand.BRPOP:
                    WriteListBlockingPopResult(result);
                    break;
                case RespCommand.BLMOVE:
                    WriteListBlockingMoveResult(result);
                    break;
                case RespCommand.BLMPOP:
                    WriteListBlockingPopMultipleResult(result);
                    break;
                case RespCommand.BZPOPMIN:
                case RespCommand.BZPOPMAX:
                    WriteSortedSetBlockingPopResult(result);
                    break;
                case RespCommand.BZMPOP:
                    WriteSortedSetBlockingMPopResult(result);
                    break;
                default:
                    Debug.Fail($"{command} is not a blocking collection command");
                    break;
            }
        }

        /// <summary>
        /// <c>BLPOP key [key ...] timeout</c> and <c>BRPOP key [key ...] timeout</c>.
        /// </summary>
        bool ListBlockingPop(RespCommand command)
        {
            if (parseState.Count < 2)
                return AbortWithWrongNumberOfArguments(command.ToString());

            var keysBytes = new byte[parseState.Count - 1][];
            for (var i = 0; i < keysBytes.Length; i++)
                keysBytes[i] = BlockingKeys.Copy(parseState.GetArgSliceByRef(i).ReadOnlySpan);

            if (!parseState.TryGetTimeout(parseState.Count - 1, out var timeout, out var error))
                return AbortWithErrorMessage(error);

            if (DenyBlocking)
                return BlockingCommandNow(command, keysBytes, default);

            ValueTask body;
            using (BeginAsyncCommand())
                body = ListBlockingPopBodyAsync(command, keysBytes, timeout);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask ListBlockingPopBodyAsync(RespCommand command, byte[][] keys, double timeout)
        {
            var result = await storeWrapper.itemBroker
                .GetCollectionItemAsync(command, keys, this, timeout).ConfigureAwait(false);

            WriteListBlockingPopResult(result);
        }

        void WriteListBlockingPopResult(CollectionItemResult result)
        {
            if (TryWriteBlockingFailure(result))
                return;

            if (!result.Found)
            {
                WriteNullArray();
                return;
            }

            WriteArrayLength(2);
            WriteBulkString(result.Key);
            WriteBulkString(result.Item);
        }

        /// <summary>
        /// <c>BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout</c>.
        /// </summary>
        bool ListBlockingMove()
        {
            if (parseState.Count != 5)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BLMOVE));

            if (!parseState.TryGetOperationDirection(2, out var srcDir) ||
                !parseState.TryGetOperationDirection(3, out var dstDir))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            if (!parseState.TryGetTimeout(4, out var timeout, out var error))
                return AbortWithErrorMessage(error);

            return ListBlockingMove(srcDir, dstDir, timeout);
        }

        /// <summary>
        /// <c>BRPOPLPUSH source destination timeout</c>, the fixed-direction form of <c>BLMOVE</c>.
        /// </summary>
        bool ListBlockingPopPush()
        {
            if (parseState.Count != 3)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BRPOPLPUSH));

            if (!parseState.TryGetTimeout(2, out var timeout, out var error))
                return AbortWithErrorMessage(error);

            return ListBlockingMove(OperationDirection.Right, OperationDirection.Left, timeout);
        }

        /// <summary>
        /// Shared implementation of <c>BLMOVE</c> and <c>BRPOPLPUSH</c>. The source key is argument 0 and the
        /// destination key argument 1 in both.
        /// </summary>
        bool ListBlockingMove(OperationDirection sourceDirection, OperationDirection destinationDirection,
            double timeout)
        {
            if (sourceDirection == OperationDirection.Unknown || destinationDirection == OperationDirection.Unknown)
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            var srcKey = BlockingKeys.Copy(parseState.GetArgSliceByRef(0).ReadOnlySpan);
            var dstKey = BlockingKeys.Copy(parseState.GetArgSliceByRef(1).ReadOnlySpan);
            var cmdArgs = CollectionItemArgs.ForMove(dstKey, sourceDirection, destinationDirection);

            if (DenyBlocking)
                return BlockingCommandNow(RespCommand.BLMOVE, [srcKey], cmdArgs);

            ValueTask body;
            using (BeginAsyncCommand())
                body = ListBlockingMoveBodyAsync(srcKey, cmdArgs, timeout);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask ListBlockingMoveBodyAsync(byte[] srcKey, CollectionItemArgs cmdArgs, double timeout)
        {
            var result = await storeWrapper.itemBroker
                .MoveCollectionItemAsync(RespCommand.BLMOVE, srcKey, this, timeout, cmdArgs).ConfigureAwait(false);

            WriteListBlockingMoveResult(result);
        }

        void WriteListBlockingMoveResult(CollectionItemResult result)
        {
            if (TryWriteBlockingFailure(result))
                return;

            if (!result.Found)
                WriteNull();
            else
                WriteBulkString(result.Item);
        }

        /// <summary>
        /// <c>BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]</c>.
        /// </summary>
        bool ListBlockingPopMultiple()
        {
            if (parseState.Count < 4)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BLMPOP));

            if (!TryParseBlockingMPop(out var keys, out var timeout, out var directionTokenId))
                return true;

            if (!parseState.TryGetOperationDirection(directionTokenId, out var popDirection))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            if (!TryParseBlockingMPopCount(directionTokenId, out var popCount))
                return true;

            var cmdArgs = CollectionItemArgs.ForListPop(popDirection, popCount);

            if (DenyBlocking)
                return BlockingCommandNow(RespCommand.BLMPOP, keys, cmdArgs);

            ValueTask body;
            using (BeginAsyncCommand())
                body = ListBlockingPopMultipleBodyAsync(keys, cmdArgs, timeout);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask ListBlockingPopMultipleBodyAsync(byte[][] keys, CollectionItemArgs cmdArgs, double timeout)
        {
            var result = await storeWrapper.itemBroker
                .GetCollectionItemAsync(RespCommand.BLMPOP, keys, this, timeout, cmdArgs).ConfigureAwait(false);

            WriteListBlockingPopMultipleResult(result);
        }

        void WriteListBlockingPopMultipleResult(CollectionItemResult result)
        {
            if (TryWriteBlockingFailure(result))
                return;

            if (!result.Found)
            {
                WriteNull();
                return;
            }

            WriteArrayLength(2);
            WriteBulkString(result.Key);

            var elements = result.Items;
            WriteArrayLength(elements.Length);
            foreach (var element in elements)
                WriteBulkString(element);
        }

        /// <summary>
        /// <c>BZPOPMIN key [key ...] timeout</c> and <c>BZPOPMAX key [key ...] timeout</c>.
        /// </summary>
        bool SortedSetBlockingPop(RespCommand command)
        {
            if (parseState.Count < 2)
                return AbortWithWrongNumberOfArguments(command.ToString());

            if (!parseState.TryGetTimeout(parseState.Count - 1, out var timeout, out var error))
                return AbortWithErrorMessage(error);

            var keysBytes = new byte[parseState.Count - 1][];
            for (var i = 0; i < keysBytes.Length; i++)
                keysBytes[i] = BlockingKeys.Copy(parseState.GetArgSliceByRef(i).ReadOnlySpan);

            if (DenyBlocking)
                return BlockingCommandNow(command, keysBytes, default);

            ValueTask body;
            using (BeginAsyncCommand())
                body = SortedSetBlockingPopBodyAsync(command, keysBytes, timeout);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask SortedSetBlockingPopBodyAsync(RespCommand command, byte[][] keys, double timeout)
        {
            var result = await storeWrapper.itemBroker
                .GetCollectionItemAsync(command, keys, this, timeout).ConfigureAwait(false);

            WriteSortedSetBlockingPopResult(result);
        }

        void WriteSortedSetBlockingPopResult(CollectionItemResult result)
        {
            if (TryWriteBlockingFailure(result))
                return;

            if (!result.Found)
            {
                WriteNull();
                return;
            }

            WriteArrayLength(3);
            WriteBulkString(result.Key);
            WriteBulkString(result.Item);
            WriteDoubleNumeric(result.Score);
        }

        /// <summary>
        /// <c>BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]</c>.
        /// </summary>
        bool SortedSetBlockingMPop()
        {
            if (parseState.Count < 4)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BZMPOP));

            if (!TryParseBlockingMPop(out var keys, out var timeout, out var orderTokenId))
                return true;

            var orderSpan = parseState.GetArgSliceByRef(orderTokenId).ReadOnlySpan;
            bool lowScoresFirst;

            if (orderSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MIN))
                lowScoresFirst = true;
            else if (orderSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MAX))
                lowScoresFirst = false;
            else
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);

            if (!TryParseBlockingMPopCount(orderTokenId, out var popCount))
                return true;

            var cmdArgs = CollectionItemArgs.ForSortedSetPop(lowScoresFirst, popCount);

            if (DenyBlocking)
                return BlockingCommandNow(RespCommand.BZMPOP, keys, cmdArgs);

            ValueTask body;
            using (BeginAsyncCommand())
                body = SortedSetBlockingMPopBodyAsync(keys, cmdArgs, timeout);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask SortedSetBlockingMPopBodyAsync(byte[][] keys, CollectionItemArgs cmdArgs, double timeout)
        {
            var result = await storeWrapper.itemBroker
                .GetCollectionItemAsync(RespCommand.BZMPOP, keys, this, timeout, cmdArgs).ConfigureAwait(false);

            WriteSortedSetBlockingMPopResult(result);
        }

        void WriteSortedSetBlockingMPopResult(CollectionItemResult result)
        {
            if (TryWriteBlockingFailure(result))
                return;

            if (!result.Found)
            {
                WriteNull();
                return;
            }

            WriteArrayLength(2);
            WriteBulkString(result.Key);

            WriteArrayLength(result.Items.Length);
            for (var i = 0; i < result.Items.Length; i++)
            {
                WriteArrayLength(2);
                WriteBulkString(result.Items[i]);
                WriteDoubleNumeric(result.Scores[i]);
            }
        }

        /// <summary>
        /// Parses the arguments <c>BLMPOP</c> and <c>BZMPOP</c> have in common:
        /// <c>timeout numkeys key [key ...] &lt;selector&gt; [COUNT count]</c>. The selector itself differs
        /// between the two and is left to the caller.
        /// </summary>
        /// <param name="keys">Keys to observe, copied onto the pinned object heap.</param>
        /// <param name="timeout">Timeout in seconds, 0 to wait indefinitely.</param>
        /// <param name="selectorTokenId">Index of the selector token that follows the keys.</param>
        /// <returns>True if parsing succeeded; otherwise false, with a RESP error already written.</returns>
        bool TryParseBlockingMPop(out byte[][] keys, out double timeout, out int selectorTokenId)
        {
            keys = null;
            selectorTokenId = default;

            var currTokenId = 0;

            if (!parseState.TryGetTimeout(currTokenId++, out timeout, out var error))
            {
                _ = AbortWithErrorMessage(error);
                return false;
            }

            if (!parseState.TryGetInt(currTokenId++, out var numKeys) || numKeys < 1)
            {
                var err = string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "numkeys");
                _ = AbortWithErrorMessage(Encoding.ASCII.GetBytes(err));
                return false;
            }

            // Either <timeout numkeys keys... selector> or that followed by <COUNT count>.
            if (parseState.Count != numKeys + 3 && parseState.Count != numKeys + 5)
            {
                _ = AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                return false;
            }

            keys = new byte[numKeys][];
            for (var i = 0; i < keys.Length; i++)
                keys[i] = BlockingKeys.Copy(parseState.GetArgSliceByRef(currTokenId++).ReadOnlySpan);

            selectorTokenId = currTokenId;
            return true;
        }

        /// <summary>
        /// Reads the optional <c>COUNT count</c> that may follow the selector of <c>BLMPOP</c> and
        /// <c>BZMPOP</c>.
        /// </summary>
        /// <remarks>
        /// Separate from <see cref="TryParseBlockingMPop"/> so that the caller validates the selector first.
        /// An unrecognized selector is a syntax error whatever follows it, and reading the count ahead of it
        /// would report the count instead.
        /// </remarks>
        /// <param name="selectorTokenId">Index of the selector token, which the count follows.</param>
        /// <param name="popCount">Maximum number of elements to pop; 1 when no count is given.</param>
        /// <returns>True if parsing succeeded; otherwise false, with a RESP error already written.</returns>
        bool TryParseBlockingMPopCount(int selectorTokenId, out int popCount)
        {
            popCount = 1;

            var currTokenId = selectorTokenId + 1;
            if (currTokenId == parseState.Count)
                return true;

            var countKeyword = parseState.GetArgSliceByRef(currTokenId++);
            if (!countKeyword.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
            {
                _ = AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                return false;
            }

            if (!parseState.TryGetInt(currTokenId, out popCount) || popCount < 1)
            {
                var err = string.Format(CmdStrings.GenericParamShouldBeGreaterThanZero, "count");
                _ = AbortWithErrorMessage(Encoding.ASCII.GetBytes(err));
                return false;
            }

            return true;
        }

        /// <summary>
        /// Writes the reply for a blocking collection result that carries no item because the wait ended in a
        /// way the caller must be told about, rather than because nothing arrived in time.
        /// </summary>
        /// <param name="result">Result of the wait.</param>
        /// <returns>True if a reply was written and the caller is done.</returns>
        bool TryWriteBlockingFailure(CollectionItemResult result)
        {
            if (result.IsForceUnblocked)
            {
                WriteError(CmdStrings.RESP_UNBLOCKED_CLIENT_VIA_CLIENT_UNBLOCK);
                return true;
            }

            if (result.IsTypeMismatch)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                return true;
            }

            return false;
        }
    }
}