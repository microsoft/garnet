// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// What an object-store command writes when its record is found.
    /// </summary>
    internal enum ObjectHitReply : byte
    {
        /// <summary>Whatever the operation serialized.</summary>
        Serialized,

        /// <summary>What the operation serialized, then a null if it reported -1.</summary>
        SerializedThenNullIfNegativeOne,

        /// <summary>The count the operation reported.</summary>
        Int32Result1,

        /// <summary><c>+OK</c>.</summary>
        Ok,

        /// <summary>
        /// The count the operation reported, or the invalid-range error if it reported <see cref="int.MaxValue"/>,
        /// as the lexicographic range commands do for a malformed bound.
        /// </summary>
        Int32Result1OrLexRangeError,
    }

    /// <summary>
    /// What an object-store command writes when it finds no record.
    /// </summary>
    internal enum ObjectMissReply : byte
    {
        /// <summary><c>:0</c>, for commands that report how many elements they saw.</summary>
        Zero,

        /// <summary>A null bulk string.</summary>
        Null,

        /// <summary>An empty array, for commands that return a collection.</summary>
        EmptyArray,

        /// <summary>An empty set, which RESP3 distinguishes from an empty array.</summary>
        EmptySet,

        /// <summary>One null per argument after the key.</summary>
        NullPerArg,

        /// <summary>
        /// <see cref="ObjectReplyShape.Count"/> copies of <c>:-2</c>, the "no such field" of the TTL commands.
        /// </summary>
        MinusTwoPerCount,

        /// <summary>An empty array when <see cref="ObjectReplyShape.Flag"/> is set, otherwise a null.</summary>
        EmptyArrayIfFlagElseNull,

        /// <summary><c>:0</c> when <see cref="ObjectReplyShape.Flag"/> is set, otherwise one per argument.</summary>
        ZeroIfFlagElseZeroPerArg,

        /// <summary>A null when <see cref="ObjectReplyShape.Flag"/> is set, otherwise one null array per argument.</summary>
        NullIfFlagElseNullArrayPerArg,

        /// <summary>The no-such-key error, for updates of an existing element.</summary>
        NoSuchKey,

        /// <summary>
        /// What a hit writes. For an update whose miss is an update that declined to create its record, such as
        /// <c>ZADD XX</c> on a missing key, which writes its reply as a hit would.
        /// </summary>
        AsHit,
    }

    /// <summary>
    /// How one object-store command writes its reply, and what it does once its operation is applied, in a form
    /// that survives a suspension.
    /// </summary>
    /// <remarks>
    /// The handler's own reply switch is replaced by this, so the suspended and in-memory paths write
    /// through the same code and cannot drift apart. Only the parts a handler cannot recompute after it
    /// has unwound are carried here: a span cannot live in a session field, and
    /// neither can the handler's locals, but the parse state can still be read on resume.
    /// </remarks>
    internal readonly struct ObjectReplyShape
    {
        /// <summary>What to write when the record is found.</summary>
        internal readonly ObjectHitReply Hit;

        /// <summary>What to write when it is not.</summary>
        internal readonly ObjectMissReply Miss;

        /// <summary>The condition of the miss replies that have two forms.</summary>
        internal readonly bool Flag;

        /// <summary>
        /// Whether the command can add elements that a blocking pop waits for, so the waiters on its key are told once
        /// the update is applied.
        /// </summary>
        internal readonly bool NotifiesCollectionUpdate;

        /// <summary>How many elements the repeating miss replies write.</summary>
        internal readonly int Count;

        internal ObjectReplyShape(ObjectMissReply miss, ObjectHitReply hit = ObjectHitReply.Serialized, bool flag = false, int count = 0,
            bool notifiesCollectionUpdate = false)
        {
            Hit = hit;
            Miss = miss;
            Flag = flag;
            NotifiesCollectionUpdate = notifiesCollectionUpdate;
            Count = count;
        }
    }
}