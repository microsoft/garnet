// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Security.Cryptography;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    ///  Set - RESP specific operations
    /// </summary>
    public partial class SetObject : IGarnetObject
    {
        private void SetAdd(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var added = 0;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var member = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

#if NET9_0_OR_GREATER
                if (setLookup.Add(member))
#else
                if (Set.Add(member.ToArray()))
#endif
                {
                    added++;
                    UpdateSize(member);
                }
            }

            if (added == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;

            if (!input.header.CheckSkipRespOutputFlag())
                writer.WriteInt32(added);

            output.Header.result1 = added;
        }

        private void SetMembers(ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            writer.WriteSetLength(Set.Count);

            foreach (var item in Set)
            {
                writer.WriteBulkString(item);
                output.Header.result1++;
            }
        }

        private void SetIsMember(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var member = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
#if NET9_0_OR_GREATER
            var isMember = setLookup.Contains(member);
#else
            var isMember = Set.Contains(member.ToArray());
#endif
            writer.WriteInt32(isMember ? 1 : 0);
            output.Header.result1 = 1;
        }

        private void SetMultiIsMember(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            writer.WriteArrayLength(input.parseState.Count);

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var member = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;
#if NET9_0_OR_GREATER
                var isMember = setLookup.Contains(member);
#else
                var isMember = Set.Contains(member.ToArray());
#endif
                writer.WriteInt32(isMember ? 1 : 0);
            }

            output.Header.result1 = input.parseState.Count;
        }

        private void SetRemove(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var removed = 0;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var field = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

#if NET9_0_OR_GREATER
                if (setLookup.Remove(field))
#else
                if (Set.Remove(field.ToArray()))
#endif
                {
                    removed++;
                    UpdateSize(field, false);
                }
            }

            if (removed == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;

            if (!input.header.CheckSkipRespOutputFlag())
                writer.WriteInt32(removed);

            output.Header.result1 = removed;
        }

        private void SetLength(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // SCARD key
            var length = Set.Count;

            if (!input.header.CheckSkipRespOutputFlag())
                writer.WriteInt32(length);

            output.Header.result1 = length;
        }

        private void SetPop(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // SPOP key [count]
            var count = input.arg1;

            if (Set.Count == 0 || count == 0)
            {
                writer.WriteEmptyArray();
                output.OutputFlags |= OutputFlags.ValueUnchanged;
                return;
            }

            var countDone = 0;

            // key [count]
            if (count >= 1)
            {
                // POP this number of random fields
                var countParameter = count > Set.Count ? Set.Count : count;

                // Write the size of the array reply
                writer.WriteSetLength(countParameter);

                for (var i = 0; i < countParameter; i++)
                {
                    // Generate a new index based on the elements left in the set
                    var index = RandomNumberGenerator.GetInt32(0, Set.Count);
                    var item = Set.ElementAt(index);
                    Set.Remove(item);
                    UpdateSize(item, false);
                    writer.WriteBulkString(item);
                    countDone++;
                }

                countDone += count - countDone;
            }
            else if (count == int.MinValue) // no count parameter is present, we just pop and return a random item of the set
            {
                // Write a bulk string value of a random field from the hash value stored at key.
                if (Set.Count > 0)
                {
                    var index = RandomNumberGenerator.GetInt32(0, Set.Count);
                    var item = Set.ElementAt(index);
                    Set.Remove(item);
                    UpdateSize(item, false);
                    writer.WriteBulkString(item);
                }
                else
                {
                    // If set empty return nil
                    writer.WriteNull();
                }
                countDone++;
            }

            output.Header.result1 = countDone;
        }

        private void SetRandomMember(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var count = input.arg1;
            var seed = input.arg2;

            var countDone = 0;

            if (count > 0)
            {
                // Return an array of distinct elements
                var countParameter = count > Set.Count ? Set.Count : count;

                // The order of fields in the reply is not truly random
                var indexes = countParameter <= RandomUtils.IndexStackallocThreshold ?
                    stackalloc int[RandomUtils.IndexStackallocThreshold].Slice(0, countParameter) : new int[countParameter];

                RandomUtils.PickKRandomIndexes(countParameter, indexes, seed);

                // Write the size of the array reply
                writer.WriteSetLength(countParameter);

                foreach (var index in indexes)
                {
                    var element = Set.ElementAt(index);
                    writer.WriteBulkString(element);
                    countDone++;
                }
                countDone += count - countParameter;
            }
            else if (count == int.MinValue) // no count parameter is present
            {
                // Return a single random element from the set
                if (Set.Count > 0)
                {
                    var index = RandomUtils.PickRandomIndex(Set.Count, seed);
                    var item = Set.ElementAt(index);
                    writer.WriteBulkString(item);
                }
                else
                {
                    // If set is empty, return nil
                    writer.WriteNull();
                }
                countDone++;
            }
            else // count < 0
            {
                // Return an array with potentially duplicate elements
                var countParameter = Math.Abs(count);

                var indexes = countParameter <= RandomUtils.IndexStackallocThreshold ?
                    stackalloc int[RandomUtils.IndexStackallocThreshold].Slice(0, countParameter) : new int[countParameter];

                RandomUtils.PickKRandomIndexes(Set.Count, indexes, seed, false);

                if (Set.Count > 0)
                {
                    // Write the size of the array reply
                    writer.WriteArrayLength(countParameter);

                    foreach (var index in indexes)
                    {
                        var element = Set.ElementAt(index);
                        writer.WriteBulkString(element);
                        countDone++;
                    }
                }
                else
                {
                    // If set is empty, return nil
                    writer.WriteNull();
                }
            }

            output.Header.result1 = countDone;
        }
    }
}