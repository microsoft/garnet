// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Security.Cryptography;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    ///  Set - RESP specific operations
    /// </summary>
    public unsafe partial class SetObject : IGarnetObject
    {
        private void SetAdd(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var member = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

#if NET9_0_OR_GREATER
                if (setLookup.Add(member))
#else
                if (Set.Add(member.ToArray()))
#endif
                {
                    _output->result1++;
                    this.UpdateSize(member);
                }
            }
        }

        private void SetMembers(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input.header, ref outputFooter);

            output.WriteArrayLength(Set.Count);

            foreach (var item in Set)
            {
                output.WriteBulkString(item);
                output.IncResult1();
            }
        }

        private void SetIsMember(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input.header, ref outputFooter);

            var member = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
#if NET9_0_OR_GREATER
            var isMember = setLookup.Contains(member);
#else
            var isMember = Set.Contains(member.ToArray());
#endif
            output.WriteInt32(isMember ? 1 : 0);
            output.SetResult1(1);
        }

        private void SetMultiIsMember(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            using var output = new GarnetObjectStoreRespOutput(ref input.header, ref outputFooter);

            output.WriteArrayLength(input.parseState.Count);

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var member = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;
#if NET9_0_OR_GREATER
                var isMember = setLookup.Contains(member);
#else
                var isMember = Set.Contains(member.ToArray());
#endif
                output.WriteInt32(isMember ? 1 : 0);
            }

            output.SetResult1(input.parseState.Count);
        }

        private void SetRemove(ref ObjectInput input, byte* output)
        {
            var _output = (ObjectOutputHeader*)output;
            *_output = default;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var field = input.parseState.GetArgSliceByRef(i).ReadOnlySpan;

#if NET9_0_OR_GREATER
                if (setLookup.Remove(field))
#else
                if (Set.Remove(field.ToArray()))
#endif
                {
                    _output->result1++;
                    this.UpdateSize(field, false);
                }
            }
        }

        private void SetLength(byte* output)
        {
            // SCARD key
            var _output = (ObjectOutputHeader*)output;
            _output->result1 = Set.Count;
        }

        private void SetPop(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            // SPOP key [count]
            var count = input.arg1;
            var countDone = 0;

            using var output = new GarnetObjectStoreRespOutput(ref input.header, ref outputFooter);

            // key [count]
            if (count >= 1)
            {
                // POP this number of random fields
                var countParameter = count > Set.Count ? Set.Count : count;

                // Write the size of the array reply
                output.WriteArrayLength(countParameter);

                for (var i = 0; i < countParameter; i++)
                {
                    // Generate a new index based on the elements left in the set
                    var index = RandomNumberGenerator.GetInt32(0, Set.Count);
                    var item = Set.ElementAt(index);
                    Set.Remove(item);
                    UpdateSize(item, false);
                    output.WriteBulkString(item);
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
                    output.WriteBulkString(item);
                }
                else
                {
                    // If set empty return nil
                    output.WriteNull();
                }
                countDone++;
            }

            output.SetResult1(countDone);
        }

        private void SetRandomMember(ref ObjectInput input, ref SpanByteAndMemory outputFooter)
        {
            var count = input.arg1;
            var seed = input.arg2;

            var countDone = 0;

            using var output = new GarnetObjectStoreRespOutput(ref input.header, ref outputFooter);

            Span<int> indexes = default;

            if (count > 0)
            {
                // Return an array of distinct elements
                var countParameter = count > Set.Count ? Set.Count : count;

                // The order of fields in the reply is not truly random
                indexes = RandomUtils.PickKRandomIndexes(Set.Count, countParameter, seed);

                // Write the size of the array reply
                output.WriteArrayLength(countParameter);

                foreach (var index in indexes)
                {
                    var element = Set.ElementAt(index);
                    output.WriteBulkString(element);
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
                    output.WriteBulkString(item);
                }
                else
                {
                    // If set is empty, return nil
                    output.WriteNull();
                }
                countDone++;
            }
            else // count < 0
            {
                // Return an array with potentially duplicate elements
                var countParameter = Math.Abs(count);

                indexes = RandomUtils.PickKRandomIndexes(Set.Count, countParameter, seed, false);

                if (Set.Count > 0)
                {
                    // Write the size of the array reply
                    output.WriteArrayLength(countParameter);

                    foreach (var index in indexes)
                    {
                        var element = Set.ElementAt(index);
                        output.WriteBulkString(element);
                        countDone++;
                    }
                }
                else
                {
                    // If set is empty, return nil
                    output.WriteNull();
                }
            }

            output.SetResult1(countDone);
        }
    }
}