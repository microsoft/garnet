// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Converts Key1 to Key2, Input1 to Input2, and creates TOutput2 for a 
    /// <see cref="DualContext{TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2, TDualInputConverter}"/>
    /// that does not find the key in the first store so must do operations on the second.
    /// </summary>
    public interface IDualInputConverter<TKey1, TInput1, TKey2, TInput2, TOutput2>
    {
        void ConvertForRead(ref TKey1 key1, ref TInput1 input1, out TKey2 key2, out TInput2 input2, out TOutput2 output2);
        void ConvertForUpsert(ref TKey1 key1, ref TInput1 input1, out TKey2 key2, out TInput2 input2, out TOutput2 output2);
        void ConvertForRMW(ref TKey1 key1, ref TInput1 input1, out TKey2 key2, out TInput2 input2, out TOutput2 output2);
        void ConvertKey(ref TKey1 key1, out TKey2 key2);
    }

    public class CopyDualInputConverter<TKey1, TInput1, TKey2, TInput2, TOutput2>
        where TKey2 : TKey1
        where TInput2 : TInput1
        where TOutput2 : new()
    {
        public void ConvertForRead(ref TKey1 key1, ref TInput1 input1, out TKey2 key2, out TInput2 input2, out TOutput2 output2)
        {
            ConvertKey(ref key1, out key2);
            input2 = (TInput2)input1;
            output2 = new();
        }
        public void ConvertForUpsert(ref TKey1 key1, ref TInput1 input1, out TKey2 key2, out TInput2 input2, out TOutput2 output2) => ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
        public void ConvertForRMW(ref TKey1 key1, ref TInput1 input1, out TKey2 key2, out TInput2 input2, out TOutput2 output2) => ConvertForRead(ref key1, ref input1, out key2, out input2, out output2);
        public void ConvertKey(ref TKey1 key1, out TKey2 key2) => key2 = (TKey2)key1;
    }
}
