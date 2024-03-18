// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Interface for variable length Inputs to RMW; only implemented for <typeparamref name="TValue"/> of <see cref="SpanByte"/>.
    /// </summary>
    internal interface IVariableLengthInput<TValue, TInput>
    {
        /// <summary>
        /// Length of resulting value object when performing RMW modification of value using given input
        /// </summary>
        int GetRMWModifiedValueLength(ref TValue value, ref TInput input);

        /// <summary>
        /// Initial expected length of value object when populated by RMW using given input
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        int GetRMWInitialValueLength(ref TInput input);
    }
}