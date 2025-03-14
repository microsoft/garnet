// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if NET9_0_OR_GREATER

namespace Garnet.common.Collections
{
    /// <summary>
    /// Implemented by an <see cref="System.Collections.Generic.IComparer{T}"/> to support comparing
    /// a <typeparamref name="TAlternate"/> instance with a <typeparamref name="T"/> instance.
    /// </summary>
    /// <typeparam name="TAlternate">The alternate type to compare.</typeparam>
    /// <typeparam name="T">The type to compare.</typeparam>
    public interface IAlternateComparer<in TAlternate, T>
        where TAlternate : allows ref struct 
        where T : allows ref struct
    {
        /// <summary>
        /// Creates a T that is considered by <see cref="System.Collections.Generic.IComparer{T}.Compare(T, T)"/> to be equal to the specified alternate.
        /// </summary>
        /// <param name="alternate">The instance of type <typeparamref name="TAlternate"/> for which an equal <typeparamref name="T"/> is required.</param>
        /// <returns>A <typeparamref name="T"/> considered equal to the specified alternate.</returns>
        T Create(TAlternate alternate);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="alternate">The instance of type <typeparamref name="TAlternate"/> to compare.</param>
        /// <param name="other">The instance of type <typeparamref name="T"/> to compare.</param>
        /// <returns></returns>
        int Compare(TAlternate alternate, T other);
    }
}

#endif