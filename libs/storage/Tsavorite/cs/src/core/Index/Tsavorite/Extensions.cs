// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Log subscription extensions
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Create observable of log records
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IObservable<AllocatorRecord<TKey, TValue>> ToRecordObservable<TKey, TValue>(this IObservable<IRecordScanner<TKey, TValue>> source)
        {
            return new RecordObservable<TKey, TValue>(source);
        }

        internal sealed class RecordObservable<TKey, TValue> : IObservable<AllocatorRecord<TKey, TValue>>
        {
            readonly IObservable<IRecordScanner<TKey, TValue>> o;

            public RecordObservable(IObservable<IRecordScanner<TKey, TValue>> o)
            {
                this.o = o;
            }

            public IDisposable Subscribe(IObserver<AllocatorRecord<TKey, TValue>> observer)
            {
                return o.Subscribe(new RecordObserver<TKey, TValue>(observer));
            }
        }

        internal sealed class RecordObserver<TKey, TValue> : IObserver<IRecordScanner<TKey, TValue>>
        {
            private readonly IObserver<AllocatorRecord<TKey, TValue>> observer;

            public RecordObserver(IObserver<AllocatorRecord<TKey, TValue>> observer)
            {
                this.observer = observer;
            }

            public void OnCompleted()
            {
                observer.OnCompleted();
            }

            public void OnError(Exception error)
            {
                observer.OnError(error);
            }

            public void OnNext(IRecordScanner<TKey, TValue> v)
            {
                while (v.GetNext(out RecordInfo info, out TKey key, out TValue value))
                {
                    observer.OnNext(new AllocatorRecord<TKey, TValue> { info = info, key = key, value = value });
                }
            }
        }
    }
}