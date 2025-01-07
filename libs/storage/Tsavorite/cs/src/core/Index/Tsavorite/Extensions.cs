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
        /// <typeparam name="TValue"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IObservable<AllocatorRecord<TValue>> ToRecordObservable<TValue>(this IObservable<ITsavoriteScanIterator<TValue>> source)
        {
            return new RecordObservable<TValue>(source);
        }

        internal sealed class RecordObservable<TValue> : IObservable<AllocatorRecord<TValue>>
        {
            readonly IObservable<ITsavoriteScanIterator<TValue>> o;

            public RecordObservable(IObservable<ITsavoriteScanIterator<TValue>> o)
            {
                this.o = o;
            }

            public IDisposable Subscribe(IObserver<AllocatorRecord<TValue>> observer)
            {
                return o.Subscribe(new RecordObserver<TValue>(observer));
            }
        }

        internal sealed class RecordObserver<TValue> : IObserver<ITsavoriteScanIterator<TValue>>
        {
            private readonly IObserver<AllocatorRecord<TValue>> observer;

            public RecordObserver(IObserver<AllocatorRecord<TValue>> observer)
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

            public void OnNext(ITsavoriteScanIterator<TValue> v)
            {
                while (v.GetNext(out RecordInfo info, out TKey key, out TValue value))
                {
                    observer.OnNext(new AllocatorRecord<TValue> { info = info, key = key, value = value });
                }
            }
        }
    }
}