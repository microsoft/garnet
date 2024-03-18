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
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IObservable<Record<Key, Value>> ToRecordObservable<Key, Value>(this IObservable<ITsavoriteScanIterator<Key, Value>> source)
        {
            return new RecordObservable<Key, Value>(source);
        }

        internal class RecordObservable<Key, Value> : IObservable<Record<Key, Value>>
        {
            readonly IObservable<ITsavoriteScanIterator<Key, Value>> o;

            public RecordObservable(IObservable<ITsavoriteScanIterator<Key, Value>> o)
            {
                this.o = o;
            }

            public IDisposable Subscribe(IObserver<Record<Key, Value>> observer)
            {
                return o.Subscribe(new RecordObserver<Key, Value>(observer));
            }
        }

        internal sealed class RecordObserver<Key, Value> : IObserver<ITsavoriteScanIterator<Key, Value>>
        {
            private readonly IObserver<Record<Key, Value>> observer;

            public RecordObserver(IObserver<Record<Key, Value>> observer)
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

            public void OnNext(ITsavoriteScanIterator<Key, Value> v)
            {
                while (v.GetNext(out RecordInfo info, out Key key, out Value value))
                {
                    observer.OnNext(new Record<Key, Value> { info = info, key = key, value = value });
                }
            }
        }
    }
}