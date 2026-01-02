// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Extends our IObserver iterator usage to provide a method for observing individual log records.
    /// </summary>
    /// <typeparam name="TObservable"></typeparam>
    public interface ITsavoriteRecordObserver<TObservable> : IObserver<TObservable>
    {
        /// <summary>
        /// Observe an individual log record.
        /// </summary>
        void OnRecord<TSourceLogRecord>(in TSourceLogRecord logRecord)
            where TSourceLogRecord : ISourceLogRecord;
    }
}