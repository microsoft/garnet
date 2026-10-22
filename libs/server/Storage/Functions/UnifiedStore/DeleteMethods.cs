// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        public bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            functionsState.OnKeyChanged(deleteInfo.KeyHash, bumpWatchVersion: !logRecord.DataHeader.ValueIsObject);

            return true;
        }

        public void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            functionsState.OnKeyChanged(deleteInfo.KeyHash, bumpWatchVersion: logRecord.DataHeader.ValueIsObject && !logRecord.Info.Modified);

            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        public bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (!logRecord.DataHeader.ValueIsObject)
                logRecord.ClearOptionals();

            functionsState.OnKeyChanged(deleteInfo.KeyHash, bumpWatchVersion: !logRecord.Info.Modified);

            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF

            // Heap object cache-size tracking and disposal are handled by
            // storeFunctions.OnDispose (GarnetRecordTriggers) which is called
            // by Tsavorite after InPlaceDeleter returns.
            return true;
        }

        /// <inheritdoc />
        public void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey, allows ref struct
            where TEpochAccessor : IEpochAccessor
        {

            if ((deleteInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogDelete(key.KeyBytes, deleteInfo.Version, deleteInfo.SessionID, epochAccessor);
        }
    }
}