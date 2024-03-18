// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal sealed class LogCompactionFunctions<Key, Value, Input, Output, Context, Functions> : IFunctions<Key, Value, Input, Output, Context>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly Functions _functions;

        public LogCompactionFunctions(Functions functions)
        {
            _functions = functions;
        }

        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint) { }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo) => true;

        public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo) { }

        /// <summary>
        /// No ConcurrentDeleter needed for compaction
        /// </summary>
        public bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        /// <summary>
        /// For compaction, we never perform concurrent writes as rolled over data defers to
        /// newly inserted data for the same key.
        /// </summary>
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => true;

        public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

        public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }

        public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }

        public int GetRMWModifiedValueLength(ref Value value, ref Input input) => 0;
        public int GetRMWInitialValueLength(ref Input input) => 0;

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo) => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => _functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }
        public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }
        public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }
        public void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) { }
        public void DisposeDeserializedFromDisk(ref Key key, ref Value value) { }
        public void DisposeForRevivification(ref Key key, ref Value value, int newKeySize) { }
    }
}