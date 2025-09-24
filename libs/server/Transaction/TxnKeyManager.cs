// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class TransactionManager
    {
        /// <summary>
        /// Save key entry
        /// </summary>
        /// <param name="key"></param>
        /// <param name="storeType"></param>
        /// <param name="type"></param>
        public void SaveKeyEntryToLock(PinnedSpanByte key, StoreType storeType, LockType type)
        {
            UpdateTransactionStoreType(storeType);
            keyEntries.AddKey(key, storeType, type);
        }

        /// <summary>
        /// Reset cached slot verification result
        /// </summary>
        public void ResetCacheSlotVerificationResult()
        {
            if (!clusterEnabled) return;
            respSession.clusterSession.ResetCachedSlotVerificationResult();
        }

        /// <summary>
        /// Reset cached slot verification result
        /// </summary>
        public void WriteCachedSlotVerificationMessage(ref MemoryResult<byte> output)
        {
            if (!clusterEnabled) return;
            respSession.clusterSession.WriteCachedSlotVerificationMessage(ref output);
        }

        /// <summary>
        /// Verify key ownership
        /// </summary>
        /// <param name="key"></param>
        /// <param name="type"></param>
        public unsafe void VerifyKeyOwnership(PinnedSpanByte key, LockType type)
        {
            if (!clusterEnabled) return;

            var readOnly = type == LockType.Shared;
            if (!respSession.clusterSession.NetworkIterativeSlotVerify(key, readOnly, respSession.SessionAsking))
            {
                this.state = TxnState.Aborted;
            }
        }

        /// <summary>
        /// Locks keys according to command's key specifications
        /// </summary>
        /// <param name="cmdInfo">Simplified command info</param>
        internal void LockKeys(SimpleRespCommandInfo cmdInfo)
        {
            if (cmdInfo.KeySpecs == null || cmdInfo.KeySpecs.Length == 0)
                return;

            foreach (var keySpec in cmdInfo.KeySpecs)
            {
                if (!respSession.parseState.TryGetKeySearchArgsFromSimpleKeySpec(keySpec, cmdInfo.IsSubCommand, out var searchArgs))
                    continue;

                var isReadOnly = (keySpec.Flags & KeySpecificationFlags.RO) == KeySpecificationFlags.RO;
                var lockType = isReadOnly ? LockType.Shared : LockType.Exclusive;

                for (var currIdx = searchArgs.firstIdx; currIdx <= searchArgs.lastIdx; currIdx += searchArgs.step)
                {
                    var key = respSession.parseState.GetArgSliceByRef(currIdx);
                    SaveKeyEntryToLock(key, cmdInfo.StoreType, lockType);
                    SaveKeyArgSlice(key);
                }
            }
        }
    }
}