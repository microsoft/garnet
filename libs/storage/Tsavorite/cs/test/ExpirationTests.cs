// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Expiration
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class ExpirationTests : AllureTestBase
    {
        const int StackAllocMax = 12;
        const int NumRecs = 5000;
        const int RandSeed = 100;
        const int ModifyKey = NumRecs / 2 + 3;
        const int NoValue = -1;
        const int NoKey = -100;
        const int SetIncrement = 1000000;
        const int MinValueLen = 2 * sizeof(int); // SpanByte Length plus at least one int element

        static int TestValue => GetValue(ModifyKey);

        static int GetRandomLength(Random rng) => rng.Next(StackAllocMax) + 1;    // +1 for 0 to StackAllocMax inclusive

        static int GetValue(int key) => key + NumRecs * 10;

        [Flags]
        internal enum Funcs
        {
            Invalid = 0, NeedInitialUpdate = 0x0001, NeedCopyUpdate = 0x0002, InPlaceUpdater = 0x0004, InitialUpdater = 0x0008, CopyUpdater = 0x0010,
            SingleReader = 0x0020, ConcurrentReader = 0x0040,
            RMWCompletionCallback = 0x0100, ReadCompletionCallback = 0x0200,
            SkippedCopyUpdate = NeedCopyUpdate | RMWCompletionCallback,
            DidCopyUpdate = NeedCopyUpdate | CopyUpdater,
            DidCopyUpdateCC = DidCopyUpdate | RMWCompletionCallback,
            DidInitialUpdate = NeedInitialUpdate | InitialUpdater
        };

        internal enum ExpirationResult
        {
            None,                       // Default value
            Incremented,                // Initial increment was done
            ExpireDelete,               // Record was expired so deleted
            ExpireRollover,             // Record was expired and reinitialized within the ISessionFunctions call
            Updated,                    // Record was updated normally
            NotUpdated,                 // Record was not updated
            Deleted,                    // Record was expired with AndStop (no reinitialization done)
            DeletedThenUpdated,         // Record was expired then InitialUpdate'd within the original record space
            DeletedThenUpdateRejected,  // Record was expired then InitialUpdate within the original record space was rejected
            DeletedThenInserted,        // Record was expired and not InitialUpdate'd within the original record space, so RMW inserted a record with the reinitialized value
            NotDeleted
        };

        public struct ExpirationInput
        {
            internal int value;
            internal int comparisonValue;
            internal TestOp testOp;
        }

        public struct ExpirationOutput
        {
            internal int retrievedValue;
            internal Funcs functionsCalled;
            internal ExpirationResult result;

            internal void AddFunc(Funcs func) => functionsCalled |= func;

            public override readonly string ToString() => $"value {retrievedValue}, funcs [{functionsCalled}], result {result}";
        }

        // The operations here describe intentions by the app. They are implemented entirely in Functions and do not have first-class support in Tsavorite
        // other than handling returns from Functions methods appropriately.
        internal enum TestOp
        {
#pragma warning disable format
            None,                               // Default value; not otherwise used
            Increment,                          // Increment a counter
            PassiveExpire,                      // Increment a counter but do not expire the record explicitly; let Read encounter it as expired.
                                                //  This simulates a timeout-based (rather than counter-based) expiration.
            ExpireDelete,                       // Increment a counter and expire the record by deleting it
                                                //  Mutable:
                                                //      IPU sets rwmInfo.DeleteRecord and returns false; we see this and TryRemoveDeletedHashEntry and return SUCCESS
                                                //  OnDisk:
                                                //      CU sets rmwInfo.DeleteRecord and returns false; Tsavorite sets the Tombstone and the operation proceeds as normal otherwise
            ExpireRollover,                     // Increment a counter and expire the record by rolling it over (back to 1)
                                                //  Mutable
                                                //      IPU - InPlace (input len <= record len): Execute IU logic in current space; return true from IPU
                                                //          - (TODO with revivication) NewRecord (input len > record len): Return false from IPU, true from NCU, set in CU
                                                //  OnDisk:
                                                //      CU  - executes IU logic
            SetIfKeyExists,                     // Update a record if the key exists, but do not create a new record
                                                //  Mutable:
                                                //      Exists: update and return true from IPU
                                                //      NotExists: return false from NIU
                                                //  OnDisk:
                                                //      Exists: return true from NCU, update in CU
                                                //      NotExists: return false from NIU
            SetIfKeyNotExists,                  // Create a new record if the key does not exist, but do not update an existing record
                                                //  Mutable:
                                                //      Exists: no-op and return true from IPU
                                                //      NotExists: return true from NIU, set in IU
                                                //  OnDisk:
                                                //      Exists: return false from NCU
                                                //      NotExists: return true from NIU, set in IU
            SetIfValueEquals,                   // Update the record for a key if the current value equals a specified value
                                                //  Mutable:
                                                //      Equals: update and return true from IPU
                                                //      NotEquals: no-op and return true from IPU
                                                //  OnDisk:
                                                //      Equals: return true from NCU, update in CU
                                                //      NotEquals: return false from NCU
                                                //  NotExists: Return false from NIU
            SetIfValueNotEquals,                // Update the record for a key if the current value does not equal a specified value
                                                //  Mutable:
                                                //      Equals: no-op and return true from IPU
                                                //      NotEquals: update and return true from IPU
                                                //  OnDisk:
                                                //      Equals: return false from NCU
                                                //      NotEquals: return true from NCU, update in CU
                                                //  NotExists: Return false from NIU

            DeleteIfValueEqualsThenUpdate,      // Delete the record for a key if the current value equals a specified value, then update in the same record space
            DeleteIfValueEqualsThenInsert,      // Delete the record for a key if the current value equals a specified value, then insert a new record 
            DeleteIfValueEqualsAndStop,         // Delete the record for a key if the current value equals a specified value, then stop (leave the tombstoned record there)
                                                //  Mutable:
                                                //      Equals: Same as ExpireDelete
                                                //      NotEquals: no-op and return true from IPU
                                                //  OnDisk:
                                                //      Equals: Same as ExpireDelete
                                                //      NotEquals: return false from NCU
                                                //  NotExists: Return false from NIU

            DeleteIfValueNotEqualsThenUpdate,   // Delete the record for a key if the current value does not equal a specified value, then update in the same record space
            DeleteIfValueNotEqualsThenInsert,   // Delete the record for a key if the current value does not equal a specified value, then insert a new record 
            DeleteIfValueNotEqualsAndStop,      // Delete the record for a key if the current value does not equal a specified value, then stop (leave the tombstoned record there)
                                                //  Mutable:
                                                //      Equals: no-op and return true from IPU
                                                //      NotEquals: Same as ExpireDelete
                                                //  Mutable:
                                                //      Equals: return false from NCU
                                                //      NotEquals: Same as ExpireDelete
                                                //  NotExists: Return false from NIU

            Revivify                            // TODO - NYI: An Update or RMW operation encounters a tombstoned record of >= size of the new value, so the record is updated.
                                                //      Test with newsize < space, then again with newsize == original space
                                                //          Verify tombstone is revivified on later insert (SingleWriter called within Tsavorite-acquired RecordInfo.SpinLock)
                                                //          Verify tombstone is revivified on later simple RMW (IU called within Tsavorite-acquired RecordInfo.SpinLock)
#pragma warning restore format
        };

        public class ExpirationFunctions : SessionFunctionsBase<SpanByte, SpanByte, ExpirationInput, ExpirationOutput, Empty>
        {
            private static unsafe void VerifyValue(int key, ref SpanByte valueSpanByte)
            {
                Span<int> valueSpan = valueSpanByte.AsSpan<int>();
                for (int j = 0; j < valueSpan.Length; j++)
                    ClassicAssert.AreEqual(key, valueSpan[j]);
            }

            static bool IsExpired(int key, int value) => value == GetValue(key) + 2;

            public override unsafe bool NeedInitialUpdate(ref SpanByte key, ref ExpirationInput input, ref ExpirationOutput output, ref RMWInfo rmwInfo)
            {
                output.AddFunc(Funcs.NeedInitialUpdate);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                    case TestOp.PassiveExpire:
                    case TestOp.ExpireDelete:
                    case TestOp.ExpireRollover:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    case TestOp.SetIfKeyExists:
                        return false;
                    case TestOp.SetIfKeyNotExists:
                        return true;
                    case TestOp.SetIfValueEquals:
                    case TestOp.SetIfValueNotEquals:
                        return false;
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                        // This means we are on the "handle expiration" sequence after IPU/CU
                        if (output.result == ExpirationResult.Deleted)
                        {
                            // Reject the update-in-original-record
                            output.result = ExpirationResult.DeletedThenUpdateRejected;
                            // return false;
                        }
                        return output.result == ExpirationResult.DeletedThenUpdateRejected;
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                        // This means we are on the "handle expiration" sequence after IPU/CU
                        return output.result == ExpirationResult.Deleted;
                    case TestOp.DeleteIfValueEqualsAndStop:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
                        // AndStop should have returned from RMW instead during the test, but this is legitimately called from VerifyKeyNotCreated
                        return false;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            public override unsafe bool NeedCopyUpdate(ref SpanByte key, ref ExpirationInput input, ref SpanByte oldValue, ref ExpirationOutput output, ref RMWInfo rmwInfo)
            {
                int field1 = oldValue.AsSpan<int>()[0];

                output.AddFunc(Funcs.NeedCopyUpdate);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                    case TestOp.PassiveExpire:
                    case TestOp.ExpireDelete:
                    case TestOp.ExpireRollover:
                    case TestOp.SetIfKeyExists:
                        return true;
                    case TestOp.SetIfKeyNotExists:
                        return false;
                    case TestOp.SetIfValueEquals:
                        return field1 == input.comparisonValue;
                    case TestOp.SetIfValueNotEquals:
                        return field1 != input.comparisonValue;
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueEqualsAndStop:
                        return field1 == input.comparisonValue;
                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
                        return field1 != input.comparisonValue;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            /// <inheritdoc/>
            public override unsafe bool CopyUpdater(ref SpanByte key, ref ExpirationInput input, ref SpanByte oldValue, ref SpanByte newValue, ref ExpirationOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                int key1 = key.AsSpan<int>()[0];
                int oldField1 = oldValue.AsSpan<int>()[0];
                ref int newField1 = ref newValue.AsSpan<int>()[0];

                output.AddFunc(Funcs.CopyUpdater);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                        ClassicAssert.AreEqual(GetValue(key1), oldField1);
                        goto case TestOp.PassiveExpire;
                    case TestOp.PassiveExpire:
                        newField1 = oldField1 + 1;
                        output.result = ExpirationResult.Incremented;
                        return true;
                    case TestOp.ExpireDelete:
                        ClassicAssert.AreEqual(GetValue(key1) + 1, oldField1);      // For this test we only call this operation when the value will expire
                        newField1 = oldField1 + 1;
                        rmwInfo.Action = RMWAction.ExpireAndResume;
                        output.result = ExpirationResult.ExpireDelete;
                        return true;
                    case TestOp.ExpireRollover:
                        ClassicAssert.AreEqual(GetValue(key1) + 1, oldField1);      // For this test we only call this operation when the value will expire
                        newField1 = GetValue(key1);
                        output.result = ExpirationResult.ExpireRollover;
                        output.retrievedValue = newField1;
                        return true;
                    case TestOp.SetIfKeyExists:
                        newField1 = input.value;
                        output.result = ExpirationResult.Updated;
                        output.retrievedValue = newField1;
                        return true;
                    case TestOp.SetIfKeyNotExists:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    case TestOp.SetIfValueEquals:
                        if (oldField1 == input.comparisonValue)
                        {
                            newField1 = input.value;
                            output.result = ExpirationResult.Updated;
                            output.retrievedValue = newField1;
                        }
                        else
                        {
                            output.result = ExpirationResult.NotUpdated;
                            output.retrievedValue = oldField1;
                        }
                        return true;
                    case TestOp.SetIfValueNotEquals:
                        if (oldField1 != input.comparisonValue)
                        {
                            newField1 = input.value;
                            output.result = ExpirationResult.Updated;
                            output.retrievedValue = newField1;
                        }
                        else
                        {
                            output.result = ExpirationResult.NotUpdated;
                            output.retrievedValue = oldField1;
                        }
                        return true;
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueEqualsAndStop:
                        if (oldField1 == input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        Assert.Fail("Should have returned false from NeedCopyUpdate");
                        return false;
                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
                        if (oldField1 != input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueNotEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        Assert.Fail("Should have returned false from NeedCopyUpdate");
                        return false;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            public override unsafe bool InitialUpdater(ref SpanByte key, ref ExpirationInput input, ref SpanByte value, ref ExpirationOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                ref int field1 = ref value.AsSpan<int>()[0];

                output.AddFunc(Funcs.InitialUpdater);
                field1 = input.value;

                // If InPlaceUpdater returned Delete, let the caller know both operations happened. Similarly, we may be 
                output.result = output.result switch
                {
                    ExpirationResult.Deleted => ExpirationResult.DeletedThenUpdated,
                    ExpirationResult.DeletedThenUpdated => ExpirationResult.DeletedThenInserted,
                    ExpirationResult.DeletedThenUpdateRejected => ExpirationResult.DeletedThenInserted,
                    _ => ExpirationResult.Updated
                };
                output.retrievedValue = field1;

                // If this is the first InitialUpdater after a Delete and the testOp is *ThenInsert, we have to fail the first InitialUpdater
                // (which is the InitialUpdater call on the deleted record's space) and will pass the second InitialUpdater (which is into a new record).
                if (output.result == ExpirationResult.DeletedThenUpdated
                    && (input.testOp == TestOp.DeleteIfValueEqualsThenInsert || input.testOp == TestOp.DeleteIfValueNotEqualsThenInsert))
                    return false;
                return true;
            }

            public override unsafe bool InPlaceUpdater(ref SpanByte key, ref ExpirationInput input, ref SpanByte value, ref ExpirationOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                int key1 = key.AsSpan<int>()[0];
                ref int field1 = ref value.AsSpan<int>()[0];

                output.AddFunc(Funcs.InPlaceUpdater);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                        ClassicAssert.AreEqual(GetValue(key1), field1);
                        goto case TestOp.PassiveExpire;
                    case TestOp.PassiveExpire:
                        ++field1;
                        output.result = ExpirationResult.Incremented;
                        return true;
                    case TestOp.ExpireDelete:
                        ClassicAssert.AreEqual(GetValue(key1) + 1, field1);            // For this test we only call this operation when the value will expire
                        ++field1;
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                        output.result = ExpirationResult.ExpireDelete;
                        return false;
                    case TestOp.ExpireRollover:
                        ClassicAssert.AreEqual(GetValue(key1) + 1, field1);            // For this test we only call this operation when the value will expire
                        field1 = GetValue(key1);
                        output.result = ExpirationResult.ExpireRollover;
                        output.retrievedValue = field1;
                        return true;
                    case TestOp.SetIfKeyExists:
                        field1 = input.value;
                        output.result = ExpirationResult.Updated;
                        output.retrievedValue = field1;
                        return true;
                    case TestOp.SetIfKeyNotExists:
                        // No-op
                        return true;
                    case TestOp.SetIfValueEquals:
                        if (field1 == input.comparisonValue)
                        {
                            field1 = input.value;
                            output.result = ExpirationResult.Updated;
                        }
                        else
                            output.result = ExpirationResult.NotUpdated;
                        output.retrievedValue = field1;
                        return true;
                    case TestOp.SetIfValueNotEquals:
                        if (field1 != input.comparisonValue)
                        {
                            field1 = input.value;
                            output.result = ExpirationResult.Updated;
                        }
                        else
                            output.result = ExpirationResult.NotUpdated;
                        output.retrievedValue = field1;
                        return true;
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueEqualsAndStop:
                        if (field1 == input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        output.result = ExpirationResult.NotDeleted;
                        output.retrievedValue = field1;
                        return true;

                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
                        if (field1 != input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueNotEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        output.result = ExpirationResult.NotDeleted;
                        output.retrievedValue = field1;
                        return true;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            public override void RMWCompletionCallback(ref SpanByte key, ref ExpirationInput input, ref ExpirationOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                output.AddFunc(Funcs.RMWCompletionCallback);
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref ExpirationInput input, ref ExpirationOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                output.AddFunc(Funcs.ReadCompletionCallback);
            }

            /// <inheritdoc/>
            public override int GetRMWModifiedValueLength(ref SpanByte value, ref ExpirationInput input) => value.TotalSize;

            /// <inheritdoc/>
            public override int GetRMWInitialValueLength(ref ExpirationInput input) => MinValueLen;

            /// <inheritdoc/>
            public override int GetUpsertValueLength(ref SpanByte value, ref ExpirationInput input) => value.TotalSize;

            // Read functions
            public override unsafe bool SingleReader(ref SpanByte key, ref ExpirationInput input, ref SpanByte value, ref ExpirationOutput output, ref ReadInfo readInfo)
            {
                int key1 = key.AsSpan<int>()[0];
                ref int field1 = ref value.AsSpan<int>()[0];

                output.AddFunc(Funcs.SingleReader);
                if (IsExpired(key1, field1))
                {
                    readInfo.Action = ReadAction.Expire;
                    return false;
                }
                output.retrievedValue = field1;
                return true;
            }

            public override unsafe bool ConcurrentReader(ref SpanByte key, ref ExpirationInput input, ref SpanByte value, ref ExpirationOutput output, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                int key1 = key.AsSpan<int>()[0];
                ref int field1 = ref value.AsSpan<int>()[0];

                output.AddFunc(Funcs.ConcurrentReader);
                if (IsExpired(key1, field1))
                {
                    readInfo.Action = ReadAction.Expire;
                    return false;
                }
                output.retrievedValue = field1;
                return true;
            }

            // Upsert functions
            public override bool SingleWriter(ref SpanByte key, ref ExpirationInput input, ref SpanByte src, ref SpanByte dst, ref ExpirationOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
                => SpanByteFunctions<Empty>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

            public override bool ConcurrentWriter(ref SpanByte key, ref ExpirationInput input, ref SpanByte src, ref SpanByte dst, ref ExpirationOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
                => SpanByteFunctions<Empty>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);
        }

        IDevice log;
        ExpirationFunctions functions;
        TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        ClientSession<SpanByte, SpanByte, ExpirationInput, ExpirationOutput, Empty, ExpirationFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;
        BasicContext<SpanByte, SpanByte, ExpirationInput, ExpirationOutput, Empty, ExpirationFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MemorySize = 1L << 19,
                PageSize = 1L << 14
            }, StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new ExpirationFunctions();
            session = store.NewSession<ExpirationInput, ExpirationOutput, Empty, ExpirationFunctions>(functions);
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        private unsafe void Populate(Random rng)
        {
            // Single alloc outside the loop, to the max length we'll need.
            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[StackAllocMax];

            for (int i = 0; i < NumRecs; i++)
            {
                keySpan[0] = i;
                var keySpanByte = keySpan.AsSpanByte();

                var valueLen = GetRandomLength(rng);
                for (int j = 0; j < valueLen; j++)
                    valueSpan[j] = GetValue(i);
                var valueSpanByte = valueSpan.AsSpanByte();

                bContext.Upsert(ref keySpanByte, ref valueSpanByte, Empty.Default);
            }
        }

        private unsafe ExpirationOutput GetRecord(int key, Status expectedStatus, FlushMode flushMode)
        {
            Span<int> keySpan = [key];
            var keySpanByte = keySpan.AsSpanByte();
            ExpirationOutput output = new();

            var status = bContext.Read(ref keySpanByte, ref output, Empty.Default);
            if (status.IsPending)
            {
                ClassicAssert.AreNotEqual(FlushMode.NoFlush, flushMode);
                bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }

            ClassicAssert.AreEqual(expectedStatus, status, status.ToString());
            return output;
        }

        private unsafe ExpirationOutput ExecuteRMW(int key, ref ExpirationInput input, FlushMode flushMode, Status expectedStatus = default)
        {
            Span<int> keySpan = [key];
            var keySpanByte = keySpan.AsSpanByte();

            ExpirationOutput output = new();
            var status = bContext.RMW(ref keySpanByte, ref input, ref output);
            if (status.IsPending)
            {
                ClassicAssert.AreNotEqual(FlushMode.NoFlush, flushMode);
                bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }

            ClassicAssert.AreEqual(expectedStatus, status, status.ToString());
            ClassicAssert.AreEqual(expectedStatus.Expired, status.Expired, status.ToString());
            return output;
        }

        private static Status GetMutableVsOnDiskStatus(FlushMode flushMode)
        {
            // The behavior is different for OnDisk vs. mutable:
            //  - Mutable results in a call to IPU which returns true, so RMW returns InPlaceUpdatedRecord.
            //  - Otherwise it results in a call to NeedCopyUpdate which returns false, so RMW returns Found
            return new(flushMode == FlushMode.NoFlush ? StatusCode.InPlaceUpdatedRecord : StatusCode.Found);
        }

        void InitialIncrement()
        {
            Populate(new Random(RandSeed));
            InitialRead(FlushMode.NoFlush, afterIncrement: false);
            IncrementValue(TestOp.Increment, FlushMode.NoFlush);
        }

        private void InitialRead(FlushMode flushMode, bool afterIncrement)
        {
            var output = GetRecord(ModifyKey, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(ModifyKey) + (afterIncrement ? 1 : 0), output.retrievedValue);
            Funcs expectedFuncs = flushMode switch
            {
                FlushMode.NoFlush => Funcs.ConcurrentReader,
                FlushMode.ReadOnly => Funcs.SingleReader,
                FlushMode.OnDisk => Funcs.SingleReader | Funcs.ReadCompletionCallback,
                _ => Funcs.Invalid
            };
            ClassicAssert.AreNotEqual(expectedFuncs, Funcs.Invalid, $"Unexpected flushmode {flushMode}");
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
        }

        private void IncrementValue(TestOp testOp, FlushMode flushMode)
        {
            var key = ModifyKey;
            ExpirationInput input = new() { testOp = testOp };
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.Incremented, output.result);
        }

        private void MaybeEvict(FlushMode flushMode)
        {
            if (flushMode == FlushMode.NoFlush)
                return;
            if (flushMode == FlushMode.ReadOnly)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            else
                store.Log.FlushAndEvict(wait: true);
            InitialRead(flushMode, afterIncrement: true);
        }

        private void VerifyKeyNotCreated(TestOp testOp, FlushMode flushMode)
        {
            var key = NoKey;
#if false // TODO - Add to ExpirationFunctions the knowledge of whether we're calling from here or the main test part
            // Key doesn't exist - no-op
            ExpirationInput input = new() { testOp = testOp, value = NoValue, comparisonValue = GetValue(key) + 1 };

            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, new(StatusCode.NotFound));
            ClassicAssert.AreEqual(Funcs.NeedInitialUpdate, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.None, output.result);
            ClassicAssert.AreEqual(0, output.retrievedValue);
#endif

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);
        }

        private static Funcs GetExpectedFuncs(FlushMode flushMode, Funcs noFlush, Funcs readOnly, Funcs onDisk)
        {
            var expectedFuncs = flushMode switch
            {
                FlushMode.NoFlush => noFlush,
                FlushMode.ReadOnly => readOnly,
                FlushMode.OnDisk => onDisk,
                _ => Funcs.Invalid
            };
            ClassicAssert.AreNotEqual(expectedFuncs, Funcs.Invalid, $"Unexpected flushmode {flushMode}");
            return expectedFuncs;
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void PassiveExpireTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            IncrementValue(TestOp.PassiveExpire, flushMode);
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
            GetRecord(ModifyKey, new(StatusCode.NotFound | StatusCode.Expired), flushMode);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void ExpireDeleteTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.ExpireDelete;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord | StatusCode.Expired) : new(StatusCode.CopyUpdatedRecord);

            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Increment/Delete it
            ExpirationInput input = new() { testOp = testOp };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.ExpireDelete, output.result);

            // Verify it's not there
            if (flushMode == FlushMode.NoFlush)
                GetRecord(key, new(StatusCode.NotFound), flushMode);    // Expiration was IPU-deletion
            else
                GetRecord(key, new(StatusCode.NotFound | StatusCode.Expired), flushMode);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void ExpireRolloverTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.ExpireRollover;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Increment/Rollover to initial state
            ExpirationInput input = new() { testOp = testOp };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.ExpireRollover, output.result);
            ClassicAssert.AreEqual(GetValue(key), output.retrievedValue);

            // Verify it's there with initial state
            output = GetRecord(key, new(StatusCode.Found), FlushMode.NoFlush /* update was appended */);
            ClassicAssert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfKeyExistsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfKeyExists;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Key exists - update it
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.Updated, output.result);
            ClassicAssert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with updated value
            output = GetRecord(key, new(StatusCode.Found), FlushMode.NoFlush /* update was appended */);
            ClassicAssert.AreEqual(input.value, output.retrievedValue);

            // Key doesn't exist - no-op
            key += SetIncrement;
            input = new() { testOp = testOp, value = GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, new(StatusCode.NotFound));

            // If the hashing happens to have the HashEntryInfo pointing below BeginAddress (i.e. there were no collisions),
            // RMWCompletionCallback won't be set. Currently it is.
            expectedFuncs = Funcs.NeedInitialUpdate;
            if (flushMode == FlushMode.OnDisk)
                expectedFuncs |= Funcs.RMWCompletionCallback;
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfKeyNotExistsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfKeyNotExists;
            var key = ModifyKey;

            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Key exists - no-op
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);

            // Verify it's there with unchanged value
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Key doesn't exist - create it
            key += SetIncrement;
            input = new() { testOp = testOp, value = GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, new(StatusCode.NotFound | StatusCode.CreatedRecord));

            // If the hashing happens to have the HashEntryInfo pointing below BeginAddress (i.e. there were no collisions),
            // RMWCompletionCallback won't be set. Currently it is.
            expectedFuncs = Funcs.DidInitialUpdate;
            if (flushMode == FlushMode.OnDisk)
                expectedFuncs |= Funcs.RMWCompletionCallback;
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);

            ClassicAssert.AreEqual(ExpirationResult.Updated, output.result);
            ClassicAssert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with specified value
            output = GetRecord(key, new(StatusCode.Found), FlushMode.NoFlush /* was just added */);
            ClassicAssert.AreEqual(input.value, output.retrievedValue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfValueEqualsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfValueEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Value equals - update it
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.Updated, output.result);
            ClassicAssert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with updated value
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(input.value, output.retrievedValue);

            // Value doesn't equal - no-op
            key += 1;   // We modified ModifyKey so get the next-higher key
            input = new() { testOp = testOp, value = -2, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotUpdated : ExpirationResult.None, output.result);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfValueNotEqualsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfValueNotEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Value equals
            ExpirationInput input = new() { testOp = testOp, value = -2, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotUpdated : ExpirationResult.None, output.result);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) + 1 : 0, output.retrievedValue);

            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Value doesn't equal
            input = new() { testOp = testOp, value = GetValue(key) + SetIncrement, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.Updated, output.result);
            ClassicAssert.AreEqual(GetValue(key) + SetIncrement, output.retrievedValue);

            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key) + SetIncrement, output.retrievedValue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteThenUpdateTest([Values] FlushMode flushMode, [Values] KeyEquality keyEquality, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            bool isEqual = keyEquality == KeyEquality.Equal;
            TestOp testOp = isEqual ? TestOp.DeleteIfValueEqualsThenUpdate : TestOp.DeleteIfValueNotEqualsThenUpdate;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush
                    ? new(StatusCode.NotFound | StatusCode.Expired | StatusCode.InPlaceUpdatedRecord)
                    : new(StatusCode.NotFound | StatusCode.Expired | StatusCode.CreatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Target value - if isEqual, the actual value of the key, else a bogus value. Delete the record, then re-initialize it from input
            var reinitValue = GetValue(key) + 1000;
            ExpirationInput input = new() { testOp = testOp, value = reinitValue, comparisonValue = isEqual ? GetValue(key) + 1 : -1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater | Funcs.DidInitialUpdate,
                                            readOnly: Funcs.DidCopyUpdate | Funcs.DidInitialUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC | Funcs.DidInitialUpdate);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.DeletedThenUpdated, output.result);

            // Verify we did the reInitialization (this test should always have restored it with its initial values)
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(reinitValue, output.retrievedValue);

            // No-op value - if isEqual, a bogus value, else the actual value of the key. Does nothing to the record.
            key += 1;   // We deleted ModifyKey so get the next-higher key
            input = new() { testOp = testOp, comparisonValue = isEqual ? -1 : GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteThenInsertTest([Values] FlushMode flushMode, [Values] KeyEquality keyEquality, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            bool isEqual = keyEquality == KeyEquality.Equal;
            TestOp testOp = isEqual ? TestOp.DeleteIfValueEqualsThenInsert : TestOp.DeleteIfValueNotEqualsThenInsert;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ?
                new(StatusCode.NotFound | StatusCode.InPlaceUpdatedRecord | StatusCode.Expired) :
                new(StatusCode.NotFound | StatusCode.Expired | StatusCode.CreatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Target value - if isEqual, the actual value of the key, else a bogus value. Delete the record, then re-initialize it from input
            var reinitValue = GetValue(key) + 1000;
            ExpirationInput input = new() { testOp = testOp, value = reinitValue, comparisonValue = isEqual ? GetValue(key) + 1 : -1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.DidInitialUpdate | Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidInitialUpdate | Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidInitialUpdate | Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.DeletedThenInserted, output.result);

            // Verify we did the reInitialization (this test should always have restored it with its initial values)
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(reinitValue, output.retrievedValue);
            // No-op value - if isEqual, a bogus value, else the actual value of the key. Does nothing to the record.
            key += 1;   // We deleted ModifyKey so get the next-higher key
            input = new() { testOp = testOp, comparisonValue = isEqual ? -1 : GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteAndCancelIfValueEqualsTest([Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            const TestOp testOp = TestOp.DeleteIfValueEqualsAndStop;
            var key = ModifyKey;
            FlushMode flushMode = FlushMode.NoFlush;
            Status expectedFoundRmwStatus = new(StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Value equals - delete it
            ExpirationInput input = new() { testOp = testOp, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            ClassicAssert.AreEqual(Funcs.InPlaceUpdater, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.Deleted, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);

            // Value doesn't equal - no-op
            key += 1;   // We deleted ModifyKey so get the next-higher key
            input = new() { testOp = testOp, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? Funcs.InPlaceUpdater : Funcs.SkippedCopyUpdate, output.functionsCalled);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteIfValueNotEqualsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.DeleteIfValueNotEqualsAndStop;
            var key = ModifyKey;

            // For this test, IPU will Cancel rather than go to the IPU path
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord | StatusCode.Expired) : new(StatusCode.CreatedRecord | StatusCode.Expired);
            ClassicAssert.IsTrue(expectedFoundRmwStatus.Expired, expectedFoundRmwStatus.ToString());

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);

            // Value equals - no-op
            ExpirationInput input = new() { testOp = testOp, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            ClassicAssert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) + 1 : 0, output.retrievedValue);

            // Verify it's there with unchanged value
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            ClassicAssert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Value doesn't equal - delete it
            input = new() { testOp = testOp, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            ClassicAssert.AreEqual(expectedFuncs, output.functionsCalled);
            ClassicAssert.AreEqual(ExpirationResult.Deleted, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);
        }
    }
}