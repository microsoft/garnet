// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Unit tests for <see cref="VectorSetCleanupWorkSet{TValue}"/>.
    /// </summary>
    [TestFixture]
    public class VectorSetCleanupWorkSetTests
    {
        private static byte[] Key(string s) => Encoding.UTF8.GetBytes(s);

        [Test]
        public void AddedWorkIsPendingUntilCompleted()
        {
            var workSet = new VectorSetCleanupWorkSet<int>();

            ClassicAssert.IsFalse(workSet.Contains(Key("a")));
            ClassicAssert.IsFalse(workSet.TryComplete(Key("a")));

            ClassicAssert.IsTrue(workSet.TryAdd(Key("a"), 1));
            ClassicAssert.IsTrue(workSet.Contains(Key("a")));
            ClassicAssert.IsFalse(workSet.TryAdd(Key("a"), 2));

            ClassicAssert.IsTrue(workSet.TryComplete(Key("a")));
            ClassicAssert.IsFalse(workSet.Contains(Key("a")));
        }

        [Test]
        public void EntriesCanBeEnumerated()
        {
            var workSet = new VectorSetCleanupWorkSet<int>();

            _ = workSet.TryAdd(Key("a"), 1);
            _ = workSet.TryAdd(Key("b"), 2);

            var values = new List<int>();
            foreach (var (_, value) in workSet)
            {
                values.Add(value);
            }

            CollectionAssert.AreEquivalent(new[] { 1, 2 }, values);
        }

        [Test]
        public async Task WaitForCompletionBlocksUntilTheEntryIsCompleted()
        {
            var workSet = new VectorSetCleanupWorkSet<int>();

            _ = workSet.TryAdd(Key("a"), 1);

            var waiter = Task.Run(() => workSet.WaitForCompletion(Key("a")));
            ClassicAssert.IsFalse(waiter.IsCompleted);

            _ = workSet.TryComplete(Key("a"));
            await waiter;
        }
    }
}