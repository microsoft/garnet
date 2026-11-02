// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class GlobUtilsTests : TestBase
    {
        [TestCase("", "", true)]
        [TestCase("*", "", true)]
        [TestCase("***", "", true)]
        [TestCase("?", "", false)]
        [TestCase("", "a", false)]
        [TestCase("a*", "a", true)]
        [TestCase("a**", "a", true)]
        [TestCase("*ab*ac", "ababac", true)]
        [TestCase("*ab*ac", "ababax", false)]
        [TestCase("*?a", "aaa", true)]
        [TestCase(@"a\*b", "a*b", true)]
        [TestCase(@"a\?b", "a?b", true)]
        [TestCase(@"a\[b", "a[b", true)]
        [TestCase(@"a\\b", @"a\b", true)]
        [TestCase("a\\", "a\\", true)]
        [TestCase("[a-c]", "b", true)]
        [TestCase("[c-a]", "b", true)]
        [TestCase("[^a-c]", "z", true)]
        [TestCase("[^a-c]", "b", false)]
        [TestCase(@"[\]]", "]", true)]
        [TestCase(@"[\-]", "-", true)]
        [TestCase("[]", "a", false)]
        [TestCase("[^]", "a", true)]
        [TestCase("[", "a", false)]
        [TestCase("[^", "a", true)]
        [TestCase("[ab", "a", true)]
        [TestCase("[ab", "z", false)]
        [TestCase("*[ab]?", "zzab", true)]
        [TestCase("*[ab]?", "zzacx", false)]
        [TestCase("A[B-D]?", "acx", true, true)]
        [TestCase("A[B-D]?", "acx", false, false)]
        [TestCase(@"[\A]", "a", false, true)]
        public void MatchesGlobSyntax(string pattern, string key, bool expected, bool ignoreCase = false)
            => ClassicAssert.AreEqual(expected, Match(Encoding.ASCII.GetBytes(pattern), Encoding.ASCII.GetBytes(key), ignoreCase));

        [Test]
        public void MatchesBinaryBytes()
        {
            ClassicAssert.IsTrue(Match([(byte)'*', 0, (byte)'?', 255], [128, 0, 129, 255]));
            ClassicAssert.IsFalse(Match([(byte)'*', 0, (byte)'?', 255], [128, 0, 129, 254]));
        }

        [Test]
        public unsafe void RespectsLogicalBufferLengths()
        {
            byte[] pattern = [(byte)'a', (byte)'*', (byte)'*', 0];
            byte[] key = [(byte)'a', (byte)'b'];
            fixed (byte* patternPtr = pattern)
            fixed (byte* keyPtr = key)
            {
                ClassicAssert.IsTrue(GlobUtils.Match(patternPtr, 1, keyPtr, 1));
                ClassicAssert.IsFalse(GlobUtils.Match(patternPtr, 1, keyPtr, 2));
                ClassicAssert.IsTrue(GlobUtils.Match(patternPtr + 1, 1, null, 0));
                ClassicAssert.IsTrue(GlobUtils.Match(null, 0, null, 0));
            }

            byte[] characterClass = [(byte)'[', (byte)'a', (byte)'b', (byte)']'];
            fixed (byte* patternPtr = characterClass)
            fixed (byte* keyPtr = key)
            {
                ClassicAssert.IsFalse(GlobUtils.Match(patternPtr, 1, keyPtr, 1));
                ClassicAssert.IsTrue(GlobUtils.Match(patternPtr, 2, keyPtr, 1));
                ClassicAssert.IsFalse(GlobUtils.Match(patternPtr, 2, keyPtr + 1, 1));
            }
        }

        [Test]
        public void MatchesDeepWildcardPatternsOnSmallStack()
        {
            var pattern = Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat("*a", 32_768)));
            var key = Encoding.ASCII.GetBytes(new string('a', 32_768));
            var matched = false;
            var thread = new Thread(() => matched = Match(pattern, key), 128 * 1024) { IsBackground = true };
            thread.Start();
            ClassicAssert.IsTrue(thread.Join(TimeSpan.FromSeconds(10)));
            ClassicAssert.IsTrue(matched);
        }

        [Test]
        [CancelAfter(10_000)]
        public void RejectsAdversarialNonMatches()
        {
            var pattern = Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat("*a", 256)) + "b");
            var key = Encoding.ASCII.GetBytes(new string('a', 1024));
            ClassicAssert.IsFalse(Match(pattern, key));
            ClassicAssert.IsFalse(Match(Encoding.ASCII.GetBytes("*" + new string('a', 1024) + "b"),
                Encoding.ASCII.GetBytes(new string('a', 4096))));
        }

        [Test]
        public void WildcardRetriesAgreeWithExhaustiveReference()
        {
            for (var patternLength = 0; patternLength <= 4; patternLength++)
            {
                for (var patternValue = 0; patternValue < 1 << (2 * patternLength); patternValue++)
                {
                    var pattern = new byte[patternLength];
                    for (var i = 0; i < pattern.Length; i++)
                        pattern[i] = "ab?*"u8[(patternValue >> (2 * i)) & 3];

                    for (var keyLength = 0; keyLength <= 4; keyLength++)
                    {
                        for (var keyValue = 0; keyValue < 1 << keyLength; keyValue++)
                        {
                            var key = new byte[keyLength];
                            for (var i = 0; i < key.Length; i++)
                                key[i] = (byte)('a' + ((keyValue >> i) & 1));

                            ClassicAssert.AreEqual(ReferenceMatch(pattern, key), Match(pattern, key),
                                $"Pattern '{Encoding.ASCII.GetString(pattern)}', key '{Encoding.ASCII.GetString(key)}'");
                        }
                    }
                }
            }
        }

        private static bool ReferenceMatch(byte[] pattern, byte[] key)
        {
            var matches = new bool[pattern.Length + 1, key.Length + 1];
            matches[0, 0] = true;
            for (var p = 1; p <= pattern.Length; p++)
            {
                for (var k = 0; k <= key.Length; k++)
                {
                    matches[p, k] = pattern[p - 1] == '*'
                        ? matches[p - 1, k] || (k > 0 && matches[p, k - 1])
                        : k > 0 && matches[p - 1, k - 1] && (pattern[p - 1] == '?' || pattern[p - 1] == key[k - 1]);
                }
            }
            return matches[pattern.Length, key.Length];
        }

        private static unsafe bool Match(byte[] pattern, byte[] key, bool ignoreCase = false)
        {
            fixed (byte* patternPtr = pattern)
            fixed (byte* keyPtr = key)
                return GlobUtils.Match(patternPtr, pattern.Length, keyPtr, key.Length, ignoreCase);
        }
    }
}