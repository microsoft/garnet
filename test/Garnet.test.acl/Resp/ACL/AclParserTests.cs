// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Allure.NUnit;
using Garnet.server.ACL;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests for the <see cref="ACLParser"/>.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    internal class AclParserTests : AclTest
    {
        /// <summary>
        /// Test cases for scenarios where ACLs reduce.
        /// </summary>
        [Test, MaxTime(1000)]
        [TestCase("user 1-command on +set", "+set")]
        [TestCase("user 2-command on +set +get", "+set +get")]
        [TestCase("user 3-command-duplicates-reduce on +set +set", "+set")]
        [TestCase("user 4-command-duplicates-complicated on +set +set -set +set", "+set")]
        [TestCase("user 5-command-duplicates-complicated on +get -set +set", "+get +set")]
        [TestCase("user 6-category on +@keyspace", "+@keyspace")]
        [TestCase("user 7-category-reduces on +@all", "+@all")]
        [TestCase("user 7-category-reduces on -@all", "")]
        [TestCase("user 8-category-reduces on -@all +@keyspace", "+@keyspace")]
        [TestCase("user 9-category-reduces on +@all +@keyspace", "+@all")]
        [TestCase("user 10-category-command-reduces on +@keyspace +del", "+@keyspace")]
        [TestCase("user 11-category-command-reduces on +@keyspace +set", "+@keyspace +set")]
        [TestCase("user 12-category-command-reduces on +@keyspace +del -del", "+@keyspace -del")]
        [TestCase("user 13-category-command-reduces on +del -@keyspace", "")]
        [TestCase("user 14-category-command-reduces on -del +@keyspace", "+@keyspace")]
        [TestCase("user 15-category-command-reduces on +set +@keyspace", "+set +@keyspace")]
        [TestCase("user 16-category-command-reduces on +@all +set", "+@all")]
        [TestCase("user 17-category-command-reduces on +@all +set +get +incr -decr", "+@all -decr")]
        [TestCase("user 18-category-command-reduces on -@all +set", "+set")]
        [TestCase("user 19-category-command-reduces on -@all +set +get", "+set +get")]
        [TestCase("user 20-category-command-reduces on -@all +set +get +incr +decr +incrby +decrby", "+set +get +incr +decr +incrby +decrby")]
        [TestCase("user 21-category-command-reduces on -@all +ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget", "+ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget")]
        [TestCase("user 22-category-command-reduces on -@all +ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +config|get", "+ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +config|get")]
        [TestCase("user 23-category-command-reduces on -@all +set +get +incr +decr +@keyspace +@hash +incrby +decrby", "+set +get +incr +decr +@keyspace +@hash +incrby +decrby")]
        [TestCase("user 24-multi-category-reduces on -@all +@keyspace +@hash", "+@keyspace +@hash")]
        [TestCase("user 25-multi-category-reduces on -@all +@keyspace +@hash -flushdb", "+@keyspace +@hash -flushdb")]
        [TestCase("user 26-multi-category-reduces on -@all +@keyspace -flushdb +@hash -flushdb", "+@keyspace -flushdb +@hash")]
        [TestCase("user 27-multi-category-reduces on -@all +set +get +incr +decr +@keyspace +@hash +incrby +decrby +script|exists +@pubsub +expire +ttl", "+set +get +incr +decr +@keyspace +@hash +incrby +decrby +script|exists +@pubsub")]
        [TestCase("user 28-command-reversed-duplicates on -set +set", "+set")]
        public void ParseACLRuleDescriptionTest(string acl, string expectedDescription)
        {
            User user = ACLParser.ParseACLRule(acl);
            ClassicAssert.IsNotNull(user);
            ClassicAssert.AreEqual(expectedDescription, user.GetEnabledCommandsDescription());
        }

        /// <summary>
        /// Test cases for scenarios where ACLs reduce and timeouts have been encountered in the past.
        /// </summary>
        [Test, MaxTime(1000)]
        [TestCase("user 1-command-notimeout on +auth +ping +get +set +del +exists +incr +decr +mget +mset +expire +ttl +keys +scan +hget +hset +lpush +rpush +sadd +decrby", "+auth +ping +get +set +del +exists +incr +decr +mget +mset +expire +ttl +keys +scan +hget +hset +lpush +rpush +sadd +decrby")]
        [TestCase("user 2-category-command-notimeout on -@all +ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +mget +mset +eval +evalsha +setex", "+ping +auth +set +get +del +incr +decr +incrby +decrby +expire +ttl +keys +scan +hget +mget +mset +eval +evalsha +setex")]
        [TestCase("user 3-category-command-notimeout on -@all +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink", "+client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink")]
        [TestCase("user 4-category-command-notimeout on +@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink", "+@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex")]
        [TestCase("user 5-category-command-notimeout on -@all +@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +del +expire +flushdb +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex +unlink", "+@keyspace +client|id +client|info +cluster|nodes +cluster|slots +echo +info +ping +config|get +decr -decr +decrby +get +incr +incrby +latency +eval +evalsha +script|exists +script|flush +script|load +set +setex")]
        public void ParseACLRuleDescriptionTimeoutsTest(string acl, string expectedDescription)
        {
            User user = ACLParser.ParseACLRule(acl);
            ClassicAssert.IsNotNull(user);
            ClassicAssert.AreEqual(expectedDescription, user.GetEnabledCommandsDescription());
        }

        /// <summary>
        /// Test cases for scenarios where ACLs reductions should occur but do not. Fixes required.
        /// </summary>
        [Explicit("Fails due to issues with reducing the commands in the ACL.")]
        [Test, MaxTime(1000)]
        [TestCase("user 1-category-command-reduces on +@keyspace +del -del +del", "+@keyspace")]
        [TestCase("user 2-command-duplicates-complicated on +set -get +get +set -set +set", "+set +get")]
        public void ParseACLRuleDescriptionShouldReduceTest(string acl, string expectedDescription)
        {
            User user = ACLParser.ParseACLRule(acl);
            ClassicAssert.AreEqual(expectedDescription, user.GetEnabledCommandsDescription());
        }
    }
}