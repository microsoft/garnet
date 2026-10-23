// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text.Json;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    public partial class RecordImportEtagTests
    {
        [TestCase(1, 0L, false, 32, false)]
        [TestCase(1, 3L, true, 2097152, false)]
        [TestCase(1, long.MaxValue, true, 32, true)]
        [TestCase(1, 1L, false, 2097152, true)]
        [TestCase(2, 0L, true, 2097152, true)]
        [TestCase(2, 3L, false, 32, true)]
        [TestCase(2, long.MaxValue, false, 2097152, false)]
        [TestCase(2, 1L, true, 32, false)]
        [Explicit("Exports logs for unchanged-reader compatibility validation.")]
        public void ExportActualV5ImportLogs(int logs, long tag, bool expiry, int length, bool rename)
        {
            string exportRoot = Environment.GetEnvironmentVariable("GARNET_V5_EXPORT_ROOT");
            Assert.That(exportRoot, Is.Not.Null.And.Not.Empty);
            server.Dispose();
            server = CreateAofServer(logs, false);
            server.Start();
            string value = new('v', length);
            long expiration;
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                IDatabase db = redis.GetDatabase();
                if (tag == 0)
                    db.StringSet("source", value);
                else
                    db.Execute("SETIFGREATER", "source", value, tag);
                if (expiry)
                    db.KeyExpire("source", TimeSpan.FromHours(4));
                expiration = (long)db.Execute("PEXPIRETIME", "source");
                db.Execute("SETIFGREATER", "destination", new string('o', length * 2), long.MaxValue);
                db.KeyExpire("destination", TimeSpan.FromHours(5));
                if (rename)
                    Assert.That(db.KeyRename("source", "destination"), Is.True);
                else
                    ImportRecord("source", "destination");
                AssertTaggedValue(db, "destination", tag, value);
                Assert.That((long)db.Execute("PEXPIRETIME", "destination"), Is.EqualTo(expiration));
                db.Execute("COMMITAOF");
            }
            server.Dispose(false);
            server = null;
            string target = Path.Combine(exportRoot, $"{logs}-{tag}-{expiry}-{length}-{rename}");
            Directory.CreateDirectory(target);
            foreach (string source in Directory.GetFiles(TestUtils.MethodTestDir, "*", SearchOption.AllDirectories))
            {
                string destination = Path.Combine(target, "data", Path.GetRelativePath(TestUtils.MethodTestDir, source));
                Directory.CreateDirectory(Path.GetDirectoryName(destination));
                File.Copy(source, destination, true);
            }
            File.WriteAllText(Path.Combine(target, "expected.json"),
                JsonSerializer.Serialize(new { Logs = logs, Tag = tag, Length = length, Expiration = expiration }));
        }
    }
}