// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

extern alias RespBenchmark;

using System.IO;
using NUnit.Framework;
using BenchmarkOptions = RespBenchmark::Resp.benchmark.Options;
using BenchmarkOpType = RespBenchmark::Resp.benchmark.OpType;

namespace Garnet.test
{
    [TestFixture]
    public class BenchmarkConfigTests : TestBase
    {
        string configPath;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            configPath = Path.Combine(TestUtils.MethodTestDir, "benchmark.conf");
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
        }

        [Test]
        public void FileOptionsAndDefaults()
        {
            File.WriteAllText(configPath, """{"NumThreads":[3,7],"Op":"SET","OpWorkload":["GET","SET"],"OpPercent":[80,20],"ClusterBench":true,"LSet":true,"UseFixedWorkerPool":false}""");
            Assert.That(BenchmarkOptions.TryParse(["--config", configPath], out var options, out _), Is.True);
            Assert.That(options.NumThreads, Is.EqualTo([3, 7]));
            Assert.That(options.Op, Is.EqualTo(BenchmarkOpType.SET));
            Assert.That(options.OpWorkload, Is.EqualTo([BenchmarkOpType.GET, BenchmarkOpType.SET]));
            Assert.That(options.OpPercent, Is.EqualTo([80, 20]));
            Assert.That(options.ClusterBench, Is.True);
            Assert.That(options.LSet, Is.True);
            Assert.That(options.UseFixedWorkerPool, Is.False);
            Assert.That(options.Port, Is.EqualTo(6379));
        }

        [TestCase("--threads", "2,4")]
        [TestCase("-t", "2,4")]
        [TestCase("--threads=2,4", null)]
        public void CommandLineOverridesFile(string option, string value)
        {
            File.WriteAllText(configPath, """{"NumThreads":[8],"Address":"127.0.0.2","Port":1234,"DbSize":1024}""");
            string[] args = value == null
                ? ["--config", configPath, option, "-p7000"]
                : ["--config", configPath, option, value, "--port", "7000"];
            Assert.That(BenchmarkOptions.TryParse(args, out var options, out _), Is.True);
            Assert.That(options.NumThreads, Is.EqualTo([2, 4]));
            Assert.That(options.Port, Is.EqualTo(7000));
            Assert.That(options.Address, Is.EqualTo("127.0.0.2"));
        }

        [Test]
        public void BundledShortOptionsOverrideFile()
        {
            File.WriteAllText(configPath, """{"Online":false,"SkipLoad":false}""");
            Assert.That(BenchmarkOptions.TryParse(["--config", configPath, "-os"], out var options, out _), Is.True);
            Assert.That(options.Online, Is.True);
            Assert.That(options.SkipLoad, Is.True);
        }

        [Test]
        public void ExplicitBooleanOverridesFile()
        {
            File.WriteAllText(configPath, """{"ClusterBench":true}""");
            Assert.That(BenchmarkOptions.TryParse(["--config", configPath, "--cluster-bench=false"], out var options, out _), Is.True);
            Assert.That(options.ClusterBench, Is.False);
        }

        [Test]
        public void AliasOverridesCanonicalFileOption()
        {
            File.WriteAllText(configPath, """{"BatchSize":[128]}""");
            Assert.That(BenchmarkOptions.TryParse(["--config", configPath, "--pipeline", "64"], out var options, out _), Is.True);
            Assert.That(options.BatchSize, Is.EqualTo([64]));
        }

        [TestCase("""{"DbSzie":1024}""")]
        [TestCase("""{"DbSize":"not a number"}""")]
        [TestCase("""{"ClusterBench":"true"}""")]
        [TestCase("""{"Op":"Unknown"}""")]
        [TestCase("""{"DbSize":1,"dbsize":2}""")]
        [TestCase("""{"DbSize":null}""")]
        [TestCase("""{"ConfigFile":"nested.conf"}""")]
        [TestCase("""{"ValidateConfig":true}""")]
        [TestCase("""{"Pipeline":[16]}""")]
        [TestCase("[]")]
        [TestCase("{")]
        public void InvalidConfigurationFails(string content)
        {
            File.WriteAllText(configPath, content);
            Assert.That(BenchmarkOptions.TryParse(["--config", configPath], out _, out var graceful), Is.False);
            Assert.That(graceful, Is.False);
        }

        [Test]
        public void MissingFileAndInvalidCliFail()
        {
            Assert.That(BenchmarkOptions.TryParse(["--config", configPath], out _, out _), Is.False);
            Assert.That(BenchmarkOptions.TryParse(["--unknown-option"], out _, out var graceful), Is.False);
            Assert.That(graceful, Is.False);
            Assert.That(BenchmarkOptions.TryParse(["--cluster-bench=invalid"], out _, out _), Is.False);
            Assert.That(BenchmarkOptions.TryParse(["--dbsize", "2048"], out var options, out _), Is.True);
            Assert.That(options.DbSize, Is.EqualTo(2048));
        }
    }
}