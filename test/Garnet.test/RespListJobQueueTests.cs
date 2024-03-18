// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespListJobQueueTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void CanCreateJobQueue()
        {
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;

            var nJobs = 10;
            var jb = new JobQueue("jobQueueSample", token);
            jb.OnJobReceived += Jb_OnJobReceived;

            Task[] jobsProcess = new Task[nJobs];
            for (int i = 0; i < nJobs; i++)
            {
                // Add a new job
                var j = jb.AddJobAsync(new RedisValue($"Job - {i + 1}"));
                jobsProcess[i] = j;
            }

            Task.WaitAll(jobsProcess);
            var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var jobsInProcessingQ = redis.GetDatabase(0).ListRange(jb.JobQueueName);
            Assert.IsTrue(jobsInProcessingQ.Length == nJobs);

            //checks jobs were moved to the jobqueue list
            jb.AsConsumer();
            var jobsInQueue = redis.GetDatabase(0).ListRange(jb.ProcessingQueueName);
            Assert.IsTrue(jobsInQueue.Length == nJobs);

            //checks jobs were removed from the processingqueue list
            jb.AsManager();
            jobsInProcessingQ = redis.GetDatabase(0).ListRange(jb.ProcessingQueueName);
            Assert.IsTrue(jobsInProcessingQ.Length == 0);
        }

        private void Jb_OnJobReceived(object sender, JobReceivedEventArgs e)
        {
            Debug.Print("OnJobReceived");
        }
    }
}