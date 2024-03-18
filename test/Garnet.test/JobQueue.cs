// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    ///  A simple JobQueue example based on the gist code at
    ///  https://gist.github.com/tenowg/c5de38cb1027ab875e56
    /// </summary>
    internal class JobQueue
    {
        private ConnectionMultiplexer connMultiplexer;

        private readonly string _jobQueue;
        private readonly string _processingQueue;
        private readonly string _subChannel;
        private readonly string _jobName;

        private readonly CancellationToken _cancellationToken;
        private bool _receiving;

        public event EventHandler<JobReceivedEventArgs> OnJobReceived;

        public string JobQueueName => _jobQueue;
        public string ProcessingQueueName => _processingQueue;

        public JobQueue(string jobName, CancellationToken cancellationToken = new CancellationToken())
        {
            connMultiplexer = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            _jobQueue = $"{jobName}:jobs";
            _processingQueue = $"{jobName}:process";
            _subChannel = $"{jobName}:channel";
            _jobName = jobName;
            _cancellationToken = cancellationToken;

            //create a channel for sending the event messages
            connMultiplexer.GetSubscriber().Subscribe(_subChannel, (channel, value) => { Debug.Print(value); });
        }

        /// <summary>
        /// When a job is finished, remove it from the processingQueue and from the
        /// cache database.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="success">if false requeue for another attempt</param>
        public async Task Finish(string key, bool success = true)
        {
            var db = connMultiplexer.GetDatabase(0);
            await db.ListRemoveAsync(_processingQueue, key);

            if (success)
            {
                await db.KeyDeleteAsync(key);
                return;
            }

            db.HashDelete(key, "active");
            db.ListRightPush(_jobQueue, key);
            connMultiplexer.GetSubscriber().Publish(_subChannel, $"Finishing {key}");
        }

        /// <summary>
        /// Consume messages from the queue
        /// </summary>
        /// <returns></returns>
        public void AsConsumer()
        {
            HandleNewJobs().Wait();
        }

        /// <summary>
        /// Removes all the jobs from the
        /// processing queue
        /// </summary>
        /// <returns></returns>
        public void AsManager()
        {
            var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            RedisValue[] values = db.ListRange(_processingQueue);
            var j = from value in values let activeTime = db.HashGet((string)value, "active") where (double)activeTime < 1000 select value;
            foreach (var value in j)
            {
                Finish(value, false).Wait();
            }
        }

        /// <summary>
        /// Move key from JobQueue to processingQueue, get key value from cache.
        /// </summary>
        /// <returns></returns>
        private async Task<Dictionary<RedisValue, RedisValue>> GetJobAsync()
        {
            var db = connMultiplexer.GetDatabase(0);
            var value = new Dictionary<RedisValue, RedisValue>();
            while (!_cancellationToken.IsCancellationRequested)
            {
                string key = await db.ListRightPopLeftPushAsync(_jobQueue, _processingQueue);
                // If key is null, then nothing was there to get, so no value is available
                if (string.IsNullOrEmpty(key))
                {
                    value.Clear();
                    break;
                }
                // check how this is saved why we don't have this vaue in the hashset
                await db.HashSetAsync(key, "active", 100);
                value = (await db.HashGetAllAsync(key)).ToDictionary();

                // if Count is 0, remove it and check for the next job
                if (value.Count == 0)
                {
                    await db.ListRemoveAsync(_processingQueue, key);
                    continue;
                }
                value.Add("key", key);
                break;
            }
            return value;
        }

        /// <summary>
        /// We have received an indicator that new jobs are available
        /// We process until we are out of jobs.
        /// </summary>
        public async Task HandleNewJobs()
        {
            if (_receiving) return;

            _receiving = true;

            var job = await GetJobAsync();
            // If a valid job cannot be found, it will return an empty Dictionary
            while (job.Count != 0)
            {
                // Fire the Event
                OnJobReceived?.Invoke(this, new JobReceivedEventArgs(job, job["key"]));
                // Get a new job if there is one
                job = await GetJobAsync();
            }
            _receiving = false;
        }

        /// <summary>
        /// Add a job to the Queue (async)
        /// the single RedisValue is marked as 'payload' in the hash
        /// </summary>
        /// <param name="job">payload</param>
        public async Task AddJobAsync(RedisValue job)
        {
            if (job.IsNullOrEmpty) return;

            var db = connMultiplexer.GetDatabase(0);

            var id = await db.StringIncrementAsync($"{_jobName}:jobid");
            var key = $"{_jobName}:{id}";
            await db.HashSetAsync(key, "payload", job);
            await db.ListLeftPushAsync(_jobQueue, key);

            //send the event message
            connMultiplexer.GetSubscriber().Publish(_subChannel, $"Job {key} was added");
        }
    }

    internal class JobReceivedEventArgs
    {
        private Dictionary<RedisValue, RedisValue> job;
        private RedisValue redisValue;

        public JobReceivedEventArgs(Dictionary<RedisValue, RedisValue> job, RedisValue redisValue)
        {
            this.job = job;
            this.redisValue = redisValue;
        }
    }
}