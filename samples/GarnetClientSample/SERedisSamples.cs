// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace GarnetClientSample
{
    /// <summary>
    /// Use Garnet with StackExchange.Redis as client library
    /// </summary>
    public class SERedisSamples
    {
        readonly string address;
        readonly int port;

        public SERedisSamples(string address, int port)
        {
            this.address = address;
            this.port = port;
        }

        public async Task RunAll()
        {
            await RespPingAsync();
            RespPing();
            SingleSetRename();
            SingleSetGet();
            SingleIncr();
            SingleIncrBy(99);
            SingleDecrBy(99);
            SingleDecr("test", 5);
            SingleIncrNoKey();
            SingleExists();
            SingleDelete();
        }

        async Task RespPingAsync()
        {
            using var redis = await ConnectionMultiplexer.ConnectAsync($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);
            await db.PingAsync();
            Console.WriteLine("RespPing: Success");
        }

        void RespPing()
        {
            using var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);
            db.Ping();
            var cname = redis.ClientName;
            Console.WriteLine("RespPing: Success");
        }

        void SingleSetRename()
        {
            using var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            string origValue = "test1";
            db.StringSet("key1", origValue);

            db.KeyRename("key1", "key2");
            string retValue = db.StringGet("key2");

            if (origValue != retValue)
                Console.WriteLine("SingleSetRename: Error");
            else
                Console.WriteLine("SingleSetRename: Success");
        }

        void SingleSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            string origValue = "abcdefg";
            db.StringSet("mykey", origValue);

            string retValue = db.StringGet("mykey");

            if (origValue != retValue)
                Console.WriteLine("SingleSetGet: Error");
            else
                Console.WriteLine("SingleSetGet: Success");
        }

        void SingleIncr()
        {
            var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = -100000;
            var strKey = "key1";
            db.StringSet(strKey, nVal);

            // string retValue = db.StringGet("key1");

            db.StringIncrement(strKey);
            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            if (nVal + 1 != nRetVal)
                Console.WriteLine("SingleIncr: Error");
            else
                Console.WriteLine("SingleIncr: Success");
        }

        void SingleIncrBy(long nIncr)
        {
            var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 1000;

            var strKey = "key1";
            db.StringSet(strKey, nVal);
            var s = db.StringGet(strKey);

            var get = db.StringGet(strKey);
            long n = db.StringIncrement(strKey, nIncr);

            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            if (n != nRetVal)
                Console.WriteLine("SingleIncrBy: Error");
            else
                Console.WriteLine("SingleIncrBy: Success");
        }

        void SingleDecrBy(long nDecr)
        {
            var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 900;

            var strKey = "key1";
            db.StringSet(strKey, nVal);
            var s = db.StringGet(strKey);

            long n = db.StringDecrement(strKey, nDecr);
            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            if (nVal - nDecr != nRetVal)
                Console.WriteLine("SingleDecrBy: Error");
            else
                Console.WriteLine("SingleDecrBy: Success");
        }

        void SingleDecr(string strKey, int nVal)
        {
            var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            // Key storing integer
            db.StringSet(strKey, nVal);
            db.StringDecrement(strKey);
            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            if (nVal - 1 != nRetVal)
                Console.WriteLine("SingleDecr: Error");
            else
                Console.WriteLine("SingleDecr: Success");
        }

        void SingleIncrNoKey()
        {
            var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            // Key storing integer
            var strKey = "key1";
            int init = Convert.ToInt32(db.StringGet(strKey));
            db.StringIncrement(strKey);

            int retVal = Convert.ToInt32(db.StringGet(strKey));

            db.StringIncrement(strKey);
            retVal = Convert.ToInt32(db.StringGet(strKey));

            if (init + 2 != retVal)
                Console.WriteLine("SingleIncrNoKey: Error");
            else
                Console.WriteLine("SingleIncrNoKey: Success");
        }

        void SingleExists()
        {
            var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            db.StringSet(strKey, nVal);

            bool fExists = db.KeyExists("key1", CommandFlags.None);
            if (fExists)
                Console.WriteLine("SingleExists: Success");
            else
                Console.WriteLine("SingleExists: Error");
        }

        void SingleDelete()
        {
            var redis = ConnectionMultiplexer.Connect($"{address}:{port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            db.StringSet(strKey, nVal);
            db.KeyDelete(strKey);

            bool fExists = db.KeyExists("key1", CommandFlags.None);
            if (!fExists)
                Console.WriteLine("Pass: strKey, Key does not exists");
            else
                Console.WriteLine("Fail: strKey, Key was not deleted");
        }
    }
}