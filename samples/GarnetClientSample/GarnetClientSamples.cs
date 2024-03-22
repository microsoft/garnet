// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;

namespace GarnetClientSample
{
    /// <summary>
    /// Use Garnet with GarnetClient as client library
    /// </summary>
    public class GarnetClientSamples
    {
        readonly string address;
        readonly int port;
        readonly bool useTLS;

        public GarnetClientSamples(string address, int port, bool useTLS)
        {
            this.address = address;
            this.port = port;
            this.useTLS = useTLS;
        }

        public async Task RunAll()
        {
            await PingAsync();
            await SetGetAsync();
            SetGetSync();
            await IncrAsync();
            await IncrByAsync(99);
            await DecrByAsync(99);
            await DecrAsync("test", 5);
            await IncrNoKeyAsync();
            await ExistsAsync();
            await DeleteAsync();
            await SetGetMemoryAsync();
        }

        async Task PingAsync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();
            var pong = await db.PingAsync();
            if (pong != "PONG")
                throw new Exception("PingAsync: Error");
            Console.WriteLine("Ping: Success");
        }

        async Task SetGetAsync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            string origValue = "abcdefg";
            await db.StringSetAsync("mykey", origValue);

            string retValue = await db.StringGetAsync("mykey");

            if (origValue != retValue)
                throw new Exception("SetGetAsync: Error");
            Console.WriteLine("SetGetAsync: Success");
        }

        void SetGetSync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            db.Connect();

            string origValue = "abcdefg";
            db.StringSet("mykey", origValue, (c, s) => { if (s != "OK") throw new Exception("SetGetSync: Error"); });

            ManualResetEventSlim e = new();
            db.StringGet("mykey", (c, s) => { if (s != origValue) throw new Exception("SetGetSync: Error"); e.Set(); });
            e.Wait();
            Console.WriteLine("SetGetSync: Success");
        }

        async Task IncrAsync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            // Key storing integer
            int nVal = 1000;
            var strKey = "key1";
            await db.StringSetAsync(strKey, $"{nVal}");
            var s = await db.StringGetAsync(strKey);

            if (s != $"{nVal}")
                throw new Exception("IncrementAsync: Error");

            long n = await db.StringIncrement(strKey);
            if (n != nVal + 1)
                throw new Exception("IncrementAsync: Error");

            int nRetVal = int.Parse(await db.StringGetAsync(strKey));
            if (n != nRetVal)
                throw new Exception("IncrementAsync: Error");
            Console.WriteLine("IncrementAsync: Success");
        }

        async Task IncrByAsync(long nIncr)
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            // Key storing integer
            int nVal = 1000;

            var strKey = "key1";
            await db.StringSetAsync(strKey, $"{nVal}");
            var s = await db.StringGetAsync(strKey);

            if (s != $"{nVal}")
                throw new Exception("IncrementByAsync: Error");

            long n = int.Parse(await db.ExecuteForStringResultAsync("INCRBY", new string[] { strKey, nIncr.ToString() }));
            if (n != nVal + nIncr)
                throw new Exception("IncrementByAsync: Error");

            int nRetVal = int.Parse(await db.StringGetAsync(strKey));
            if (n != nRetVal)
                throw new Exception("IncrementByAsync: Error");
            Console.WriteLine("IncrementByAsync: Success");
        }

        async Task DecrByAsync(long nDecr)
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            // Key storing integer
            int nVal = 900;

            var strKey = "key1";
            await db.StringSetAsync(strKey, $"{nVal}");
            var s = await db.StringGetAsync(strKey);

            if (s != $"{nVal}")
                throw new Exception("DecrByAsync: Error");

            long n = int.Parse(await db.ExecuteForStringResultAsync("DECRBY", new string[] { strKey, nDecr.ToString() }));
            if (n != nVal - nDecr)
                throw new Exception("DecrByAsync: Error");

            int nRetVal = int.Parse(await db.StringGetAsync(strKey));
            if (n != nRetVal)
                throw new Exception("DecrByAsync: Error");
            Console.WriteLine("DecrByAsync: Success");
        }

        async Task DecrAsync(string strKey, int nVal)
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            await db.StringSetAsync(strKey, $"{nVal}");
            var s = await db.StringGetAsync(strKey);

            if (s != $"{nVal}")
                throw new Exception("DecrAsync: Error");

            long n = await db.StringDecrement(strKey);
            if (n != nVal - 1)
                throw new Exception("DecrAsync: Error");

            int nRetVal = int.Parse(await db.StringGetAsync(strKey));
            if (n != nRetVal)
                throw new Exception("DecrAsync: Error");
            Console.WriteLine("DecrAsync: Success");
        }

        async Task IncrNoKeyAsync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            // Key storing integer
            var strKey = "key1";
            int init = int.Parse(await db.StringGetAsync(strKey));
            await db.StringIncrement(strKey);

            var retVal = int.Parse(await db.StringGetAsync(strKey));

            await db.StringIncrement(strKey);
            retVal = int.Parse(await db.StringGetAsync(strKey));

            if (init + 2 != retVal)
                throw new Exception("IncrNoKeyAsync: Error");
            Console.WriteLine("IncrNoKeyAsync: Success");
        }

        async Task ExistsAsync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            // Key storing integer
            int nVal = 100;
            var strKey = "key1";
            await db.StringSetAsync(strKey, $"{nVal}");

            bool fExists = int.Parse(await db.ExecuteForStringResultAsync("EXISTS", new string[] { strKey })) == 1 ? true : false;
            if (!fExists)
                throw new Exception("ExistsAsync: Error");
            Console.WriteLine("ExistsAsync: Success");
        }

        async Task DeleteAsync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            await db.StringSetAsync(strKey, $"{nVal}");
            await db.KeyDeleteAsync(strKey);

            bool fExists = int.Parse(await db.ExecuteForStringResultAsync("EXISTS", new string[] { strKey })) == 1 ? true : false;
            if (fExists)
                throw new Exception("DeleteAsync: Error");
            Console.WriteLine("DeleteAsync: Success");
        }


        async Task SetGetMemoryAsync()
        {
            using var db = new GarnetClient(address, port, GetSslOpts());
            await db.ConnectAsync();

            var key = new Memory<byte>(new byte[17]);
            Encoding.UTF8.GetBytes("SetGetMemoryAsync".AsSpan(), key.Span);

            var origValueStr = "abcdefg";
            var origValue = new Memory<byte>(new byte[7]);
            Encoding.UTF8.GetBytes(origValueStr.AsSpan(), origValue.Span);

            await db.StringSetAsync(key, origValue);

            using var retValue = await db.StringGetAsMemoryAsync(key);

            if (!origValue.Span.SequenceEqual(retValue.Span))
                throw new Exception("SetGetAsync: Error");

            Console.WriteLine("SetGetMemoryAsync: Success");
        }

        SslClientAuthenticationOptions GetSslOpts() => useTLS ? new()
        {
            ClientCertificates = [new X509Certificate2("testcert.pfx", "placeholder")],
            TargetHost = "GarnetTest",
            RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true,
        } : null;

        public void ConnectionStressTest()
        {
            for (int t = 0; t < 8; t++)
            {
                new Thread(() => ConnectionStressRunner(t)).Start();
            }
            Thread.Sleep(Timeout.Infinite);

            void ConnectionStressRunner(int thread_id)
            {
                int i = 0;
                while (true)
                {
                    using var client = new GarnetClient(address, port, GetSslOpts());
                    client.Connect();
                    Console.WriteLine($"{thread_id}:{i++}: {client.PingAsync().GetAwaiter().GetResult()}");
                    client.Dispose();
                }
            }
        }
    }
}