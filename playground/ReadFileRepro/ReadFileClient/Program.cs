using System.Net;
using System.Net.Sockets;
using System.Text;

namespace ReadFileClient
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Create N client connections, each sending "PING" and receiving the same back
            int clientCount = 4;
            var clientThreads = new Thread[clientCount];
            for (int i = 0; i < clientCount; i++)
            {
                Console.WriteLine($"starting client {i}");
                clientThreads[i] = new Thread(() => RunClient("127.0.0.1", 9000));
                clientThreads[i].Start();
            }

            foreach (var t in clientThreads)
            {
                t.Join();
            }
        }

        static void RunClient(string host, int port)
        {
            var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var connectArgs = new SocketAsyncEventArgs
            {
                RemoteEndPoint = new IPEndPoint(IPAddress.Parse(host), port)
            };
            var connectDone = new ManualResetEvent(false);
            connectArgs.Completed += (s, e) => connectDone.Set();
            if (!client.ConnectAsync(connectArgs))
                connectDone.Set();
            connectDone.WaitOne();

            Thread.Sleep(1000);

            while (true)
            {
                // Send "PING"
                var sendArgs = new SocketAsyncEventArgs();
                byte[] sendBuffer = Encoding.UTF8.GetBytes("PING");
                sendArgs.SetBuffer(sendBuffer, 0, sendBuffer.Length);
                var sendDone = new ManualResetEvent(false);
                sendArgs.Completed += (s, e) => sendDone.Set();
                sendArgs.AcceptSocket = client;
                if (!client.SendAsync(sendArgs))
                    sendDone.Set();
                sendDone.WaitOne();

                // Receive echo
                var recvArgs = new SocketAsyncEventArgs();
                byte[] recvBuffer = new byte[1024];
                recvArgs.SetBuffer(recvBuffer, 0, recvBuffer.Length);
                var recvDone = new ManualResetEvent(false);
                recvArgs.Completed += (s, e) => recvDone.Set();
                recvArgs.AcceptSocket = client;
                if (!client.ReceiveAsync(recvArgs))
                    recvDone.Set();
                recvDone.WaitOne();

                //string response = Encoding.UTF8.GetString(recvBuffer, 0, recvArgs.BytesTransferred);
                //Console.WriteLine($"Client received: {response}");
            }
            client.Close();
        }
    }
}
