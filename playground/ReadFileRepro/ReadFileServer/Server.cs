using System.Net;
using System.Net.Sockets;
using System.Text;

namespace ReadFileServer
{
    public class SocketServer
    {
        private readonly Socket _listenSocket;
        private readonly int _port;
        private const int BufferSize = 1024;
        private readonly byte[] _expectedBuffer;
        private readonly IDevice _store;
        private int count = 0;

        public SocketServer(int port, byte[] expectedBuffer, IDevice store)
        {
            _port = port;
            _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _expectedBuffer = expectedBuffer;
            _store = store;
        }

        public void Start()
        {
            _listenSocket.Bind(new IPEndPoint(IPAddress.Any, _port));
            _listenSocket.Listen(100);
            StartAccept(null);
            Console.WriteLine($"Server started on port {_port}");
        }

        private void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if (acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += AcceptCompleted;
            }
            else
            {
                acceptEventArg.AcceptSocket = null;
            }

            if (!_listenSocket.AcceptAsync(acceptEventArg))
            {
                ProcessAccept(acceptEventArg);
            }
        }

        private void AcceptCompleted(object sender, SocketAsyncEventArgs e)
        {
            ProcessAccept(e);
        }

        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            Socket clientSocket = e.AcceptSocket;
            e.AcceptSocket.NoDelay = true;
            var readEventArgs = new SocketAsyncEventArgs();
            readEventArgs.SetBuffer(new byte[BufferSize], 0, BufferSize);
            readEventArgs.UserToken = clientSocket;
            readEventArgs.Completed += IOCompleted;

            StartReceive(readEventArgs);
            StartAccept(e);
        }

        private void StartReceive(SocketAsyncEventArgs readEventArgs)
        {
            Socket clientSocket = (Socket)readEventArgs.UserToken;
            if (!clientSocket.ReceiveAsync(readEventArgs))
            {
                Task.Run(() => ProcessReceive(readEventArgs));
            }
        }

        private void IOCompleted(object sender, SocketAsyncEventArgs e)
        {
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive(e);
                    break;
                case SocketAsyncOperation.Send:
                    ProcessSend(e);
                    break;
            }
        }

        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            Socket clientSocket = (Socket)e.UserToken;
            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                string request = Encoding.UTF8.GetString(e.Buffer, e.Offset, e.BytesTransferred);

                // Execute ReadFile logic before responding
                try
                {
                    ReadFile(_expectedBuffer, _store);
                }
                catch (Exception ex)
                {
                    string error = $"ERROR: {ex.Message}";
                    byte[] errorBytes = Encoding.UTF8.GetBytes(error);
                    e.SetBuffer(errorBytes, 0, errorBytes.Length);
                    if (!clientSocket.SendAsync(e))
                        ProcessSend(e);
                    return;
                }

                // Echo back the request
                byte[] responseBytes = Encoding.UTF8.GetBytes(request);
                e.SetBuffer(responseBytes, 0, responseBytes.Length);
                if (!clientSocket.SendAsync(e))
                {
                    ProcessSend(e);
                }
            }
            else
            {
                clientSocket.Close();
                e.Dispose();
            }
        }

        private void ProcessSend(SocketAsyncEventArgs e)
        {
            // After sending, start receiving again
            StartReceive(e);
        }

        private void ReadFile(byte[] expectedBuffer, IDevice store)
        {
            if (Interlocked.Increment(ref count) % 1000 == 0)
                Console.WriteLine($"read #{count}");

            DeviceUtils.ReadInto(store, 0, 512, out var readBuffer);
            if (!expectedBuffer.SequenceEqual(readBuffer))
                throw new Exception("unexpected read bytes");
        }
    }
}