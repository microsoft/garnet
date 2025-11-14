namespace ReadFileServer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // You can change the device type here to test LocalStorage (native windows) and RandomAccess storage devices.
            using var store = Devices.CreateLogDevice("test.dat", deviceType: DeviceType.LocalStorage);

            // For the repro, use a SSD device with a sector size of 512 bytes
            // create a byte array buffer of 512 random bytes, write to disk
            var buffer = new byte[512];
            new Random().NextBytes(buffer);

            // Write 2KB of data (4 blocks of 512 bytes each)
            DeviceUtils.WriteInto(store, 0, buffer);
            DeviceUtils.WriteInto(store, 512, buffer);
            DeviceUtils.WriteInto(store, 1024, buffer);
            DeviceUtils.WriteInto(store, 1536, buffer);

            // Start the server
            new SocketServer(9000, buffer, store).Start();
            Console.WriteLine("Server started.");

            Console.ReadLine();
        }
    }
}
