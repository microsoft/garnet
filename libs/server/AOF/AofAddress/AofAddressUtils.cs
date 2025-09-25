using System;
using System.IO;

namespace Garnet.server
{
    public static class AofAddressUtils
    {
        // Helper method to deserialize based on AddressType
        public static unsafe IAofAddress DeserializeByType(BinaryReader reader)
        {
            var addressType = (AddressType)reader.ReadByte();
            return addressType switch
            {
                AddressType.SingleLog => SingleLogAofAddress.Deserialize(reader),
                //AddressType.MultiLog => MultiLogAofAddress.Deserialize(reader),
                _ => throw new ArgumentException($"Unknown AddressType: {addressType}")
            };
        }

        public static IAofAddress Fill(int size, long value)
        {
            return size switch
            {
                1 => new SingleLogAofAddress(value),
                _ => default //new MultiLogAofAddress(size, value),
            };
        }
    }
}