// using System;
// using System.IO;

// namespace Garnet.server
// {
//     public struct MultiLogAofAddress : IAofAddress
//     {
//         readonly long[] aofAddressSpan;

//         public MultiLogAofAddress(int size = 1, long value = 0)
//         {
//             aofAddressSpan = GC.AllocateArray<long>(size, pinned: true);
//             Array.Fill(aofAddressSpan, value);
//         }

//         public bool OutOfRangeAof(IAofAddress beginAddress, IAofAddress tailAddress)
//         {
//             if (beginAddress is MultiLogAofAddress multiBegin && tailAddress is MultiLogAofAddress multiTail)
//             {
//                 for (var i = 0; i < aofAddressSpan.Length && i < multiBegin.aofAddressSpan.Length && i < multiTail.aofAddressSpan.Length; i++)
//                 {
//                     if (aofAddressSpan[i] < multiBegin.aofAddressSpan[i] || aofAddressSpan[i] > multiTail.aofAddressSpan[i])
//                         return true;
//                 }
//                 return false;
//             }
//             throw new ArgumentException("Incompatible address type");
//         }

//         public long Get(uint idx = 0) => aofAddressSpan[idx];

//         public void Set(uint aofAddress, uint idx = 0) => this.aofAddressSpan[idx] = aofAddress;

//         public unsafe void Serialize(BinaryWriter writer)
//         {
//             writer.Write((byte)AddressType.MultiLog);
//             writer.Write(aofAddressSpan.Length);
//             fixed (long* ptr = aofAddressSpan)
//             {
//                 var span = new ReadOnlySpan<byte>(ptr, aofAddressSpan.Length * sizeof(long));
//                 writer.Write(span);
//             }
//         }

//         public static unsafe IAofAddress Deserialize(BinaryReader reader)
//         {
//             var length = reader.ReadInt32();
//             var span = new MultiLogAofAddress(length);
//             fixed (long* ptr = span.aofAddressSpan)
//             {
//                 var bytes = new Span<byte>(ptr, length * sizeof(long));
//                 reader.Read(bytes);
//             }
//             return span;
//         }

//         public long AggregateLag(IAofAddress beginAddress)
//         {
//             if (beginAddress is MultiLogAofAddress multi)
//             {
//                 long aggregateLag = 0;
//                 for (int i = 0; i < Math.Min(aofAddressSpan.Length, multi.aofAddressSpan.Length); i++)
//                     aggregateLag += aofAddressSpan[i] - multi.aofAddressSpan[i];
//                 return aggregateLag;
//             }
//             throw new ArgumentException("Incompatible address type");
//         }

//         public IAofAddress GetAddressOrDefault()
//         {
//             var aofAddress = new MultiLogAofAddress(aofAddressSpan.Length, IAofAddress.kFirstValidAofAddress);
//             for (var i = 0; i < aofAddressSpan.Length; i++)
//             {
//                 if (aofAddressSpan[i] > IAofAddress.kFirstValidAofAddress)
//                     aofAddress.aofAddressSpan[i] = aofAddressSpan[i];
//             }
//             return aofAddress;
//         }
//     }
// }