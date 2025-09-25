using System;
using System.IO;

namespace Garnet.server
{
    public struct SingleLogAofAddress(long aofAddress) : IAofAddress
    {
        long aofAddress = aofAddress;

        public unsafe void Serialize(BinaryWriter writer)
        {
            writer.Write((byte)AddressType.SingleLog);
            writer.Write(aofAddress);
        }

        public static unsafe IAofAddress Deserialize(BinaryReader reader)
            => new SingleLogAofAddress(reader.ReadInt64());

        public long AggregateLag(IAofAddress beginAddress)
        {
            if (beginAddress is SingleLogAofAddress single)
                return aofAddress - single.aofAddress;
            throw new ArgumentException("Incompatible address type");
        }

        public bool OutOfRangeAof(IAofAddress beginAddress, IAofAddress tailAddress)
        {
            if (beginAddress is SingleLogAofAddress singleBeginAddress && tailAddress is SingleLogAofAddress singleTailAddress)
                return aofAddress < singleBeginAddress.aofAddress || aofAddress > singleTailAddress.aofAddress;
            throw new ArgumentException("Incompatible address type");
        }

        public long Get(uint idx = 0) => aofAddress;

        public void Set(long aofAddress, uint idx = 0) => this.aofAddress = aofAddress;

        public IAofAddress GetAddressOrDefault()
        {
            if (this.aofAddress > IAofAddress.kFirstValidAofAddress)
                return this;
            return new SingleLogAofAddress(IAofAddress.kFirstValidAofAddress);
        }

        public bool IsGreater(IAofAddress aofAddress)
        {
            if (aofAddress is SingleLogAofAddress singleLogAofAddress)
                return this.aofAddress > singleLogAofAddress.aofAddress;
            throw new ArgumentException("Incompatible address type");
        }

        public bool IsLesser(IAofAddress aofAddress)
        {
            if (aofAddress is SingleLogAofAddress singleLogAofAddress)
                return this.aofAddress < singleLogAofAddress.aofAddress;
            throw new ArgumentException("Incompatible address type");
        }

        void FirstValidIfZero()
        {
            if (aofAddress == 0) aofAddress = IAofAddress.kFirstValidAofAddress;
        }

        public IAofAddress Min(IAofAddress aofAddress)
        {
            if (aofAddress is SingleLogAofAddress singleLogAofAddress)
                return new SingleLogAofAddress(Math.Min(this.aofAddress, singleLogAofAddress.aofAddress));
            throw new ArgumentException("Incompatible address type");
        }

        public void ExchangeMin(IAofAddress aofAddress)
        {
            this.aofAddress = Math.Min(((SingleLogAofAddress)aofAddress).aofAddress, this.aofAddress);
        }

        public void MonotonicUpdate(IAofAddress aofAddress)
        {
            Tsavorite.core.Utility.MonotonicUpdate(ref this.aofAddress, ((SingleLogAofAddress)aofAddress).aofAddress, out _);
        }
    }
}