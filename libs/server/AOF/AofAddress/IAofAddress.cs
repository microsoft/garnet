using System.IO;

namespace Garnet.server
{
    public interface IAofAddress
    {
        public static readonly long kFirstValidAofAddress = 64;

        long Get(uint idx = 0);

        void Set(long aofAddress, uint idx = 0);

        unsafe void Serialize(BinaryWriter writer);

        long AggregateLag(IAofAddress beginAddress);

        bool OutOfRangeAof(IAofAddress aofBeginAddress, IAofAddress aofTailAddress);

        IAofAddress GetAddressOrDefault();

        bool IsGreater(IAofAddress aofAddress);
        bool IsLesser(IAofAddress aofAddress);

        IAofAddress Min(IAofAddress aofAddress);
        void ExchangeMin(IAofAddress aofAddress);
        void MonotonicUpdate(IAofAddress aofAddress);
    }
}