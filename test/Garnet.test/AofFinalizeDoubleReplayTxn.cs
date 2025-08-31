using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.test
{
    // Test transaction used to make sure we are not doing double replay of items enqueued to AOF at finalize step
    // AOFFINDOUBLEREP KEY
    public class AofFinalizeDoubleReplayTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            int offset = 0;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, false);
            return true;
        }
        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            // No-op
        }

        public override void Finalize<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            int offset = 0;
            api.Increment(GetNextArg(ref procInput, ref offset), out _);
        }
    }
}