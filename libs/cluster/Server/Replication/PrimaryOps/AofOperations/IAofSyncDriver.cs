using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal interface IAofSyncDriver
    {
        Task Run();
        void ConnectClient();
        Task<string> IssuesFlushAll();
        void InitializeIterationBuffer();
        void InitializeIfNeeded(bool isMainStore);
        Task<string> ExecuteAttachSync(SyncMetadata syncMetadata);
        bool TryWriteKeyValueSpanByte(ref SpanByte key, ref SpanByte value, out Task<string> task);
        bool TryWriteKeyValueByteArray(byte[] key, byte[] value, long expiration, out Task<string> task);
        Task<string> SendAndResetIterationBuffer();
    }
}