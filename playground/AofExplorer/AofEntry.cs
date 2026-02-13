
// Use AOF types directly from Garnet.server via type aliases
using AofHeader = Garnet.server.AofHeader;
using AofShardedHeader = Garnet.server.AofShardedHeader;

namespace AofExplorer
{
    /// <summary>
    /// Parsed AOF entry
    /// </summary>
    internal class AofEntry
    {
        public bool IsExtended { get; set; }
        public AofHeader Header { get; set; }
        public AofShardedHeader? ExtendedHeader { get; set; }
        public byte[] Key { get; set; } = [];
        public byte[] Value { get; set; } = [];
        public byte[] Input { get; set; } = [];
        public long FileOffset { get; set; }
        public int TotalLength { get; set; }
    }
}