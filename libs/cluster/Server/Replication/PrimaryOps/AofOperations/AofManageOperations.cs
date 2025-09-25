using Garnet.server;

namespace Garnet.cluster
{

    public sealed partial class ClusterProvider
    {
        void SafeTruncateAofAddress(IAofAddress TruncatedUntil)
        {
            var _truncatedUntil = TruncatedUntil.Get();
            if (_truncatedUntil is > 0 and < long.MaxValue)
            {
                if (serverOptions.FastAofTruncate)
                {
                    storeWrapper.appendOnlyFile?.UnsafeShiftBeginAddress(_truncatedUntil, snapToPageStart: true, truncateLog: true);
                }
                else
                {
                    storeWrapper.appendOnlyFile?.TruncateUntil(_truncatedUntil);
                    storeWrapper.appendOnlyFile?.Commit();
                }
            }
        }
    }
};