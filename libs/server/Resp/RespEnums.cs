namespace Garnet.server
{
    internal enum ExpirationOption : byte
    {
        None,
        EX,
        PX,
        EXAT,
        PXAT,
        KEEPTTL
    }

    internal enum EtagOption : byte
    {
        None,
        WithETag,
    }

    internal enum ExistOptions : byte
    {
        None,
        NX,
        XX
    }
}