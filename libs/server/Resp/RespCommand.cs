// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Garnet.server
{
    /// <summary>
    /// Basic RESP command enum
    /// </summary>
    enum RespCommand : byte
    {
        NONE,

        //Read Only Commands
        GET = (0x40 | 0x0),
        GETRANGE = (0x40 | 0x1),
        MGET = (0x40 | 0x2),
        GETBIT = (0x40 | 0x3),
        BITCOUNT = (0x40 | 0x4),
        BITPOS = (0x40 | 0x5),
        BITFIELD_RO = (0x40 | 0x6),
        PFCOUNT = (0x40 | 0x7),
        EXISTS = (0x40 | 0x8),
        TTL = (0x40 | 0x9),
        PTTL = (0x40 | 0x10),
        STRLEN = 0x26,
        COSCAN = 0x27,

        //Upsert or RMW Commands
        SET = (0x80 | 0x0),
        MSET = (0x80 | 0x1),
        PSETEX = (0x80 | 0x2),
        SETEX = (0x80 | 0x3),
        SETEXNX = (0x80 | 0x4),
        SETEXXX = (0x80 | 0x5),
        SETKEEPTTL = (0x80 | 0x6),
        SETKEEPTTLXX = (0x80 | 0x7),
        SETBIT = (0x80 | 0x8),
        BITOP = (0x80 | 0x9),
        BITFIELD = (0x80 | 0xA),
        PFADD = (0x80 | 0xB),
        PFMERGE = (0x80 | 0xC),
        INCR = (0x80 | 0xD),
        INCRBY = (0x80 | 0xE),
        DECR = (0x80 | 0xF),
        DECRBY = (0x80 | 0x10),
        RENAME = (0x80 | 0x11),
        PERSIST = (0x80 | 0x12),
        EXPIRE = (0x80 | 0x13),
        DEL = (0x80 | 0x14),
        PEXPIRE = (0x80 | 0x15),
        SETRANGE = (0x80 | 0x16),
        GETDEL = (0x80 | 0x17),
        MSETNX = (0x80 | 0x18),
        APPEND = (0x80 | 0x19),

        //Object Store Commands
        SortedSet = (0xC0 | 0x0),
        List = (0xC0 | 0x1),
        Hash = (0xC0 | 0x2),
        Set = (0xC0 | 0x3),
        All = (0xC0 | 0x26),

        //Admin commands
        PING = 0x1,
        QUIT = 0x2,
        AUTH = 0x3,
        COMMAND = 0x4,
        DBSIZE = 0x5,
        KEYS = 0x6,
        PUBLISH = 0x7,
        SUBSCRIBE = 0x8,
        PSUBSCRIBE = 0x9,
        UNSUBSCRIBE = 0xA,
        PUNSUBSCRIBE = 0xB,
        NOAUTH = 0xC,
        ASKING = 0xD,
        MIGRATE = 0xE,
        SELECT = 0xF,
        ECHO = 0x10,
        CONFIG = 0x11,
        CLIENT = 0x12,
        UNLINK = 0x13,
        TYPE = 0x20,
        SCAN = 0x21,
        MEMORY = 0x23,
        MONITOR = 0x24,
        MODULE = 0x25,
        REGISTERCS = 0x28,

        //Txn Commands
        MULTI = 0x14,
        EXEC = 0x15,
        DISCARD = 0x16,
        WATCH = 0x17,
        WATCHMS = 0x18,
        WATCHOS = 0x19,
        UNWATCH = 0x1A,
        RUNTXP = 0x1B,

        //Cluster commands
        READONLY = 0x1C,
        READWRITE = 0x1D,
        REPLICAOF = 0x1E,
        SECONDARYOF = 0x1F
    }

    /// <summary>
    /// RESP command options enum
    /// </summary>
    enum RespCommandOption : byte
    {
        EX, NX, XX, GET, PX, EXAT, PXAT, PERSIST, GT, LT
    }

    /// <summary>
    /// Server session for RESP protocol - command definitions and fast parsing
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Fast-parses for command type
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RespCommand FastParseCommand(byte* ptr)
        {
            // TODO: make sure this condition is appropriate per command
            if (bytesRead - readHead >= 19)
            {
                // GET key1 value1 => [*2\r\n$3\r\nGET\r\n$]3\r\nkey\r\n
                if (*(long*)ptr == 724291344956994090L && *(2 + (int*)ptr) == 223626567 && *(ushort*)(12 + ptr) == 9226)
                    return RespCommand.GET;

                // SET key1 value1 => [*3\r\n$3\r\nSET\r\n$]3\r\nkey\r\n$5\r\nvalue\r\n
                if (*(long*)ptr == 724291344956994346L && *(2 + (int*)ptr) == 223626579 && *(ushort*)(12 + ptr) == 9226)
                    return RespCommand.SET;

                // setrange key offset value => [*4\r\n$8\r\nSETRANGE\r\nkey\r\noffset\r\nvalue]
                if (*(long*)ptr == 724296842515133482L && *(long*)(ptr + 8) == 4992044754424579411L && *(ushort*)(ptr + 16) == 2573)
                    return RespCommand.SETRANGE;

                // GETRANGE key start length [*4\r\n$8\r\n GETSLICE \r\n key start legth]
                if (*(long*)ptr == 724296842515133482L && *(long*)(ptr + 8) == 4992044754424579399L && *(ushort*)(ptr + 16) == 2573)
                    return RespCommand.GETRANGE;

                // PUBLISH channel message => [*3\r\n$7\r\nPUBLISH\r\n$]7\r\nchannel\r\n$7\r\message\r\n
                if (*(long*)ptr == 724295743003505450L && *(2 + (int*)ptr) == 1279415632 && *(ushort*)(16 + ptr) == 9226)
                    return RespCommand.PUBLISH;

                // Single Key DEL key1 [*2\r\n$3\r\nDEL\r\n$]3\r\nkey\r\n
                if (*(long*)ptr == 724291344956994090L && *(2 + (int*)ptr) == 223102276 && *(ushort*)(12 + ptr) == 9226)
                    return RespCommand.DEL;

                // INCR key1 ==> [*2\r\n$4\r\nINCR\r\n$]3\r\nkey\r\n
                if (*(long*)ptr == 724292444468621866L && *(12 + (byte*)ptr) == 0x0d
                    && *(2 + (int*)ptr) == 0x52434e49 && *(ushort*)(13 + ptr) == 9226)
                    return RespCommand.INCR;

                // Fast path for transactions with few arguments (command array size < 10)
                // RUNTXP ==> [*X\r\n$6\r\nRUNTXP\r\n]
                if (*(long*)(ptr + 4) == 6074886758213695012 && *(int*)(ptr + 12) == 168644696)
                {
                    if (*ptr == '*' && *(ushort*)(ptr + 2) == 2573 && *(ptr + 1) >= '0' && *(ptr + 1) <= '9')
                    {
                        return RespCommand.RUNTXP;
                    }
                }

                // EXPIRE key time => [*3\r\n$6\r\nEXPIRE\r\n$]
                if ((*(long*)ptr == 724294643491877674L || *(long*)ptr == 724294643491877930L) && *(1 + (long*)ptr) == 724311334796154949 && *(ushort*)(15 + ptr) == 9226)
                    return RespCommand.EXPIRE;

                // PEXPIRE key time => [*3\r\n$7\r\nPEXPIRE\r\n$]
                if ((*(long*)ptr == 724295743003505706L || *(long*)ptr == 724295743003505450L) && *(1 + (long*)ptr) == 956260970720150864 && *(ptr + 16) == 10)
                    return RespCommand.PEXPIRE;

                // PERSIST key => [*2\r\n$7\r\n PERSIST\r \n]
                if (*(long*)ptr == 724295743003505194L && *(long*)(ptr + 8) == 960484194932376912L && *(ptr + 16) == 10)
                    return RespCommand.PERSIST;

                // TTL key => [*2\r\n$3\r\n TTL\r \n]
                if (*(long*)ptr == 724291344956994090L && *(int*)(ptr + 8) == 223106132 && *(ptr + 12) == 10)
                    return RespCommand.TTL;

                // PTTL key => [*2\r\n$4\r\nPTTL\r\n]
                if (*(long*)ptr == 724292444468621866L && *(int*)(ptr + 8) == 1280595024 && *(ptr + 13) == 10)
                    return RespCommand.PTTL;

                // [| *3\r\n$5\r\n | PFADD\r\n$ | 3\r\nkey\r\n$]
                if (*(long*)ptr == 724293543980249898L && *(long*)(ptr + 8) == 2596902721986578000L)
                    return RespCommand.PFADD;

                //[ *2\r\n$7\r\n | PFCOUNT\r | \n$3\r\nkey ]
                if (*(long*)ptr == 724295743003505194L && *(long*)(ptr + 8) == 960478748845753936L)
                    return RespCommand.PFCOUNT;

                //[ *2\r\n$7\r\n | PFMERGE\r | \n$3\r\nkey ]
                if (*(long*)ptr == 724295743003505450L && *(long*)(ptr + 8) == 956248914561680976)
                    return RespCommand.PFMERGE;

                // INCRBY key 1000 ==> [*3\r\n$6\r\nINCRBY\r\n$]4\r\nkey\r\n4\r\1000\r\n => Increment the key by 1000
                if (*(long*)ptr == 0x0a0d36240a0d332a && *((long*)ptr + 1) == 0x0a0d594252434e49 && 0x24 == *(16 + (byte*)ptr))
                    return RespCommand.INCRBY;

                // DECR key1 ==> [*2\r\n$4\r\nDECR\r\n$]3\r\nkey\r\n
                if (*(long*)ptr == 724292444468621866L && *(12 + (byte*)ptr) == 0x0d
                               && *(2 + (int*)ptr) == 0x52434544 && *(ushort*)(13 + ptr) == 9226)
                    return RespCommand.DECR;

                // DECRBY key 1000 ==> [*3\r\n$6\r\nDECRBY\r\n$]4\r\nkey\r\n4\r\1000\r\n => Decrement the key by 1000
                if (*(long*)ptr == 0x0a0d36240a0d332a && *((long*)ptr + 1) == 0x0a0d594252434544 && 0x24 == *(16 + (byte*)ptr))
                    return RespCommand.DECRBY;

                // EXISTS key1 ==> [*2\r\n$6\r\nEXISTS\r\n$]4\r\nkey1\r\n => Does key1 exist in the store
                if (*(long*)ptr == 0x0a0d36240a0d322a && *((long*)ptr + 1) == 0x0a0d535453495845 && 0x24 == *(16 + (byte*)ptr))
                    return RespCommand.EXISTS;

                // SETEX
                if (*(long*)ptr == 724293543980250154L && *(1 + (long*)ptr) == 2596902807903946067L)
                    return RespCommand.SETEX;

                // PSETEX
                if (*(long*)ptr == 724294643491877930L && *(1 + (long*)ptr) == 724332169866335056L)
                    return RespCommand.PSETEX;

                // SET [EX KEEPTTL] [NX XX] [GET]
                if ((*(long*)ptr == 724291344956994602L || // *4
                    *(long*)ptr == 724291344956994858L ||  // *5
                    *(long*)ptr == 724291344956995114L ||  // *6
                    *(long*)ptr == 724291344956995370L)    // *7
                    && *(2 + (int*)ptr) == 223626579 && *(ushort*)(12 + ptr) == 9226)
                    return RespCommand.SETEXNX;

                // RENAME key1 key2 ==> [*3\r\n$6\r\nRENAME\r\n$]4\r\nkey1\r\n$4\r\nkey2\r\n => Rename key1 to key2 in the store
                if (*(long*)ptr == 0x0a0d36240a0d332a && *((long*)ptr + 1) == 0x0a0d454d414e4552 && 0x24 == *(16 + (byte*)ptr))
                    return RespCommand.RENAME;

                // *4\r\n$6\r\n | SETBIT\r\n | $3\r\nkey\r\n $4\r\n4444\r\n $1\r\n [1|0]\r\n
                if (*(long*)ptr == 724294643491877930L && *(long*)(ptr + 8) == 724327788698682707L)
                    return RespCommand.SETBIT;

                // *3\r\n$6\r\n | GETBIT\r\n | $3\r\nkey\r\n $4\r\n4444\r\n
                if (*(long*)ptr == 724294643491877674L && *(long*)(ptr + 8) == 724327788698682695L)
                    return RespCommand.GETBIT;

                // *2\r\n$8\r\n | BITCOUNT | \r\n | $3\r\nkey\r\n \\ without [start end] offsets
                // *3\r\n$8\r\n | BITCOUNT | \r\n | $3\r\nkey\r\n | $4\r\n4444\r\n \\ with start offset only
                // *4\r\n$8\r\n | BITCOUNT | \r\n | $3\r\nkey\r\n | $4\r\n4444\r\n | $4\r\n8888\r\n \\ with start and end offsets
                // *5\r\n$8\r\n | BITCOUNT | \r\n | $3\r\nkey\r\n | $4\r\n4444\r\n | $4\r\n8888\r\n | $[3|4]\r\n[BIT|BYTE]\r\n \\ with start and end offsets
                if ((*(long*)ptr == 724296842515132970L ||
                    *(long*)ptr == 724296842515133226L ||
                    *(long*)ptr == 724296842515133482L ||
                    *(long*)ptr == 724296842515133738L) &&
                    *(long*)(ptr + 8) == 6074886746289752386L && *(ushort*)(ptr + 16) == 2573)
                    return RespCommand.BITCOUNT;

                // *3\r\n$6\r\n | BITPOS\r\n | $3\r\nkey\r\n | $1\r\n[1|0]\r\n
                // *4\r\n$6\r\n | BITPOS\r\n | $3\r\nkey\r\n | $1\r\n[1|0]\r\n | $4\r\n4444\r\n
                // *5\r\n$6\r\n | BITPOS\r\n | $3\r\nkey\r\n | $1\r\n[1|0]\r\n | $4\r\n4444\r\n | $4\r\n8888\r\n
                // *6\r\n$6\r\n | BITPOS\r\n | $3\r\nkey\r\n | $1\r\n[1|0]\r\n | $4\r\n4444\r\n | $4\r\n8888\r\n | $[3|4]\r\n[BIT|BYTE]\r\n
                if ((*(long*)ptr == 724294643491877674L ||
                    *(long*)ptr == 724294643491877930L ||
                    *(long*)ptr == 724294643491878186L ||
                    *(long*)ptr == 724294643491878442L) &&
                    *(long*)(ptr + 8) == 724326715191740738L)
                    return RespCommand.BITPOS;

                // *2\r\n$6\r\n | GETDEL\r\n | $3\r\nkey\r\n
                if (*(long*)ptr == 0x0a0d36240a0d322a && *(long*)(ptr + 8) == 0x0a0d4c4544544547)
                    return RespCommand.GETDEL;

                // 2a 33 0d 0a 24 36 0d 0a
                // 41 50 50 45 4e 44 0d 0a
                // *3..$6..APPEND..$2..k1..$4..val1..
                if (*(long*)ptr == 0x0a0d36240a0d332a && *(long*)(ptr + 8) == 0x0a0d444e45505041)
                    return RespCommand.APPEND;
            }

            if (bytesRead - readHead >= 6)
            {
                // PING => [PING\r\n]
                if (*(int*)ptr == 1196312912 && *(ushort*)(4 + ptr) == 2573)
                    return RespCommand.PING;
                //*1\r\n$4\r\n PING\r\n
                if (*(long*)ptr == 724292444468621610L && *(int*)(ptr + 8) == 1196312912 && *(ushort*)(ptr + 12) == 2573)
                    return RespCommand.PING;

                // QUIT => [QUIT\r\n]
                if (*(int*)ptr == 1414092113 && *(ushort*)(4 + ptr) == 2573)
                    return RespCommand.QUIT;

                //*1\r\n$6\r\n ASKING\r\n
                if (*(long*)ptr == 724294643491877162L && *(long*)(ptr + 8) == 724313516639212353L)
                    return RespCommand.ASKING;

                //*1\r\n$8\r\n READONLY \r\n
                if (*(long*)ptr == 724296842515132714L && *(long*)(ptr + 8) == 6434604069960107346L && *(short*)(ptr + 16) == 2573)
                    return RespCommand.READONLY;

                //*1\r\n$9\r\n READWRIT E\r\n
                if (*(long*)ptr == 724297942026760490L && *(long*)(ptr + 8) == 6073476107246585170L && *(short*)(ptr + 16) == 3397)
                    return RespCommand.READWRITE;

                // MULTI => *1\r\n$5\r\nMULTI\r\n
                if (*(long*)ptr == 724293543980249386L && *(2 + (int*)ptr) == 1414288717 && *(ushort*)(12 + ptr) == 3401 && *(14 + ptr) == 10)
                    return RespCommand.MULTI;

                // DISCARD => *1\r\n$7\r\nDISCARD\r\n
                if (*(long*)ptr == 724295743003504938L && *(1 + (long*)ptr) == 955979461165271364L && *(16 + ptr) == 10)
                    return RespCommand.DISCARD;

                // EXEC => *1\r\n$4\r\nEXEC\r\n
                if (*(long*)ptr == 724292444468621610L && *(2 + (int*)ptr) == 1128618053 && *(ushort*)(12 + ptr) == 2573)
                    return RespCommand.EXEC;

                // UNWATCH => *1\r\n$7\r\UNWATCH\r\n
                if (*(long*)ptr == 724295743003504938L && *(1 + (long*)ptr) == 957088949968784981L && *(16 + ptr) == 10)
                    return RespCommand.UNWATCH;
            }

            // TODO: add other types here
            return RespCommand.NONE;
        }

        private (RespCommand, byte) FastParseArrayCommand(int count, byte* ptr)
        {
            if (bytesRead - readHead >= 10)
            {
                // [$4\r\nMGET\r\n]
                if (*(long*)ptr == 6072338068785673252L && *(ushort*)(ptr + 8) == 2573)
                    return (RespCommand.MGET, 0);

                // [$4\r\nMSET\r\n]
                if (*(long*)(recvBufferPtr + readHead) == 6072351262925206564L && *(ushort*)(recvBufferPtr + readHead + 8) == 2573)
                    return (RespCommand.MSET, 0);

                // [$6\r\nMSETNX\r\n]
                if (*(long*)ptr == 6072351262925207076L && *(int*)(ptr + 8) == 168646734)
                    return (RespCommand.MSETNX, 0);

                // [$3\r\nDEL\r \n]
                if (*(long*)ptr == 958216979251802916L && *(ptr + 8) == 10)
                    return (RespCommand.DEL, 0);

                // [$6\r\nUNLI NK\r\n]
                if (*(long*)ptr == 5281682590146573860L && *(int*)(ptr + 8) == 168643406)
                    return (RespCommand.UNLINK, 0);

                // SUBSCRIBE channel1 channel2.. ==> [$9\r\nSUBSCRIBE\r\n$]8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2
                if (*(int*)ptr == 168638756 && *(long*)((int*)ptr + 1) == 4776439328916264275L && 0x24 == *(15 + (byte*)ptr))
                    return (RespCommand.SUBSCRIBE, 0);

                // PSUBSCRIBE channel1 channel2.. ==> [$10\r\nPSUBSCRIBE\r\n$]8\r\nchannel1\r\n$8\r\nchannel2\r\n => PSubscribe to channel1 and channel2
                if (*(int*)ptr == 221262116 && *(long*)(ptr + 5) == 5283359337733247824L && 0x24 == *(17 + (byte*)ptr))
                    return (RespCommand.PSUBSCRIBE, 0);

                // UNSUBSCRIBE channel1 channel2.. ==> [$11\r\nUNSUBSCRIBE\r\n]$8\r\nchannel1\r\n$8\r\nchannel2\r\n => Unsubscribe to channel1 and channel2
                if (*(int*)ptr == 221327652 && *(long*)((int*)ptr + 1) == 4851294157845452042L && 10 == *(17 + (byte*)ptr))
                    return (RespCommand.UNSUBSCRIBE, 0);

                // PUNSUBSCRIBE channel1 channel2.. ==> [$12\r\nPUNSUBSCRIBE\r\n]$8\r\nchannel1\r\n$8\r\nchannel2\r\n => Punsubscribe to channel1 and channel2
                if (*(int*)ptr == 221393188 && *(long*)(ptr + 5) == 4851294157845452112L && 10 == *(18 + (byte*)ptr))
                    return (RespCommand.PUNSUBSCRIBE, 0);

                // [$4\r\nZADD\r\n]
                if (*(long*)ptr == 4919128547966923812L && *(ushort*)(ptr + 8) == 2573)
                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZADD);

                // [$4\r\nZREM\r\n]
                if (*(long*)ptr == 5567947060982658084L && *(ushort*)(ptr + 8) == 2573)
                    return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREM);

                //  -----8----- ---4---
                // [$5\r\nPFAD | D\r\n$]
                if (*(long*)ptr == 4918289577645258020L && *(int*)(ptr + 8) == 604638532)
                    return (RespCommand.PFADD, 0);

                //  -----8----- ---4--- --2--
                // [$5\r\nPFME | RGE\r | \n$]
                if (*(long*)ptr == 4993724871403714340L && *(int*)(ptr + 8) == 222644050 && *(short*)(ptr + 8 + 4) == 9226)
                    return (RespCommand.PFMERGE, 0);

                //-----8-----  ---4---
                //[$7\r\nPFCO | UNT\r]
                if (*(long*)ptr == 5711486062015887140L && *(int*)(ptr + 8) == 223628885)
                    return (RespCommand.PFCOUNT, 0);

                //[$6\r\nSELECT\r]
                if (*(long*)ptr == 4993442309800277540L && *(int*)(ptr + 8) == 168645699)
                    return (RespCommand.SELECT, 0);

                //[$4\r\nKEYS\r\n]
                if (*(long*)ptr == 6005907766668768292L && *(ushort*)(ptr + 8) == 2573)
                    return (RespCommand.KEYS, 0);

                //[$6\r\nDBSIZE\r\n]
                if (*(long*)ptr == 5283639647829571108L && *(ushort*)(ptr + 8) == 17754)
                    return (RespCommand.DBSIZE, 0);

                //$6\r\nEXISTS\r\n -> multiple keys exists
                if (*(long*)ptr == 6001425031992522276L && *(ushort*)(ptr + 8) == 21332)
                    return (RespCommand.EXISTS, 0);

                //[$4\r\nSCAN\r\n]
                if (*(long*)ptr == 5638862232374555684L && *(ushort*)(ptr + 8) == 2573)
                    return (RespCommand.SCAN, 0);

                //[$4\r\nTYPE\r\n]
                if (*(long*)ptr == 4994590204234642468L && *(ushort*)(ptr + 8) == 2573)
                    return (RespCommand.TYPE, 0);

                //[$6\r\nMEMORY\r\n]
                if (*(long*)ptr == 5714299699386463780L && *(ushort*)(ptr + 8) == 22866)
                    return (RespCommand.MEMORY, 0);

                //[$7\r\nMONITOR\r\n]
                if (*(long*)ptr == 5638862232374555684L && *(ushort*)(ptr + 8) == 2573)
                    return (RespCommand.MONITOR, 0);

                // STRLEN key
                if (*(long*)ptr == 5499550810600453668L && *(ushort*)(ptr + 8) == 20037)
                    return (RespCommand.STRLEN, 0);

                // MODULE NAMEOFMODULE
                if (*(long*)ptr == 6144122983939913252L && *(ushort*)(ptr + 8) == 17740)
                    return (RespCommand.MODULE, 0);

                // [$16\r\nCUSTOMOBJECTSCAN\r\n$5\r\nKey\r\n$1\r\n0\r\n$4\r\nPATTERN\r\nCOUNT]
                if (*(int*)ptr == 221655332 && *(long*)(ptr + 5) == 4778122732775888195L && 10 == *(22 + (byte*)ptr))
                    return (RespCommand.All, (byte)RespCommand.COSCAN);

                if (bytesRead - readHead >= 11)
                {
                    //[$5\r\nWATCH\r\n$2\r\nOS\r\n]
                    if (*(long*)ptr == 4851574540671464740 && *(long*)(ptr + 8) == 5695379187767577928 && *(ushort*)(ptr + 16) == 3411 && *(byte*)(ptr + 18) == 10)
                        return (RespCommand.WATCHOS, 0);

                    //[$5\r\nWATCH\r\n$2\r\nMS\r\n]
                    if (*(long*)ptr == 4851574540671464740 && *(long*)(ptr + 8) == 5551263999691722056 && *(ushort*)(ptr + 16) == 3411 && *(byte*)(ptr + 18) == 10)
                        return (RespCommand.WATCHMS, 0);

                    //[$5\r\nWATCH\r\n]
                    if (*(long*)ptr == 4851574540671464740 && *(ushort*)(ptr + 8) == 3400 && *(byte*)(ptr + 10) == 10)
                        return (RespCommand.WATCH, 0);

                    #region SortedSet Operations
                    //-----8-----  ---3---
                    // [$5\r\nZCAR D\r\n]
                    if (*(long*)ptr == 5927092638591038756L && *(ushort*)(ptr + 8) == 3396 && *(ptr + 10) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZCARD);

                    //-----8-----  ---5---
                    // [$7\r\nZPOP MAX\r\n]
                    if (*(long*)ptr == 5786932393840293668L && *(int*)(ptr + 8) == 223887693 && *(ptr + 12) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMAX);

                    // [$7\r\nZSCORE\r\n]
                    if (*(long*)ptr == 5711500398616720932L && *(int*)(ptr + 8) == 168641874)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCORE);

                    //[$6|ZCOUNT|] = 12 bytes = 8 (long) + 2 (ushort) + 2 bytes
                    if (*(long*)ptr == 6147206070378772004L && *(ushort*)(ptr + 8) == 21582 && *(ptr + 11) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZCOUNT);

                    //[$7|ZINCRBY|] = 13 bytes = 8 (long) + 2 (ushort) + 3 bytes
                    if (*(long*)ptr == 4849894499789125412L && *(ushort*)(ptr + 8) == 16978 && *(ptr + 12) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZINCRBY);

                    //[$5|ZRANK|] = 11 bytes = 8 (long) + 2 (ushort) + 1 bytes
                    if (*(long*)ptr == 5638878755113743652L && *(ushort*)(ptr + 8) == 3403 && *(ptr + 10) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANK);

                    //[$6|ZRANGE|] = 12 bytes = 8 (long) + 2 (ushort) + 2 bytes
                    if (*(long*)ptr == 5638878755113743908L && *(ushort*)(ptr + 8) == 17735 && *(ptr + 11) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGE);

                    //[$13|ZRANGEBYSCORE|] = 19 bytes = 8 (long) + 2 (ushort) + 9 bytes
                    if (*(long*)ptr == 4706923559773221156L && *(ushort*)(ptr + 8) == 18254 && *(ptr + 19) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANGEBYSCORE);

                    //[$8|ZREVRANK|] = 14 bytes = 8 (long) + 2 (ushort) + 4 bytes
                    if (*(long*)ptr == 6216465407324010532L && *(ushort*)(ptr + 8) == 16722 && *(ptr + 13) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANK);

                    //[$14|ZREMRANGEBYLEX|] = 20 bytes = 8 (long) + 2 (ushort) + 10 bytes
                    if (*(long*)ptr == 4995153935924998436L && *(ushort*)(ptr + 8) == 21069 && *(ptr + 20) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYLEX);

                    //[$15|ZREMRANGEBYRANK|] = 21 bytes = 8 (long) + 2 (ushort) + 11 bytes
                    if (*(long*)ptr == 4995153935925063972L && *(ushort*)(ptr + 8) == 21069 && *(ptr + 21) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYRANK);

                    //[$16|ZREMRANGEBYSCORE|] = 22 bytes = 8 (long) + 2 (ushort) + 12 bytes
                    if (*(long*)ptr == 4995153935925129508L && *(ushort*)(ptr + 8) == 21069 && *(ptr + 22) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREMRANGEBYSCORE);

                    //[$9|ZLEXCOUNT|] = 15 bytes = 8 (long) + 2 (ushort) + 5 bytes
                    if (*(long*)ptr == 6360573998330100004L && *(ushort*)(ptr + 8) == 20291 && *(ptr + 14) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZLEXCOUNT);

                    //[$7|ZPOPMIN|] = 13 bytes = 8 (long) + 2 (ushort) + 3 bytes
                    if (*(long*)ptr == 5786932393840293668L && *(ushort*)(ptr + 8) == 18765 && *(ptr + 12) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZPOPMIN);

                    //[$11|ZRANDMEMBER|] = 17 bytes = 8 (long) + 2 (ushort) + 7 bytes
                    if (*(long*)ptr == 4706923559773090084L && *(ushort*)(ptr + 8) == 17486 && *(ptr + 17) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZRANDMEMBER);

                    //[$5|ZDIFF|] = 11 bytes = 8 (long) + 2 (ushort) + 1 bytes
                    if (*(long*)ptr == 5064654409461216548L && *(ushort*)(ptr + 8) == 3398 && *(ptr + 10) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZDIFF);

                    //[$5|ZSCAN|] = 11 bytes = 8 (long) + 2 (ushort) + 1 bytes
                    if (*(long*)ptr == 4702694082085729572L && *(ushort*)(ptr + 8) == 3406 && *(ptr + 10) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZSCAN);

                    #region SortedSet Operations with Geo Commands
                    //[$6|GEOADD|] = 12 bytes = 8 (long) + 2 (ushort) + 2 bytes
                    if (*(long*)ptr == 4706056307039090212L && *(ushort*)(ptr + 8) == 17476 && *(ptr + 11) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOADD);

                    //[$6|GEOHASH|] = 13 bytes = 8 (long) + 2 (ushort) + 3 bytes
                    if (*(long*)ptr == 5210459465304586020L && *(ushort*)(ptr + 8) == 21313 && *(ptr + 12) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOHASH);

                    //[$6|GEODIST|] = 13 bytes = 8 (long) + 2 (ushort) + 3 bytes
                    if (*(long*)ptr == 4922229089152874276L && *(ushort*)(ptr + 8) == 21321 && *(ptr + 12) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.GEODIST);

                    //[$6|GEOPOS|] = 12 bytes = 8 (long) + 2 (ushort) + 2 bytes
                    if (*(long*)ptr == 5786920217608009252L && *(ushort*)(ptr + 8) == 21327 && *(ptr + 11) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOPOS);

                    //[$6|GEOSEARCH|] = 15 bytes = 8 (long) + 2 (ushort) + 5 bytes
                    if (*(long*)ptr == 6003092999721793828L && *(ushort*)(ptr + 8) == 16709 && *(ptr + 14) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.GEOSEARCH);

                    //[$9|ZREVRANGE|] = 15 bytes = 8 (long) + 2 (ushort) + 5 bytes
                    if (*(long*)ptr == 6216465407324010788L && *(ushort*)(ptr + 8) == 16722 && *(ptr + 14) == 10)
                        return (RespCommand.SortedSet, (byte)SortedSetOperation.ZREVRANGE);

                    #endregion

                    #endregion

                    #region List Operations
                    //[$5\r\nLPUSH\r\n$]
                    if (*(long*)ptr == 0x5355504c0a0d3524 && *(int*)(ptr + 8) == 0x240a0d48)
                        return (RespCommand.List, (byte)ListOperation.LPUSH);
                    if (*(long*)ptr == 6004793965684799012L && *(int*)(ptr + 8) == 168646728)
                        return (RespCommand.List, (byte)ListOperation.LPUSHX);
                    //[$4\r\nLPOP\r\n$]
                    if (*(long*)ptr == 0x504f504c0a0d3424 && *(ushort*)(ptr + 8) == 0x0a0d && *(ptr + 10) == 0x24)
                        return (RespCommand.List, (byte)ListOperation.LPOP);
                    //[$5\r\nRPUSH\r\n$]
                    if (*(long*)ptr == 0x535550520a0d3524 && *(int*)(ptr + 8) == 0x240a0d48)
                        return (RespCommand.List, (byte)ListOperation.RPUSH);
                    if (*(long*)ptr == 6004793991454602788L && *(int*)(ptr + 8) == 168646728)
                        return (RespCommand.List, (byte)ListOperation.RPUSHX);
                    //[$4\r\nRPOP\r\n$]
                    if (*(long*)ptr == 0x504f50520a0d3424 && *(ushort*)(ptr + 8) == 0x0a0d && *(ptr + 10) == 0x24)
                        return (RespCommand.List, (byte)ListOperation.RPOP);
                    //[$4\r\nLLEN\r\n$]
                    if (*(long*)ptr == 0x4e454c4c0a0d3424 && *(ushort*)(ptr + 8) == 0x0a0d && *(ptr + 10) == 0x24)
                        return (RespCommand.List, (byte)ListOperation.LLEN);
                    //[$5\r\nLTRIM\r\n$]
                    if (*(long*)ptr == 0x4952544c0a0d3524 && *(int*)(ptr + 8) == 0x240a0d4d)
                        return (RespCommand.List, (byte)ListOperation.LTRIM);
                    //[$6\r\nLRANGE\r\n$9\r\n]
                    if (*(long*)ptr == 0x4e41524c0a0d3624 && *(int*)(ptr + 8) == 0x0a0d4547 && *(ptr + 12) == 0x24)
                        return (RespCommand.List, (byte)ListOperation.LRANGE);
                    //[$6\r\nLINDEX\r\n$9\r\n$]
                    if (*(long*)ptr == 0x444e494c0a0d3624 && *(int*)(ptr + 8) == 0x0a0d5845 && *(ptr + 12) == 0x24)
                        return (RespCommand.List, (byte)ListOperation.LINDEX);
                    //$7\r\nLINSERT\r\n$9\r\nList_Test\r\n$
                    if (*(long*)ptr == 0x534e494c0a0d3724 && *(int*)(ptr + 8) == 0x0d545245 && *(ushort*)(ptr + 12) == 0x240a)
                        return (RespCommand.List, (byte)ListOperation.LINSERT);
                    //[$4\r\nLREM\r\n$]
                    if (*(long*)ptr == 0x4d45524c0a0d3424 && *(ushort*)(ptr + 8) == 0x0a0d && *(ptr + 10) == 0x24)
                        return (RespCommand.List, (byte)ListOperation.LREM);
                    //-----8----- ---7-----
                    //[$9\r\nRPOP LPUSH\r\n]
                    if (*(long*)ptr == 5786932359480555812L && *(int*)(ptr + 8) == 1398100044 && *(ptr + 14) == 10)
                        return (RespCommand.List, (byte)ListOperation.RPOPLPUSH);
                    //-----8----- --3--
                    //[$5\r\nLMOV E\r\n]
                    if (*(long*)ptr == 6219274599403435300L && *(int*)(ptr + 8) == 604638533 && *(ptr + 10) == 10)
                        return (RespCommand.List, (byte)ListOperation.LMOVE);
                    #endregion

                    #region Hash Operations
                    //[$5|HMSET|] = 11 bytes = 8 (long) + 2 (ushort) + 1 (byte)
                    if (*(long*)ptr == 4995421383485633828L && *(ushort*)(ptr + 8) == 3412 && *(ptr + 10) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HMSET);
                    //[$4|HSET|] = 10 bytes = 8 (long) + 2 (ushort)
                    if (*(long*)ptr == 6072351241450370084L && *(ushort*)(ptr + 8) == 2573)
                        return (RespCommand.Hash, (byte)HashOperation.HSET);
                    //[$4|HGET|$] = 10 bytes = 8 (long) + 2 (ushort)
                    if (*(long*)ptr == 6072338047310836772L && *(ushort*)(ptr + 8) == 2573)
                        return (RespCommand.Hash, (byte)HashOperation.HGET);
                    //[$5|HMGET|] = 11 bytes = 8 (long) + 2 (ushort) + 1 (byte)
                    if (*(long*)ptr == 4992043683765105956L && *(ushort*)(ptr + 8) == 3412 && *(ptr + 10) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HMGET);
                    //[$4|HDEL|] = 10 bytes = 8 (long) + 2 (ushort)
                    if (*(long*)ptr == 5495873996472529956L && *(ushort*)(ptr + 8) == 2573)
                        return (RespCommand.Hash, (byte)HashOperation.HDEL);
                    //[$4|HLEN|] = 10 bytes = 8 (long) + 2 (ushort)
                    if (*(long*)ptr == 5639997980641408036L && *(ushort*)(ptr + 8) == 2573)
                        return (RespCommand.Hash, (byte)HashOperation.HLEN);
                    //[$7|HGETALL|] = 13 bytes = 8 (long) + 2 (ushort) + 3 bytes
                    if (*(long*)ptr == 6072338047310837540L && *(ushort*)(ptr + 8) == 19521 && *(ptr + 12) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HGETALL);
                    //[$7|HEXISTS|] = 13 bytes = 8 (long) + 2 (ushort) + 3 bytes
                    if (*(long*)ptr == 5285050338427877156L && *(ushort*)(ptr + 8) == 21587 && *(ptr + 12) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HEXISTS);
                    //[$5|HKEYS|] = 11 bytes = 8 (long) + 2 (ushort) + 1 byte
                    if (*(long*)ptr == 6432630415546987812L && *(ushort*)(ptr + 8) == 3411 && *(ptr + 10) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HKEYS);
                    //[$5|HVALS|] = 11 bytes = 8 (long) + 2 (ushort) + 1 byte
                    if (*(long*)ptr == 5494767887774987556L && *(ushort*)(ptr + 8) == 3411 && *(ptr + 10) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HVALS);
                    //[$7|HINCRBY|] = 13 bytes = 8 (long) + 2 (ushort) + 3 bytes
                    if (*(long*)ptr == 4849894422479714084L && *(ushort*)(ptr + 8) == 16978 && *(ptr + 12) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HINCRBY);
                    //[$12|HINCRBYFLOAT|] = 18 bytes = 8 (long) + 2 (ushort) + 7 bytes
                    if (*(long*)ptr == 5641119216266522916L && *(ushort*)(ptr + 8) == 21059 && *(ptr + 18) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HINCRBYFLOAT);
                    //[$6|HSETNX|] = 12 bytes = 8 (long) + 2 (ushort) + 2 bytes
                    if (*(long*)ptr == 6072351241450370596L && *(ushort*)(ptr + 8) == 22606 && *(ptr + 11) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HSETNX);
                    //[$10|HRANDFIELD|] = 16 bytes = 8 (long) + 2 (ushort) + 6 bytes
                    if (*(long*)ptr == 4706903768563724580L && *(ushort*)(ptr + 8) == 17486 && *(ptr + 16) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HRANDFIELD);
                    if (*(long*)ptr == 4702694004776318244L && *(ushort*)(ptr + 8) == 3406 && *(ptr + 10) == 10)
                        return (RespCommand.Hash, (byte)HashOperation.HSCAN);
                    #endregion

                    #region Set Operations
                    //[$4|SADD|] = 10 bytes = 8 (long) + 2 (ushort)
                    if (*(long*)ptr == 4919128517902152740L && *(ushort*)(ptr + 8) == 2573 && *(ptr + 9) == 10)
                        return (RespCommand.Set, (byte)SetOperation.SADD);
                    //[$8|SMEMBERS|] = 14 bytes = 8 (long) + 2 (ushort) + 2 bytes
                    if (*(long*)ptr == 5567941533359749156L && *(ushort*)(ptr + 8) == 17730 && *(ptr + 13) == 10)
                        return (RespCommand.Set, (byte)SetOperation.SMEMBERS);
                    //[$4|SREM|] = 10 bytes = 8 (long) + 2 (ushort) 
                    if (*(long*)ptr == 5567947030917887012L && *(ushort*)(ptr + 8) == 2573 && *(ptr + 9) == 10)
                        return (RespCommand.Set, (byte)SetOperation.SREM);
                    //[$5|SCARD|] = 11 bytes = 8 (long) + 2 (ushort) + 1 byte
                    if (*(long*)ptr == 5927092608526267684L && *(ushort*)(ptr + 8) == 3396 && *(ptr + 10) == 10)
                        return (RespCommand.Set, (byte)SetOperation.SCARD);
                    //[$4|SPOP|] = 10 bytes = 8 (long) + 2 (ushort) 
                    if (*(long*)ptr == 5786932363775521828L && *(ushort*)(ptr + 8) == 2573 && *(ptr + 9) == 10)
                        return (RespCommand.Set, (byte)SetOperation.SPOP);
                    //[$5|SSCAN|] = 14 bytes = 8 (long) + 2 (ushort)
                    if (*(long*)ptr == 4702694052020958500L && *(ushort*)(ptr + 8) == 3406 && *(ptr + 10) == 10)
                        return (RespCommand.Set, (byte)SetOperation.SSCAN);
                    #endregion
                }

                if (bytesRead - readHead >= 20)
                {
                    // -----8-----  --4--  --2--
                    // [$8\r\nBITF | IELD | \r\n]
                    if (*(long*)ptr == 5067756028683958308L && *(int*)(ptr + 8) == 1145849161 && *(short*)(ptr + 8 + 4) == 2573)
                        return (RespCommand.BITFIELD, 0);

                    // -----8-----  ---8---  --2--
                    // [$11\r\nBIT | FIELD_RO | \r\n$]
                    if (*(long*)ptr == 6073458183424258340L && *(long*)(ptr + 8) == 5715735624028604742L && *(short*)(ptr + 8 + 8) == 2573)
                        return (RespCommand.BITFIELD_RO, 0);

                    // $5\r\nBITO | P\r\n$ | [2|3]\r\n[AND|OR|XOR|NOT|]\r\n | $4\r\ndest $4\r\nsrc1\r\n $4\r\nsrc2\r\n
                    if (*(long*)ptr == 5716274375025308964L)
                    {
                        // ----8----    -----8-----   ---4---
                        // $5\r\nBITO | P\r\n$3\r\nA | ND\r\n
                        // $5\r\nBITO | P\r\n$3\r\na | nd\r\n
                        if ((*(long*)(ptr + 8) == 4686572875531554128L && *(int*)(ptr + 16) == 168641614) ||
                            (*(long*)(ptr + 8) == 6992415884745248080L && *(int*)(ptr + 16) == 168649838))
                            return (RespCommand.BITOP, (byte)BitmapOperation.AND);

                        // ----8----    -----8-----   ---4---
                        // $5\r\nBITO | P\r\n$2\r\nO | R\r\n$
                        // $5\r\nBITO | P\r\n$2\r\no | r\r\n$
                        if ((*(long*)(ptr + 8) == 5695379187767577936L && *(int*)(ptr + 16) == 604638546) ||
                            (*(long*)(ptr + 8) == 8001222196981271888L && *(int*)(ptr + 16) == 604638578))
                            return (RespCommand.BITOP, (byte)BitmapOperation.OR);

                        // ----8----    -----8-----   ---4---
                        // $5\r\nBITO | P\r\n$3\r\nX | OR\r\n
                        // $5\r\nBITO | P\r\n$3\r\nx | or\r\n
                        if ((*(long*)(ptr + 8) == 6343897538403896656L && *(int*)(ptr + 16) == 168645199) ||
                            (*(long*)(ptr + 8) == 8649740547617590608L && *(int*)(ptr + 16) == 168653423))
                            return (RespCommand.BITOP, (byte)BitmapOperation.XOR);

                        // ----8----    -----8-----   ---4---
                        // $5\r\nBITO | P\r\n$3\r\nN | OT\r\n
                        if ((*(long*)(ptr + 8) == 5623321598024617296L && *(int*)(ptr + 16) == 168645711) ||
                            (*(long*)(ptr + 8) == 7929164607238311248L && *(int*)(ptr + 16) == 168653935))
                            return (RespCommand.BITOP, (byte)BitmapOperation.NOT);
                    }

                    // [$10\r\nREGISTERCS\r\n$3\r\n]
                    if (*(long*)ptr == 5135601153210331428L && *(long*)(ptr + 8) == 960185166189581129L && *(int*)(ptr + 16) == 221455370 && *(ptr + 20) == 10)
                        return (RespCommand.REGISTERCS, 0);
                }
            }

            // TODO: add other types here
            return (RespCommand.NONE, 0);
        }
    }
}