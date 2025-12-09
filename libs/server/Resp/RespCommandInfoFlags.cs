// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;

namespace Garnet.server
{
    /// <summary>
    /// RESP command flags
    /// </summary>
    [Flags]
    public enum RespCommandFlags
    {
        None = 0,
        [Description("admin")]
        Admin = 1,
        [Description("asking")]
        Asking = 1 << 1,
        [Description("blocking")]
        Blocking = 1 << 2,
        [Description("denyoom")]
        DenyOom = 1 << 3,
        [Description("fast")]
        Fast = 1 << 4,
        [Description("loading")]
        Loading = 1 << 5,
        [Description("movablekeys")]
        MovableKeys = 1 << 6,
        [Description("no_auth")]
        NoAuth = 1 << 7,
        [Description("no_async_loading")]
        NoAsyncLoading = 1 << 8,
        [Description("no_mandatory_keys")]
        NoMandatoryKeys = 1 << 9,
        [Description("no_multi")]
        NoMulti = 1 << 10,
        [Description("noscript")]
        NoScript = 1 << 11,
        [Description("pubsub")]
        PubSub = 1 << 12,
        [Description("random")]
        Random = 1 << 13,
        [Description("readonly")]
        ReadOnly = 1 << 14,
        [Description("sort_for_script")]
        SortForScript = 1 << 15,
        [Description("skip_monitor")]
        SkipMonitor = 1 << 16,
        [Description("skip_slowlog")]
        SkipSlowLog = 1 << 17,
        [Description("stale")]
        Stale = 1 << 18,
        [Description("write")]
        Write = 1 << 19,
        [Description("allow_busy")]
        AllowBusy = 1 << 20,
    }

    /// <summary>
    /// RESP ACL categories
    /// </summary>
    [Flags]
    public enum RespAclCategories
    {
        None = 0,
        [Description("admin")]
        Admin = 1,
        [Description("bitmap")]
        Bitmap = 1 << 1,
        [Description("blocking")]
        Blocking = 1 << 2,
        [Description("connection")]
        Connection = 1 << 3,
        [Description("dangerous")]
        Dangerous = 1 << 4,
        [Description("geo")]
        Geo = 1 << 5,
        [Description("hash")]
        Hash = 1 << 6,
        [Description("hyperloglog")]
        HyperLogLog = 1 << 7,
        [Description("fast")]
        Fast = 1 << 8,
        [Description("keyspace")]
        KeySpace = 1 << 9,
        [Description("list")]
        List = 1 << 10,
        [Description("pubsub")]
        PubSub = 1 << 11,
        [Description("read")]
        Read = 1 << 12,
        [Description("scripting")]
        Scripting = 1 << 13,
        [Description("set")]
        Set = 1 << 14,
        [Description("sortedset")]
        SortedSet = 1 << 15,
        [Description("slow")]
        Slow = 1 << 16,
        [Description("stream")]
        Stream = 1 << 17,
        [Description("string")]
        String = 1 << 18,
        [Description("transaction")]
        Transaction = 1 << 19,
        [Description("write")]
        Write = 1 << 20,
        [Description("garnet")]
        Garnet = 1 << 21,
        [Description("custom")]
        Custom = 1 << 22, 
        [Description("meta")]
        Meta = 1 << 23,
        [Description("all")]
        All = (Meta << 1) - 1,
    }
}