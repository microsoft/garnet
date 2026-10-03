// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

static class Program
{
    internal const BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    internal static object Member(object instance, string name)
    {
        for (var type = instance.GetType(); type != null; type = type.BaseType)
        {
            var field = type.GetField(name, Flags | BindingFlags.DeclaredOnly);
            if (field != null)
                return field.GetValue(instance);
            var property = type.GetProperty(name, Flags | BindingFlags.DeclaredOnly);
            if (property != null)
                return property.GetValue(instance);
        }
        throw new MissingMemberException(instance.GetType().Name, name);
    }

    internal static void SetField(object instance, string name, object value)
    {
        for (var type = instance.GetType(); type != null; type = type.BaseType)
        {
            var field = type.GetField(name, Flags | BindingFlags.DeclaredOnly);
            if (field != null)
            {
                field.SetValue(instance, value);
                return;
            }
        }
        throw new MissingFieldException(instance.GetType().Name, name);
    }

    internal static byte[] Command(params string[] args)
        => Encoding.UTF8.GetBytes($"*{args.Length}\r\n" + string.Concat(args.Select(arg => $"${Encoding.UTF8.GetByteCount(arg)}\r\n{arg}\r\n")));

    internal static GarnetProvider Provider(GarnetServer server) => (GarnetProvider)Member(server, "Provider");

    static GarnetServerOptions Options(string path, bool aof = false, bool recover = false, bool lua = false, bool noObjects = true) => new()
    {
        EndPoints = [new IPEndPoint(IPAddress.Loopback, 0)],
        LogMemorySize = "16m",
        PageSize = "4k",
        SegmentSize = "16m",
        IndexMemorySize = "128k",
        DisableObjects = noObjects,
        DisablePubSub = true,
        EnableDebugCommand = ConnectionProtectionOption.Yes,
        QuietMode = true,
        EnableAOF = aof,
        CommitFrequencyMs = 0,
        Recover = recover,
        EnableLua = lua,
        LuaTransactionMode = lua,
        LuaOptions = lua ? new LuaOptions() : null,
        CheckpointDir = path
    };

    static void Main(string[] args)
    {
        Trace.Listeners.Add(new ConsoleTraceListener());
        using var loggerFactory = new ProbeLoggerFactory();
        if (args.Contains("abort"))
        {
            ProbeAbort(loggerFactory);
            return;
        }
        if (args.Contains("lua"))
        {
            ProbeLua(loggerFactory);
            return;
        }
        if (args.Contains("append-failure"))
        {
            ProbeAppendFailure(loggerFactory);
            return;
        }
        if (args.Contains("sync-tls"))
        {
            ProbeSynchronousTls(loggerFactory);
            return;
        }

        using var server = new GarnetServer(Options(".round7-probes/framing"), loggerFactory, servers: []);
        server.Start();
        var provider = Provider(server);
        using var victim = new ProbeConnection(provider, "victim");
        using var observer = new ProbeConnection(provider, "observer");

        Console.WriteLine("== Payload of failing command ==");
        victim.Send("LPUSH", "disabled-list", Encoding.UTF8.GetString(Command("INCR", "inside-failure")));
        observer.Send("GET", "inside-failure");
        victim.Send("PING");
        observer.Send("GET", "inside-failure");

        Console.WriteLine("== Complete pipeline behind failure ==");
        victim.Push(Command("LPUSH", "disabled-list", "v").Concat(Command("INCR", "behind-failure")).ToArray());
        observer.Send("GET", "behind-failure");
        victim.Send("PING");
        observer.Send("GET", "behind-failure");
    }

    static void ProbeLua(ILoggerFactory loggerFactory)
    {
        foreach (var noObjects in new[] { false, true })
        {
            using var server = new GarnetServer(Options($".round7-probes/lua-{noObjects}", lua: true, noObjects: noObjects), loggerFactory, servers: []);
            server.Start();
            using var victim = new ProbeConnection(Provider(server), "lua-victim");
            var driver = Member(Member(victim.Session, "storageSession"), "stateMachineDriver");
            void Counts() => Console.WriteLine($"Active transaction counters: [{string.Join(", ", (long[])Member(driver, "NumActiveTransactions"))}]");
            Counts();
            Console.WriteLine($"== Transaction-mode Lua, noObjects={noObjects} ==");
            var script = noObjects ? "return redis.pcall('LPUSH', KEYS[1], 'v')" : "return redis.pcall('EXEC')";
            victim.Send("EVAL", script, "1", "lua-key");
            Counts();
            victim.Send("PING");
        }
    }

    static void ProbeAppendFailure(ILoggerFactory loggerFactory)
    {
        const string Path = ".round7-probes/append-failure";
        if (Directory.Exists(Path))
            Directory.Delete(Path, recursive: true);
        using var server = new GarnetServer(Options(Path, aof: true), loggerFactory, servers: []);
        server.Start();
        var provider = Provider(server);
        using var victim = new ProbeConnection(provider, "append-victim");
        using var observer = new ProbeConnection(provider, "append-observer");
        victim.Send("SET", "append-key", "before");
        victim.Send("MULTI");
        victim.Send("SET", "append-key", "after");
        var transaction = (TransactionManager)Member(victim.Session, "txnManager");
        typeof(TransactionManager).GetMethod("Run", Flags).Invoke(transaction, [false, false, TimeSpan.Zero]);
        var wrapper = (StoreWrapper)Member(provider, "storeWrapper");
        var aof = (GarnetAppendOnlyFile)Member(wrapper, "appendOnlyFile");
        var commitNumber = Member(aof.Log.SingleLog, "commitNum");
        SetField(aof.Log.SingleLog, "commitNum", long.MaxValue);
        var api = (IGarnetApi)Member(victim.Session, "transactionalGarnetApi");
        var key = GC.AllocateUninitializedArray<byte>(10, pinned: true);
        Encoding.ASCII.GetBytes("append-key").CopyTo(key, 0);
        try
        {
            api.SET(PinnedSpanByte.FromPinnedSpan(key), Encoding.ASCII.GetBytes("after").AsMemory());
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SET threw after store work: {ex.GetType().Name}: {ex.Message}");
        }
        try
        {
            typeof(TransactionManager).GetMethod("Abandon", Flags).Invoke(transaction, null);
        }
        catch (TargetInvocationException ex)
        {
            Console.WriteLine($"Abandon threw: {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");
        }
        var storage = Member(victim.Session, "storageSession");
        var driver = Member(storage, "stateMachineDriver");
        Console.WriteLine($"After Abandon: state={transaction.state}; active=[{string.Join(", ", (long[])Member(driver, "NumActiveTransactions"))}]");
        typeof(TransactionManager).GetMethod("Reset", Flags, null, [typeof(bool)], null).Invoke(transaction, [true]);
        SetField(aof.Log.SingleLog, "commitNum", commitNumber);
        observer.Send("GET", "append-key");
        Console.WriteLine($"After manual test cleanup: state={transaction.state}; active=[{string.Join(", ", (long[])Member(driver, "NumActiveTransactions"))}]");
    }

    static void ProbeSynchronousTls(ILoggerFactory loggerFactory)
    {
        using var server = new GarnetServer(Options(".round7-probes/sync-tls"), loggerFactory, servers: []);
        server.Start();
        using var sender = new ProbeSender();
        var session = Provider(server).GetSession(WireFormat.ASCII, sender);
        using var pool = new LimitedFixedBufferPool(4096);
        var settings = new NetworkBufferSettings(sendBufferSize: 4096, initialReceiveBufferSize: 4096, maxReceiveBufferSize: 4096);
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        var hook = new ProbeHook();
        using var handler = new ProbeTlsHandler(hook, sender, socket, settings, pool, session);
        using var originalTls = (SslStream)Member(handler, "sslStream");
        SetField(handler, "sslStream", new SynchronousSslStream(Command("INCR", "sync-witness").Concat(Command("DEBUG", "BLOCK", "30")).ToArray()));
        SetField(handler, "readerStatus", Enum.Parse(Member(handler, "readerStatus").GetType(), "Rest"));
        using var e = new SocketAsyncEventArgs { AcceptSocket = socket };
        typeof(SocketAsyncEventArgs).GetFields(Flags).Single(field => field.Name.Contains("bytesTransferred", StringComparison.OrdinalIgnoreCase)).SetValue(e, 1);
        var counter = typeof(INetworkSender).Assembly.GetType("Garnet.networking.ParkDiagnostics").GetField("ReceiveStateReclaimed", BindingFlags.Static | BindingFlags.NonPublic);
        var baseline = (int)counter.GetValue(null);
        using (ExceptionInjectionHelper.EnabledScope(ExceptionInjectionType.Session_Fail_Batch_Cleanup))
        {
            var loop = typeof(TcpNetworkHandlerBase<ProbeHook, ProbeSender>).GetMethod("ReceiveLoopWithTLSAsync", Flags);
            ((ValueTask<bool>)loop.Invoke(handler, [e, true])).AsTask().GetAwaiter().GetResult();
        }
        var disposed = false;
        try { e.SetBuffer(new byte[1], 0, 1); }
        catch (ObjectDisposedException) { disposed = true; }
        Console.WriteLine($"Synchronous TLS cleanup: consumerDisposed={hook.Disposals}; argsDisposed={disposed}; counterDelta={(int)counter.GetValue(null) - baseline}; poolLiveBytes={pool.LiveBytes}; rendezvous={Member(handler, "parkRendezvous")}");
    }

    static void ProbeAbort(ILoggerFactory loggerFactory)
    {
        const string Path = ".round7-probes/abort";
        if (Directory.Exists(Path))
            Directory.Delete(Path, recursive: true);
        using (var server = new GarnetServer(Options(Path, aof: true), loggerFactory, servers: []))
        {
            server.Start();
            var provider = Provider(server);
            using var first = new ProbeConnection(provider, "abort-victim");
            using var second = new ProbeConnection(provider, "abort-control");
            var wrapper = (StoreWrapper)Member(provider, "storeWrapper");
            var aof = (GarnetAppendOnlyFile)Member(wrapper, "appendOnlyFile");
            var storage = Member(first.Session, "storageSession");
            var sessionId = (int)Member(storage, "SessionID");
            var store = Member(wrapper, "store");
            var version = (long)Member(store, "CurrentVersion");
            var enqueue = typeof(GarnetLog).GetMethod("EnqueueTxn", Flags);
            Console.WriteLine($"readConsistencyManager null: {aof.readConsistencyManager == null}");
            enqueue.Invoke(aof.Log, [AofEntryType.TxnStart, version, sessionId, 0UL, null, 0]);
            first.Send("SET", "aborted", "value");
            enqueue.Invoke(aof.Log, [AofEntryType.TxnAbort, version, sessionId, 0UL, null, 0]);
            second.Send("SET", "after-abort", "retained");
            second.Send("COMMITAOF");
        }
        using var recovered = new GarnetServer(Options(Path, aof: true, recover: true), loggerFactory, servers: []);
        recovered.Start();
        using var observer = new ProbeConnection(Provider(recovered), "abort-recovered");
        observer.Send("GET", "after-abort");
    }
}

sealed class ProbeConnection : IDisposable
{
    readonly byte[] receive = GC.AllocateUninitializedArray<byte>(262144, pinned: true);
    readonly string name;
    int buffered;
    public IMessageConsumer Session { get; }
    public ProbeSender Sender { get; } = new();

    public ProbeConnection(GarnetProvider provider, string name)
    {
        this.name = name;
        Session = provider.GetSession(WireFormat.ASCII, Sender);
    }

    public void Send(params string[] args) => Push(Program.Command(args));

    public unsafe void Push(byte[] input)
    {
        input.CopyTo(receive.AsSpan(buffered));
        buffered += input.Length;
        ProbeLoggerFactory.Current = Session;
        var consumed = Session.TryConsumeMessages((byte*)Unsafe.AsPointer(ref receive[0]), buffered);
        ProbeLoggerFactory.Current = null;
        receive.AsSpan(consumed, buffered - consumed).CopyTo(receive);
        buffered -= consumed;
        Console.WriteLine($"{name}: consumed={consumed}; retained={buffered}; closed={Sender.Closed}; reply={Sender.TakeReply().Replace("\r\n", "|")}");
    }

    public void Dispose()
    {
        Session.Dispose();
        Sender.Dispose();
    }
}

sealed unsafe class ProbeSender : INetworkSender
{
    readonly byte[] buffer = GC.AllocateUninitializedArray<byte>(65536, pinned: true);
    readonly MemoryStream replies = new();
    readonly object sync = new();
    public bool Closed { get; private set; }
    public MaxSizeSettings GetMaxSizeSettings { get; } = new();
    public string RemoteEndpointName => "probe";
    public string LocalEndpointName => "probe";
    public bool IsLocalConnection() => true;
    public void Enter() => Monitor.Enter(sync);
    public void Exit() => Monitor.Exit(sync);
    public void EnterAndGetResponseObject(out byte* head, out byte* tail)
    {
        Enter();
        head = GetResponseObjectHead();
        tail = GetResponseObjectTail();
    }
    public void ExitAndReturnResponseObject() => Exit();
    public void GetResponseObject() { }
    public void ReturnResponseObject() { }
    public byte* GetResponseObjectHead() => (byte*)Unsafe.AsPointer(ref buffer[0]);
    public byte* GetResponseObjectTail() => GetResponseObjectHead() + buffer.Length;
    public bool SendResponse(int offset, int size)
    {
        replies.Write(buffer.AsSpan(offset, size));
        return true;
    }
    public void SendResponse(byte[] value, int offset, int count, object context) => replies.Write(value, offset, count);
    public void SendCallback(object context) { }
    public void DisposeNetworkSender(bool waitForSendCompletion) => Closed = true;
    public void Throttle() { }
    public bool TryClose() { Closed = true; return true; }
    public void Dispose() => Closed = true;
    public string TakeReply()
    {
        var result = Encoding.UTF8.GetString(replies.ToArray());
        replies.SetLength(0);
        return result;
    }
}

sealed class ProbeLoggerFactory : ILoggerFactory
{
    public static IMessageConsumer Current;
    public ILogger CreateLogger(string name) => new ProbeLogger();
    public void AddProvider(ILoggerProvider provider) { }
    public void Dispose() { }

    sealed class ProbeLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel level) => level >= LogLevel.Warning;
        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception ex, Func<TState, Exception, string> formatter)
        {
            if (level >= LogLevel.Warning)
            {
                Console.WriteLine($"LOG: {formatter(state, ex)}");
                if (ex != null)
                    Console.WriteLine(ex.ToString());
            }

            if (ex?.Message == "Object store is disabled" && Current != null)
                Console.WriteLine($"Outer state at catch: readHead={Program.Member(Current, "readHead")}; endReadHead={Program.Member(Current, "endReadHead")}; bytesRead={Program.Member(Current, "bytesRead")}");
        }
    }
}

sealed class ProbeHook : IServerHook
{
    public int Disposals;
    public bool Disposed => false;
    public bool TryCreateMessageConsumer(Span<byte> input, INetworkSender sender, out IMessageConsumer session)
    {
        session = null;
        return false;
    }
    public void DisposeMessageConsumer(INetworkHandler handler)
    {
        handler.Session.Dispose();
        Disposals++;
    }
}

sealed class ProbeTlsHandler(ProbeHook hook, ProbeSender sender, Socket socket, NetworkBufferSettings settings, LimitedFixedBufferPool pool, IMessageConsumer session)
    : TcpNetworkHandlerBase<ProbeHook, ProbeSender>(hook, sender, socket, settings, pool, useTLS: true, messageConsumer: session)
{
}

sealed class SynchronousSslStream(byte[] input) : SslStream(new MemoryStream())
{
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        input.AsMemory().CopyTo(buffer);
        return ValueTask.FromResult(input.Length);
    }
}
