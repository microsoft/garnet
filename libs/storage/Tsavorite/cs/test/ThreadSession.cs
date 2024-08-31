// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.statemachine
{
    internal static class Extension
    {
        public static ThreadSession<K, V, I, O, C, F, SF, A> CreateThreadSession<K, V, I, O, C, F, SF, A>(this TsavoriteKV<K, V, SF, A> store, F f)
            where K : new()
            where V : new()
            where F : ISessionFunctions<K, V, I, O, C>
            where SF : IStoreFunctions<K, V>
            where A : IAllocator<K, V, SF>
            => new(store, f);

        public static LUCThreadSession<K, V, I, O, C, F, SF, A> CreateLUCThreadSession<K, V, I, O, C, F, SF, A>(this TsavoriteKV<K, V, SF, A> store, F f)
            where K : new()
            where V : new()
            where F : ISessionFunctions<K, V, I, O, C>
            where SF : IStoreFunctions<K, V>
            where A : IAllocator<K, V, SF>
            => new(store, f);
    }

    internal class ThreadSession<K, V, I, O, C, F, SF, A>
        where K : new()
        where V : new()
        where F : ISessionFunctions<K, V, I, O, C>
        where SF : IStoreFunctions<K, V>
        where A : IAllocator<K, V, SF>
    {
        readonly TsavoriteKV<K, V, SF, A> store;
        ClientSession<K, V, I, O, C, F, SF, A> s2;
        UnsafeContext<K, V, I, O, C, F, SF, A> uc2;

        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();

        public ThreadSession(TsavoriteKV<K, V, SF, A> store, F f)
        {
            this.store = store;
            this.f = f;
            var ss = new Thread(SecondSession);
            ss.Start();
            _ = ev.WaitOne();
        }

        public void Refresh(bool waitComplete = true)
        {
            OtherSession("refresh", waitComplete);
        }

        public void CompleteOp()
        {
            _ = ev.WaitOne();
        }

        public void Dispose()
        {
            OtherSession("dispose");
        }

        private void SecondSession()
        {
            s2 = store.NewSession<I, O, C, F>(f, null);
            uc2 = s2.UnsafeContext;
            TestTransientKernelSession<K, V, I, O, C, F, SF, A> kernelSession = new(s2);
            kernelSession.BeginUnsafe();

            _ = ev.Set();

            while (true)
            {
                var cmd = q.DequeueAsync().Result;
                switch (cmd)
                {
                    case "refresh":
                        uc2.Refresh();
                        _ = ev.Set();
                        break;
                    case "dispose":
                        kernelSession.EndUnsafe();
                        s2.Dispose();
                        _ = ev.Set();
                        return;
                    default:
                        throw new Exception("Unsupported command");
                }
            }
        }

        private void OtherSession(string command, bool waitComplete = true)
        {
            q.Enqueue(command);
            if (waitComplete)
                _ = ev.WaitOne();
        }
    }

    internal class LUCThreadSession<K, V, I, O, C, F, SF, A>
        where K : new()
        where V : new()
        where F : ISessionFunctions<K, V, I, O, C>
        where SF : IStoreFunctions<K, V>
        where A : IAllocator<K, V, SF>
    {
        readonly TsavoriteKV<K, V, SF, A> store;
        ClientSession<K, V, I, O, C, F, SF, A> session;
        LockableUnsafeContext<K, V, I, O, C, F, SF, A> luc;

        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();
        public bool isProtected = false;

        public LUCThreadSession(TsavoriteKV<K, V, SF, A> store, F f)
        {
            this.store = store;
            this.f = f;
            var ss = new Thread(LUCThread);
            ss.Start();
            _ = ev.WaitOne();
        }
        public void Refresh()
        {
            queue("refresh");
        }

        public void Dispose()
        {
            queue("dispose");
        }
        public void DisposeLUC()
        {
            queue("DisposeLUC");
        }

        public void getLUC()
        {
            queue("getLUC");
        }

        private void LUCThread()
        {
            session = store.NewSession<I, O, C, F>(f, null);
            _ = ev.Set();
            TestTransactionalKernelSession<K, V, I, O, C, F, SF, A> kernelSession = new(session);

            while (true)
            {
                var cmd = q.DequeueAsync().Result;
                switch (cmd)
                {
                    case "refresh":
                        if (isProtected)
                            luc.Refresh();
                        else
                            session.BasicContext.Refresh();
                        _ = ev.Set();
                        break;
                    case "dispose":
                        if (isProtected)
                        {
                            kernelSession.EndUnsafe();
                        }
                        session.Dispose();
                        _ = ev.Set();
                        return;
                    case "getLUC":
                        luc = session.LockableUnsafeContext;
                        if (session.IsInPreparePhase())
                        {
                            isProtected = false;
                        }
                        else
                        {
                            kernelSession.BeginUnsafe();
                            kernelSession.BeginTransaction();
                            isProtected = true;
                        }
                        _ = ev.Set();
                        break;
                    case "DisposeLUC":
                        kernelSession.EndTransaction();
                        kernelSession.EndUnsafe();
                        isProtected = false;
                        _ = ev.Set();
                        break;
                    default:
                        throw new Exception("Unsupported command");
                }
            }
        }
        private void queue(string command)
        {
            q.Enqueue(command);
            _ = ev.WaitOne();
        }
    }
}