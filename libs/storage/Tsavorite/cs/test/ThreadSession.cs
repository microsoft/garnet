// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.statemachine
{
    internal static class Extension
    {
        public static ThreadSession<V, I, O, C, F, SF, A> CreateThreadSession<V, I, O, C, F, SF, A>(this TsavoriteKV<V, SF, A> store, F f)
            where V : new()
            where F : ISessionFunctions<V, I, O, C>
            where SF : IStoreFunctions<V>
            where A : IAllocator<V, SF>
            => new(store, f);

        public static LUCThreadSession<V, I, O, C, F, SF, A> CreateLUCThreadSession<V, I, O, C, F, SF, A>(this TsavoriteKV<V, SF, A> store, F f)
            where V : new()
            where F : ISessionFunctions<V, I, O, C>
            where SF : IStoreFunctions<V>
            where A : IAllocator<V, SF>
            => new(store, f);
    }

    internal class ThreadSession<V, I, O, C, F, SF, A>
        where V : new()
        where F : ISessionFunctions<V, I, O, C>
        where SF : IStoreFunctions<V>
        where A : IAllocator<V, SF>
    {
        readonly TsavoriteKV<V, SF, A> store;
        ClientSession<V, I, O, C, F, SF, A> s2;
        UnsafeContext<V, I, O, C, F, SF, A> uc2;
        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();

        public ThreadSession(TsavoriteKV<V, SF, A> store, F f)
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

        public void Dispose()        {
            OtherSession("dispose");
        }

        private void SecondSession()
        {
            s2 = store.NewSession<I, O, C, F>(f);
            uc2 = s2.UnsafeContext;
            uc2.BeginUnsafe();

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
                        uc2.EndUnsafe();
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

    internal class LUCThreadSession<V, I, O, C, F, SF, A>
        where V : new()
        where F : ISessionFunctions<V, I, O, C>
        where SF : IStoreFunctions<V>
        where A : IAllocator<V, SF>
    {
        readonly TsavoriteKV<V, SF, A> store;
        ClientSession<V, I, O, C, F, SF, A> session;
        TransactionalUnsafeContext<V, I, O, C, F, SF, A> luc;
        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();
        public bool isProtected = false;

        public LUCThreadSession(TsavoriteKV<V, SF, A> store, F f)
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
            session = store.NewSession<I, O, C, F>(f);
            _ = ev.Set();

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
                            luc.EndUnsafe();
                        }
                        session.Dispose();
                        _ = ev.Set();
                        return;
                    case "getLUC":
                        luc = session.TransactionalUnsafeContext;
                        if (session.IsInPreparePhase())
                        {
                            isProtected = false;
                        }
                        else
                        {
                            luc.BeginUnsafe();
                            luc.BeginTransaction();
                            isProtected = true;
                        }
                        _ = ev.Set();
                        break;
                    case "DisposeLUC":
                        luc.EndTransaction();
                        luc.EndUnsafe();
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