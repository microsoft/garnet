// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.statemachine
{
    internal static class Extension
    {
        public static ThreadSession<K, V, I, O, C, F> CreateThreadSession<K, V, I, O, C, F>(this TsavoriteKV<K, V> store, F f)
            where K : new() where V : new() where F : IFunctions<K, V, I, O, C>
            => new ThreadSession<K, V, I, O, C, F>(store, f);

        public static LUCThreadSession<K, V, I, O, C, F> CreateLUCThreadSession<K, V, I, O, C, F>(this TsavoriteKV<K, V> store, F f)
            where K : new() where V : new() where F : IFunctions<K, V, I, O, C>
            => new LUCThreadSession<K, V, I, O, C, F>(store, f);
    }

    internal class ThreadSession<K, V, I, O, C, F>
        where K : new() where V : new() where F : IFunctions<K, V, I, O, C>
    {
        readonly TsavoriteKV<K, V> store;
        ClientSession<K, V, I, O, C, F> s2;
        UnsafeContext<K, V, I, O, C, F> uc2;
        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();

        public ThreadSession(TsavoriteKV<K, V> store, F f)
        {
            this.store = store;
            this.f = f;
            var ss = new Thread(SecondSession);
            ss.Start();
            ev.WaitOne();
        }

        public void Refresh(bool waitComplete = true)
        {
            OtherSession("refresh", waitComplete);
        }

        public void CompleteOp()
        {
            ev.WaitOne();
        }

        public void Dispose()
        {
            OtherSession("dispose");
        }

        private void SecondSession()
        {
            s2 = store.NewSession<I, O, C, F>(f, null);
            uc2 = s2.UnsafeContext;
            uc2.BeginUnsafe();

            ev.Set();

            while (true)
            {
                var cmd = q.DequeueAsync().Result;
                switch (cmd)
                {
                    case "refresh":
                        uc2.Refresh();
                        ev.Set();
                        break;
                    case "dispose":
                        uc2.EndUnsafe();
                        s2.Dispose();
                        ev.Set();
                        return;
                    default:
                        throw new Exception("Unsupported command");
                }
            }
        }

        private void OtherSession(string command, bool waitComplete = true)
        {
            q.Enqueue(command);
            if (waitComplete) ev.WaitOne();
        }
    }

    internal class LUCThreadSession<K, V, I, O, C, F>
        where K : new() where V : new() where F : IFunctions<K, V, I, O, C>
    {
        readonly TsavoriteKV<K, V> store;
        ClientSession<K, V, I, O, C, F> session;
        LockableUnsafeContext<K, V, I, O, C, F> luc;
        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();
        public bool isProtected = false;

        public LUCThreadSession(TsavoriteKV<K, V> store, F f)
        {
            this.store = store;
            this.f = f;
            var ss = new Thread(LUCThread);
            ss.Start();
            ev.WaitOne();
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
            ev.Set();

            while (true)
            {
                var cmd = q.DequeueAsync().Result;
                switch (cmd)
                {
                    case "refresh":
                        if (isProtected)
                            luc.Refresh();
                        else
                            session.Refresh();
                        ev.Set();
                        break;
                    case "dispose":
                        if (isProtected)
                        {
                            luc.EndUnsafe();
                        }
                        session.Dispose();
                        ev.Set();
                        return;
                    case "getLUC":
                        luc = session.LockableUnsafeContext;
                        if (session.IsInPreparePhase())
                        {
                            isProtected = false;
                        }
                        else
                        {
                            luc.BeginUnsafe();
                            luc.BeginLockable();
                            isProtected = true;
                        }
                        ev.Set();
                        break;
                    case "DisposeLUC":
                        luc.EndLockable();
                        luc.EndUnsafe();
                        isProtected = false;
                        ev.Set();
                        break;
                    default:
                        throw new Exception("Unsupported command");
                }
            }
        }
        private void queue(string command)
        {
            q.Enqueue(command);
            ev.WaitOne();
        }
    }
}