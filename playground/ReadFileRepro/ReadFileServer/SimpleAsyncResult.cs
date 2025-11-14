// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace ReadFileServer
{
    sealed unsafe class SimpleAsyncResult : IAsyncResult
    {
        public DeviceIOCompletionCallback callback;
        public object context;
        public Overlapped overlapped;
        public NativeOverlapped* nativeOverlapped;

        public object AsyncState => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();

        public bool IsCompleted => throw new NotImplementedException();
    }
}