// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace ReadFileServer
{
    using System;
    using System.Runtime.InteropServices;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Interop with WINAPI for file I/O
    /// </summary>
    public static unsafe class Native32
    {
        internal const int ERROR_IO_PENDING = 997;
        internal const int ERROR_PATH_NOT_FOUND = 3;
        internal const int WIN32_MAX_PATH = 260;
        internal const uint GENERIC_READ = 0x80000000;
        internal const uint GENERIC_WRITE = 0x40000000;
        internal const uint FILE_FLAG_DELETE_ON_CLOSE = 0x04000000;
        internal const uint FILE_FLAG_NO_BUFFERING = 0x20000000;
        internal const uint FILE_FLAG_OVERLAPPED = 0x40000000;
        internal const uint FILE_SHARE_DELETE = 0x00000004;

        [DllImport("Kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern SafeFileHandle CreateFileW(
            [In] string lpFileName,
            [In] UInt32 dwDesiredAccess,
            [In] UInt32 dwShareMode,
            [In] IntPtr lpSecurityAttributes,
            [In] UInt32 dwCreationDisposition,
            [In] UInt32 dwFlagsAndAttributes,
            [In] IntPtr hTemplateFile);

        [DllImport("Kernel32.dll", SetLastError = true)]
        internal static extern bool ReadFile(
            [In] SafeFileHandle hFile,
            [Out] IntPtr lpBuffer,
            [In] UInt32 nNumberOfBytesToRead,
            [Out] out UInt32 lpNumberOfBytesRead,
            [In] System.Threading.NativeOverlapped* lpOverlapped);

        [DllImport("Kernel32.dll", SetLastError = true)]
        internal static extern bool WriteFile(
            [In] SafeFileHandle hFile,
            [In] IntPtr lpBuffer,
            [In] UInt32 nNumberOfBytesToWrite,
            [Out] out UInt32 lpNumberOfBytesWritten,
            [In] System.Threading.NativeOverlapped* lpOverlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool GetFileSizeEx(
            [In] SafeFileHandle hFile,
            [Out] out long lpFileSize);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern bool GetDiskFreeSpace(string lpRootPathName,
           out uint lpSectorsPerCluster,
           out uint lpBytesPerSector,
           out uint lpNumberOfFreeClusters,
           out uint lpTotalNumberOfClusters);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool DeleteFileW([MarshalAs(UnmanagedType.LPWStr)] string lpFileName);

        internal static int MakeHRFromErrorCode(int errorCode)
        {
            return unchecked(((int)0x80070000) | errorCode);
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern IntPtr CreateIoCompletionPort(
            [In] SafeFileHandle hFile,
            IntPtr ExistingCompletionPort,
            UIntPtr CompletionKey,
            uint NumberOfConcurrentThreads);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool GetQueuedCompletionStatus(
            [In] IntPtr hCompletionPort,
            [Out] out UInt32 lpNumberOfBytesWritten,
            [Out] out IntPtr lpCompletionKey,
            [Out] out NativeOverlapped* lpOverlapped,
            [In] UInt32 dwMilliseconds);
    }
}