// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    using System;
    using System.IO;
    using System.Numerics;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Interop with WINAPI for file I/O, threading, and NUMA functions.
    /// </summary>
    public static unsafe class Native32
    {
        #region Native structs
        [StructLayout(LayoutKind.Sequential)]
        private struct LUID
        {
            public uint lp;
            public int hp;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct LUID_AND_ATTRIBUTES
        {
            public LUID Luid;
            public uint Attributes;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct TOKEN_PRIVILEGES
        {
            public uint PrivilegeCount;
            public LUID_AND_ATTRIBUTES Privileges;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct MARK_HANDLE_INFO
        {
            public uint UsnSourceInfo;
            public IntPtr VolumeHandle;
            public uint HandleInfo;
        }

        internal enum FILE_INFO_BY_HANDLE_CLASS
        {
            FileBasicInfo = 0,
            FileStandardInfo = 1,
            FileNameInfo = 2,
            FileRenameInfo = 3,
            FileDispositionInfo = 4,
            FileAllocationInfo = 5,
            FileEndOfFileInfo = 6,
            FileStreamInfo = 7,
            FileCompressionInfo = 8,
            FileAttributeTagInfo = 9,
            FileIdBothDirectoryInfo = 10,// 0x0A
            FileIdBothDirectoryRestartInfo = 11, // 0xB
            FileIoPriorityHintInfo = 12, // 0xC
            FileRemoteProtocolInfo = 13, // 0xD
            FileFullDirectoryInfo = 14, // 0xE
            FileFullDirectoryRestartInfo = 15, // 0xF
            FileStorageInfo = 16, // 0x10
            FileAlignmentInfo = 17, // 0x11
            FileIdInfo = 18, // 0x12
            FileIdExtdDirectoryInfo = 19, // 0x13
            FileIdExtdDirectoryRestartInfo = 20, // 0x14
            MaximumFileInfoByHandlesClass
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct FILE_STORAGE_INFO
        {
            public uint LogicalBytesPerSector;
            public uint PhysicalBytesPerSectorForAtomicity;
            public uint PhysicalBytesPerSectorForPerformance;
            public uint FileSystemEffectivePhysicalBytesPerSectorForAtomicity;
            public uint Flags;
            public uint ByteOffsetForSectorAlignment;
            public uint ByteOffsetForPartitionAlignment;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct OVERLAPPED_ENTRY
        {
            public UIntPtr lpCompletionKey;
            public NativeOverlapped* lpOverlapped;
            public uint Internal; // This is the NTSTATUS code
            public UIntPtr dwNumberOfBytesTransferred;
        }

        #endregion

        #region io constants and flags
        internal const int ERROR_IO_PENDING = 997;
        internal const int ERROR_PATH_NOT_FOUND = 3;
        internal const int WIN32_MAX_PATH = 260;
        internal const uint GENERIC_READ = 0x80000000;
        internal const uint GENERIC_WRITE = 0x40000000;
        internal const uint FILE_FLAG_DELETE_ON_CLOSE = 0x04000000;
        internal const uint FILE_FLAG_NO_BUFFERING = 0x20000000;
        internal const uint FILE_FLAG_OVERLAPPED = 0x40000000;

        internal const uint FILE_SHARE_DELETE = 0x00000004;
        #endregion

        #region io functions

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
            [In] NativeOverlapped* lpOverlapped);

        [DllImport("Kernel32.dll", SetLastError = true)]
        internal static extern bool WriteFile(
            [In] SafeFileHandle hFile,
            [In] IntPtr lpBuffer,
            [In] UInt32 nNumberOfBytesToWrite,
            [Out] out UInt32 lpNumberOfBytesWritten,
            [In] NativeOverlapped* lpOverlapped);

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

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool GetQueuedCompletionStatusEx(
            [In] IntPtr hCompletionPort,
            [In] OVERLAPPED_ENTRY* lpCompletionPortEntries,
            [In] uint ulCount,
            [Out] out uint ulNumEntriesRemoved,
            [In] uint dwMilliseconds,
            [In] bool fAlertable);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool GetFileSizeEx(
            [In] SafeFileHandle hFile,
            [Out] out long lpFileSize);

        internal enum EMoveMethod : uint
        {
            Begin = 0,
            Current = 1,
            End = 2
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern uint SetFilePointer(
              [In] SafeFileHandle hFile,
              [In] int lDistanceToMove,
              [In, Out] ref int lpDistanceToMoveHigh,
              [In] EMoveMethod dwMoveMethod);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool SetEndOfFile(
            [In] SafeFileHandle hFile);


        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern bool GetDiskFreeSpace(string lpRootPathName,
           out uint lpSectorsPerCluster,
           out uint lpBytesPerSector,
           out uint lpNumberOfFreeClusters,
           out uint lpTotalNumberOfClusters);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern bool GetFileInformationByHandleEx([In] SafeFileHandle hFile, FILE_INFO_BY_HANDLE_CLASS infoClass, out FILE_STORAGE_INFO fileStorageInfo, uint dwBufferSize);


        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool DeleteFileW([MarshalAs(UnmanagedType.LPWStr)] string lpFileName);
        #endregion

        #region Thread and NUMA functions
        [DllImport("kernel32.dll")]
        private static extern IntPtr GetCurrentThread();
        [DllImport("kernel32")]
        internal static extern uint GetCurrentThreadId();
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint GetCurrentProcessorNumber();
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint GetActiveProcessorCount(uint count);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern ushort GetActiveProcessorGroupCount();
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern int SetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY GroupAffinity, ref GROUP_AFFINITY PreviousGroupAffinity);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern int GetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY PreviousGroupAffinity);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetLogicalProcessorInformation(IntPtr buffer, ref uint returnLength);

        private static readonly uint ALL_PROCESSOR_GROUPS = 0xffff;


        private enum LOGICAL_PROCESSOR_RELATIONSHIP
        {
            RelationProcessorCore = 0,
            RelationNumaNode = 1,
            RelationCache = 2,
            RelationProcessorPackage = 3,
            RelationGroup = 4,
            RelationAll = 0xffff
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct SYSTEM_LOGICAL_PROCESSOR_INFORMATION
        {
            public UIntPtr ProcessorMask;
            public LOGICAL_PROCESSOR_RELATIONSHIP Relationship;
            public ProcessorInfoUnion Info;
        }

        [StructLayout(LayoutKind.Explicit)]
        private struct ProcessorInfoUnion
        {
            [FieldOffset(0)] public byte ProcessorCoreFlags;
            [FieldOffset(0)] public uint NumaNodeNumber;
            [FieldOffset(0)] public CacheDescriptor Cache;
            [FieldOffset(0)] public ulong Reserved1;
            [FieldOffset(8)] public ulong Reserved2;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct CacheDescriptor
        {
            public byte Level;
            public byte Associativity;
            public ushort LineSize;
            public uint Size;
            public uint Type;
        }

        [StructLayoutAttribute(LayoutKind.Sequential)]
        private struct GROUP_AFFINITY
        {
            public ulong Mask;
            public uint Group;
            public uint Reserved1;
            public uint Reserved2;
            public uint Reserved3;
        }

        private static uint? cachedLogicalProcessorsPerCore = null;

        /// <summary>
        /// Gets the number of logical processors per physical core (e.g., 2 for hyperthreading).
        /// </summary>
        public static uint GetLogicalProcessorsPerCore()
        {
            if (cachedLogicalProcessorsPerCore.HasValue)
                return cachedLogicalProcessorsPerCore.Value;

            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                cachedLogicalProcessorsPerCore = 1;
                return 1;
            }

            uint returnLength = 0;
            GetLogicalProcessorInformation(IntPtr.Zero, ref returnLength);

            IntPtr buffer = Marshal.AllocHGlobal((int)returnLength);
            try
            {
                if (!GetLogicalProcessorInformation(buffer, ref returnLength))
                {
                    cachedLogicalProcessorsPerCore = 1;
                    return 1;
                }

                int structSize = Marshal.SizeOf<SYSTEM_LOGICAL_PROCESSOR_INFORMATION>();
                int count = (int)returnLength / structSize;

                for (int i = 0; i < count; i++)
                {
                    var info = Marshal.PtrToStructure<SYSTEM_LOGICAL_PROCESSOR_INFORMATION>(
                        IntPtr.Add(buffer, i * structSize));

                    if (info.Relationship == LOGICAL_PROCESSOR_RELATIONSHIP.RelationProcessorCore)
                    {
                        // Count bits set in ProcessorMask to get logical processors for this core
                        uint logicalPerCore = (uint)BitOperations.PopCount((ulong)info.ProcessorMask);
                        cachedLogicalProcessorsPerCore = logicalPerCore;
                        return logicalPerCore;
                    }
                }
            }
            finally
            {
                Marshal.FreeHGlobal(buffer);
            }

            cachedLogicalProcessorsPerCore = 1;
            return 1;
        }

        /// <summary>
        /// Accepts thread id = 0, 1, 2, ... and sprays them round-robin
        /// across all cores (viewed as a flat space). On NUMA machines,
        /// this gives us [socket, core] ordering of affinitization. That is, 
        /// if there are N cores per socket, then thread indices of 0 to N-1 map
        /// to the range [socket 0, core 0] to [socket 0, core N-1].
        /// </summary>
        /// <param name="threadIdx">Index of thread (from 0 onwards)</param>
        public static void AffinitizeThreadRoundRobin(uint threadIdx, bool skipHyperthreads = true)
        {
            uint nrOfProcessors = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            ushort nrOfProcessorGroups = GetActiveProcessorGroupCount();
            uint nrOfProcsPerGroup = nrOfProcessors / nrOfProcessorGroups;

            GROUP_AFFINITY groupAffinityThread = default(GROUP_AFFINITY);
            GROUP_AFFINITY oldAffinityThread = default(GROUP_AFFINITY);

            IntPtr thread = GetCurrentThread();
            GetThreadGroupAffinity(thread, ref groupAffinityThread);

            if (skipHyperthreads)
            {
                uint logicalPerCore = GetLogicalProcessorsPerCore();
                uint nrOfPhysicalCores = nrOfProcessors / logicalPerCore;
                threadIdx = (threadIdx % nrOfPhysicalCores) * logicalPerCore + ((threadIdx / nrOfPhysicalCores) % logicalPerCore);
            }
            else
            {
                threadIdx = threadIdx % nrOfProcessors;
            }

            groupAffinityThread.Mask = (ulong)1L << ((int)(threadIdx % (int)nrOfProcsPerGroup));
            groupAffinityThread.Group = (uint)(threadIdx / nrOfProcsPerGroup);

            if (SetThreadGroupAffinity(thread, ref groupAffinityThread, ref oldAffinityThread) == 0)
            {
                throw new TsavoriteException("Unable to affinitize thread");
            }
        }

        /// <summary>
        /// Get number of groups (sockets) and processors per group
        /// </summary>
        /// <returns></returns>
        public static (uint numGroups, uint numProcsPerGroup) GetNumGroupsProcsPerGroup()
        {
            uint nrOfProcessors = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            ushort nrOfProcessorGroups = GetActiveProcessorGroupCount();
            uint nrOfProcsPerGroup = nrOfProcessors / nrOfProcessorGroups;
            return (nrOfProcessorGroups, nrOfProcsPerGroup);
        }

        /// <summary>
        /// Accepts thread id = 0, 1, 2, ... and sprays them round-robin
        /// across all cores (viewed as a flat space). On NUMA machines,
        /// this gives us [core, socket] ordering of affinitization. That is, 
        /// if there are N cores per socket, then thread indices of 0 to N-1 map
        /// to the range [socket 0, core 0] to [socket N-1, core 0].
        /// </summary>
        /// <param name="threadIdx">Index of thread (from 0 onwards)</param>
        /// <param name="nrOfProcessorGroups">Number of NUMA sockets</param>
        public static void AffinitizeThreadShardedNuma(uint threadIdx, ushort nrOfProcessorGroups)
        {
            uint nrOfProcessors = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            uint nrOfProcsPerGroup = nrOfProcessors / nrOfProcessorGroups;

            threadIdx = nrOfProcsPerGroup * (threadIdx % nrOfProcessorGroups) + (threadIdx / nrOfProcessorGroups);
            AffinitizeThreadRoundRobin(threadIdx);
            return;
        }
        #endregion

        #region Advanced file ops
        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool LookupPrivilegeValue(string lpSystemName, string lpName, ref LUID lpLuid);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr GetCurrentProcess();

        [DllImport("advapi32", SetLastError = true)]
        private static extern bool OpenProcessToken(IntPtr ProcessHandle, uint DesiredAccess, out IntPtr TokenHandle);

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool AdjustTokenPrivileges(IntPtr tokenhandle, int disableprivs, ref TOKEN_PRIVILEGES Newstate, int BufferLengthInBytes, int PreviousState, int ReturnLengthInBytes);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr hObject);

        [DllImport("Kernel32.dll", SetLastError = true)]
        private static extern bool DeviceIoControl(SafeFileHandle hDevice, uint IoControlCode, void* InBuffer, int nInBufferSize, IntPtr OutBuffer, int nOutBufferSize, ref uint pBytesReturned, IntPtr Overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetFilePointerEx(SafeFileHandle hFile, long liDistanceToMove, out long lpNewFilePointer, uint dwMoveMethod);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetFileValidData(SafeFileHandle hFile, long ValidDataLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern SafeFileHandle CreateFile(string filename, uint access, uint share, IntPtr securityAttributes, uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);

        private static bool? processPrivilegeEnabled = null;

        /// <summary>
        /// Enable privilege for process
        /// </summary>
        /// <returns></returns>
        public static bool EnableProcessPrivileges()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return false;

            if (processPrivilegeEnabled.HasValue) return processPrivilegeEnabled.Value;

            TOKEN_PRIVILEGES token_privileges = default(TOKEN_PRIVILEGES);
            token_privileges.PrivilegeCount = 1;
            token_privileges.Privileges.Attributes = 0x2;

            if (!LookupPrivilegeValue(null, "SeManageVolumePrivilege",
                ref token_privileges.Privileges.Luid))
            {
                processPrivilegeEnabled = false;
                return false;
            }

            if (!OpenProcessToken(GetCurrentProcess(), 0x20, out IntPtr token))
            {
                processPrivilegeEnabled = false;
                return false;
            }
            if (!AdjustTokenPrivileges(token, 0, ref token_privileges, 0, 0, 0))
            {
                CloseHandle(token);
                processPrivilegeEnabled = false;
                return false;
            }
            if (Marshal.GetLastWin32Error() != 0)
            {
                CloseHandle(token);
                processPrivilegeEnabled = false;
                return false;
            }
            CloseHandle(token);
            processPrivilegeEnabled = true;
            return true;
        }

        private static uint CTL_CODE(uint DeviceType, uint Function, uint Method, uint Access)
        {
            return (((DeviceType) << 16) | ((Access) << 14) | ((Function) << 2) | (Method));
        }

        internal static bool EnableVolumePrivileges(string filename, SafeFileHandle handle)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return false;

            if (processPrivilegeEnabled == false)
                return false;

            string volume_string = "\\\\.\\" + filename.Substring(0, 2);

            uint fileCreation = unchecked((uint)FileMode.Open);

            SafeFileHandle volume_handle = CreateFile(volume_string, 0, 0, IntPtr.Zero, fileCreation,
                0x80, IntPtr.Zero);
            if (volume_handle == null)
            {
                return false;
            }

            MARK_HANDLE_INFO mhi;
            mhi.UsnSourceInfo = 0x1;
            mhi.VolumeHandle = volume_handle.DangerousGetHandle();
            mhi.HandleInfo = 0x1;

            uint bytes_returned = 0;
            bool result = DeviceIoControl(handle, CTL_CODE(0x9, 63, 0, 0),
                (void*)&mhi, sizeof(MARK_HANDLE_INFO), IntPtr.Zero,
                0, ref bytes_returned, IntPtr.Zero);

            volume_handle.Close();
            return result;
        }

        /// <summary>
        /// Set file size
        /// </summary>
        /// <param name="file_handle"></param>
        /// <param name="file_size"></param>
        /// <returns></returns>
        public static bool SetFileSize(SafeFileHandle file_handle, long file_size)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return false;

            if (!SetFilePointerEx(file_handle, file_size, out _, 0))
            {
                return false;
            }

            // Set a fixed file length
            if (!SetEndOfFile(file_handle))
            {
                return false;
            }

            if (!SetFileValidData(file_handle, file_size))
            {
                return false;
            }

            return true;
        }

        internal static int MakeHRFromErrorCode(int errorCode)
        {
            return unchecked(((int)0x80070000) | errorCode);
        }

        [DllImport("ntdll.dll")]
        internal static extern uint RtlNtStatusToDosError(uint NtStatus);
        #endregion
    }
}