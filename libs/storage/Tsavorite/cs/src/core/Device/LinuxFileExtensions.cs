// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Tsavorite.core
{
    /// <summary>
    /// Helpers for opening files on Linux with flags that .NET FileOptions cannot express, namely O_DIRECT (which bypasses the
    /// kernel page cache, like Windows FILE_FLAG_NO_BUFFERING). Returns a <see cref="SafeFileHandle"/> that can be used directly
    /// with <c>System.IO.RandomAccess.*</c> APIs or wrapped in a <see cref="FileStream"/>.
    /// </summary>
    /// <remarks>
    /// .NET <see cref="FileOptions"/> on Linux silently strips bits that aren't part of the known enum (including a casted
    /// O_DIRECT), so a <see cref="FileStream"/> opened with <c>FileOptions.WriteThrough | (FileOptions)0x20000000</c> ends up
    /// with neither O_DIRECT nor O_DSYNC set. This helper opens via libc <c>open(2)</c> and wraps the FD into a
    /// <see cref="SafeFileHandle"/>, letting Tsavorite's managed devices behave like the native libaio device with respect to
    /// the page cache. Buffers, lengths and file offsets used by the device path are already sector-aligned via
    /// <see cref="SectorAlignedMemory"/> and the allocator's page allocation.
    /// </remarks>
    [SupportedOSPlatform("linux")]
    internal static class LinuxFileExtensions
    {
        // libc open(2) flag values. These constants match Linux/x86_64 and Linux/arm64, which both use
        // asm-generic/fcntl.h. Other architectures (alpha, mips, parisc, powerpc, sparc) override some
        // of these. We probe at runtime via <see cref="IsDirectIOSupported"/> so unsupported architectures
        // fall back to the page-cache path automatically.
        private const int O_RDONLY = 0x0000;
        private const int O_WRONLY = 0x0001;
        private const int O_RDWR = 0x0002;
        private const int O_CREAT = 0x0040;
        private const int O_TRUNC = 0x0200;
        private const int O_DIRECT = 0x4000;
        private const int O_DSYNC = 0x1000;
        private const int O_CLOEXEC = 0x80000;
        // O_TMPFILE = __O_TMPFILE | O_DIRECTORY (Linux >= 3.11). Creating a file with this
        // bit set returns an anonymous inode in the *given directory's* filesystem that has
        // NO directory entry — invisible to readdir/getdents and so cannot be observed by
        // any concurrent listing. The inode is freed on the last close. Used by
        // <see cref="IsDirectIOSupported"/> to test O_DIRECT support without leaving
        // visible artifacts even if the test races with a directory enumeration.
        private const int O_TMPFILE = 0x410000;
        // 0644
        private const int DefaultMode = 0x1A4;

        private const int EINVAL = 22;
        private const int EACCES = 13;
        private const int EOPNOTSUPP = 95;
        private const int EISDIR = 21;

        [DllImport("libc", SetLastError = true, EntryPoint = "open")]
        private static extern int LibcOpen(IntPtr pathname, int flags, int mode);

        [DllImport("libc", SetLastError = true, EntryPoint = "close")]
        private static extern int LibcClose(int fd);

        /// <summary>
        /// Open <paramref name="path"/> on Linux with O_DIRECT. The returned <see cref="SafeFileHandle"/> can be passed to
        /// <c>System.IO.RandomAccess</c> or wrapped in a <see cref="FileStream"/> (with <c>bufferSize: 1</c> to avoid the
        /// FileStream's user-mode buffer double-buffering on top of O_DIRECT).
        /// </summary>
        /// <param name="path">File system path.</param>
        /// <param name="access">Access mode: Read, Write or ReadWrite.</param>
        /// <param name="createIfMissing">When true, opens with O_CREAT (mode 0644). When false, the file must already exist.</param>
        /// <param name="dsync">When true, adds O_DSYNC. Most O_DIRECT writes are already on
        /// the platter on completion so this is normally unnecessary; provided as an opt-in
        /// for callers that use <see cref="FileOptions.WriteThrough"/> on the managed file
        /// APIs and want the equivalent fsync-on-write semantics here.</param>
        /// <returns>An owning SafeFileHandle. Throws <see cref="IOException"/> if open fails.</returns>
        public static SafeFileHandle OpenDirect(string path, FileAccess access, bool createIfMissing, bool dsync = false)
        {
            int flags = access switch
            {
                FileAccess.Read => O_RDONLY,
                FileAccess.Write => O_WRONLY,
                _ => O_RDWR,
            };
            flags |= O_DIRECT | O_CLOEXEC;
            if (createIfMissing)
                flags |= O_CREAT;
            if (dsync)
                flags |= O_DSYNC;

            return OpenWithFlags(path, flags, DefaultMode);
        }

        /// <summary>
        /// Probes whether O_DIRECT is supported for files created in <paramref name="directoryPath"/>. Some filesystems
        /// (tmpfs, overlayfs without an O_DIRECT-capable lower layer, FUSE without the FUSE_DIRECT_IO capability) reject
        /// O_DIRECT with EINVAL on <c>open(2)</c>. Callers can use the result to decide between true O_DIRECT and a
        /// page-cache fallback for the entire device.
        /// </summary>
        public static bool IsDirectIOSupported(string directoryPath)
        {
            // Avoid creating a marker if the directory does not exist yet — caller will create it.
            if (string.IsNullOrEmpty(directoryPath))
                return false;
            try
            {
                if (!Directory.Exists(directoryPath))
                    return false;

                // Race-free probe: O_TMPFILE asks the kernel to create an anonymous inode
                // in the directory's filesystem with NO directory entry. The inode never
                // appears in readdir/getdents output, so no concurrent ListContents can ever
                // observe a probe file regardless of timing. The inode is freed on close.
                // O_TMPFILE | O_RDWR | O_DIRECT in one open(2) tells us atomically whether
                // the filesystem supports both anonymous-inode creation AND O_DIRECT-style
                // unbuffered IO on this mount.
                //
                // If O_TMPFILE itself is not supported (kernel < 3.11 or filesystem doesn't
                // implement it — open returns EOPNOTSUPP/EISDIR), we conservatively report
                // "no O_DIRECT" so the device falls back to the page-cache path. We
                // deliberately do NOT fall back to a named-file probe: a named-file probe
                // can leak its file into the directory on a race, which is exactly the bug
                // we're avoiding.
                using var handle = OpenWithFlagsOrNull(directoryPath, O_TMPFILE | O_RDWR | O_DIRECT | O_CLOEXEC, DefaultMode);
                return handle != null && !handle.IsInvalid;
            }
            catch
            {
                return false;
            }
        }

        private static SafeFileHandle OpenWithFlags(string path, int flags, int mode)
        {
            int fd = OpenSyscall(path, flags, mode);
            if (fd < 0)
            {
                int err = Marshal.GetLastWin32Error();
                throw new IOException($"Linux open(\"{path}\", flags=0x{flags:X}) failed: errno={err} ({GetErrnoName(err)})");
            }
            return new SafeFileHandle(new IntPtr(fd), ownsHandle: true);
        }

        private static SafeFileHandle OpenWithFlagsOrNull(string path, int flags, int mode)
        {
            int fd = OpenSyscall(path, flags, mode);
            if (fd < 0)
                return null;
            return new SafeFileHandle(new IntPtr(fd), ownsHandle: true);
        }

        private static unsafe int OpenSyscall(string path, int flags, int mode)
        {
            // open() expects a NUL-terminated UTF-8 byte string on Linux.
            var byteCount = Encoding.UTF8.GetByteCount(path);
            Span<byte> buffer = byteCount < 1024 ? stackalloc byte[byteCount + 1] : new byte[byteCount + 1];
            var written = Encoding.UTF8.GetBytes(path, buffer);
            buffer[written] = 0;
            fixed (byte* ptr = buffer)
            {
                return LibcOpen((IntPtr)ptr, flags, mode);
            }
        }

        private static string GetErrnoName(int err) => err switch
        {
            EINVAL => "EINVAL",
            EACCES => "EACCES",
            2 => "ENOENT",
            5 => "EIO",
            21 => "EISDIR",
            24 => "EMFILE",
            28 => "ENOSPC",
            _ => "errno",
        };
    }
}