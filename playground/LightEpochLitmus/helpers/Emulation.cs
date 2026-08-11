// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;

namespace Tsavorite.epoch.litmus
{
    /// <summary>
    /// Best-effort detection of running under an emulator, which interleaves guest instructions on
    /// the host's memory model, so the reorderings this harness hunts cannot occur. One-sided: a
    /// positive is reliable, a negative is not.
    /// </summary>
    internal static class Emulation
    {
        internal readonly struct Result
        {
            internal bool IsEmulated { get; init; }
            internal string Evidence { get; init; }
        }

        internal static Result Detect()
        {
            // A process architecture that differs from the OS architecture is emulation by
            // definition: x64 under Windows-on-ARM Prism, or an x64 container on Apple silicon.
            if (RuntimeInformation.ProcessArchitecture != RuntimeInformation.OSArchitecture)
                return Emulated($"process is {RuntimeInformation.ProcessArchitecture} on a {RuntimeInformation.OSArchitecture} OS");

            if (!OperatingSystem.IsLinux())
                return default;

            // qemu-user maps its own binary into the guest process, even though uname, /proc/cpuinfo
            // and the process architecture all report the emulated target faithfully.
            if (TryReadFile("/proc/self/maps", out var maps) && maps.Contains("qemu", StringComparison.OrdinalIgnoreCase))
                return Emulated("qemu is mapped into this process (/proc/self/maps)");

            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("QEMU_LD_PREFIX")))
                return Emulated("QEMU_LD_PREFIX is set");

            if (!TryReadFile("/proc/cpuinfo", out var cpuinfo))
                return default;

            if (cpuinfo.Contains("QEMU", StringComparison.Ordinal))
                return Emulated("/proc/cpuinfo reports a QEMU CPU");

            // The check that catches `docker run --platform linux/arm64` on an x86 host, where
            // qemu-user gives itself away nowhere else: every real implementer is registered and
            // non-zero (0x41 ARM, 0x50 Ampere, 0x51 Qualcomm, 0x61 Apple), QEMU synthesises 0x00.
            if (TryGetCpuImplementer(cpuinfo, out var implementer) && implementer == 0)
                return Emulated("/proc/cpuinfo reports CPU implementer 0x00, which no real part uses");

            return default;
        }

        static bool TryGetCpuImplementer(string cpuinfo, out int implementer)
        {
            implementer = -1;

            foreach (var line in cpuinfo.Split('\n'))
            {
                if (!line.StartsWith("CPU implementer", StringComparison.Ordinal))
                    continue;

                var colon = line.IndexOf(':');
                if (colon < 0)
                    continue;

                var value = line[(colon + 1)..].Trim();
                if (value.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
                    value = value[2..];

                return int.TryParse(value, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out implementer);
            }

            return false;
        }

        static Result Emulated(string evidence) => new() { IsEmulated = true, Evidence = evidence };

        static bool TryReadFile(string path, out string contents)
        {
            try
            {
                contents = File.ReadAllText(path);
                return true;
            }
            catch (Exception)
            {
                contents = null;
                return false;
            }
        }
    }
}