// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using Tsavorite.core;

namespace ReplicaTransfer.benchmark;

/// <summary>
/// Shared startup device settings for owned servers and component devices.
/// These are configuration inputs, not measurements of kernel-normalized ring sizes.
/// </summary>
/// <param name="Type">Real local storage device implementation; Linux full runs require Native.</param>
/// <param name="Backend">Linux Native backend; omission selects Uring, while explicit Default selects Libaio.</param>
/// <param name="CompletionThreads">Completion drainers per Native device, not a process-wide pool.</param>
/// <param name="IoContexts">Kernel contexts/rings per Native device; zero uses backend defaults.</param>
/// <param name="QueueDepth">Submission entries per ring; zero uses device defaults and Uring rounds upward.</param>
/// <param name="ThrottleLimit">Per-device in-flight software bound; zero keeps its built-in default.</param>
/// <param name="AioMaxDevices">
/// Process-wide policy for automatic Libaio reservations against the machine's AIO budget.
/// Ignored by Uring and by Libaio automatic sizing when an explicit queue depth is selected.
/// </param>
internal sealed record DeviceSettings(DeviceType Type, NativeStorageDevice.IoBackend Backend,
    int CompletionThreads, int IoContexts, int QueueDepth, int ThrottleLimit, int AioMaxDevices)
{
    internal static readonly string[] Switches = ["--device-type", "--device-io-backend", "--device-completion-threads",
        "--device-io-contexts", "--device-queue-depth", "--device-throttle-limit", "--device-aio-max-devices"];

    /// <summary>Resolves profile defaults and rejects contradictory aliases or ignored native tuning.</summary>
    /// <param name="values">Already checked option/value pairs, including the component's legacy device alias.</param>
    /// <param name="full">Select the real-server Linux profile rather than component defaults.</param>
    /// <param name="linux">Whether to apply Linux Native requirements and tuning support.</param>
    internal static DeviceSettings Parse(Dictionary<string, string> values, bool full, bool linux)
    {
        if (values.ContainsKey("--device") && values.ContainsKey("--device-type"))
            throw new ArgumentException("Specify only one of --device and --device-type.");
        var typeText = values.GetValueOrDefault("--device-type",
            values.GetValueOrDefault("--device", full && linux ? "Native" : "RandomAccess"));
        if (!Enum.TryParse<DeviceType>(typeText, true, out var type) ||
            type is not (DeviceType.RandomAccess or DeviceType.Native or DeviceType.FileStream))
            throw new ArgumentException("--device-type must be RandomAccess, Native, or FileStream.");
        if (full && linux && type != DeviceType.Native)
            throw new ArgumentException("Linux full replication requires --device-type Native.");
        var profile = full && linux;
        if (!Enum.TryParse<NativeStorageDevice.IoBackend>(
            values.GetValueOrDefault("--device-io-backend", linux && type == DeviceType.Native ? "Uring" : "Default"), true, out var backend) ||
            !Enum.IsDefined(backend))
            throw new ArgumentException("--device-io-backend must be Default, Libaio, or Uring.");

        // Do not label a managed/Windows run with native tuning that the physical consumer would ignore.
        var nativeKnobs = Switches.Where(name => name is not ("--device-type" or "--device-throttle-limit"));
        if (nativeKnobs.Any(values.ContainsKey) && (!linux || type != DeviceType.Native))
            throw new ArgumentException("Native I/O tuning requires Linux and --device-type Native.");
        int Number(string name, int fallback, int min, int max)
        {
            var text = values.GetValueOrDefault(name, fallback.ToString(CultureInfo.InvariantCulture));
            if (text.Length == 0 || !char.IsAsciiDigit(text[0]) || !char.IsAsciiDigit(text[^1]) ||
                !int.TryParse(text.Replace("_", ""), NumberStyles.None, CultureInfo.InvariantCulture, out var number) ||
                number < min || number > max)
                throw new ArgumentOutOfRangeException(name, $"Expected {min} through {max}.");
            return number;
        }

        // Match the existing host option bounds; zero retains the device-owned default where supported.
        return new(type, backend, Number("--device-completion-threads", profile ? 16 : 4, 1, 64),
            Number("--device-io-contexts", profile ? 256 : 0, 0, 4096),
            Number("--device-queue-depth", profile ? 256 : 0, 0, 32768),
            Number("--device-throttle-limit", profile ? 4096 : 0, 0, 65536),
            Number("--device-aio-max-devices", 32, 1, 4096));
    }

    /// <summary>Enumerates exact server startup inputs; Linux-only tuning is omitted on other platforms.</summary>
    /// <param name="linux">Platform of the owned server, which runs locally beside its runner.</param>
    internal IEnumerable<(string Name, string Value)> ServerArguments(bool linux)
    {
        yield return ("--device-type", Type.ToString());
        yield return ("--device-throttle-limit", ThrottleLimit.ToString(CultureInfo.InvariantCulture));
        if (!linux || Type != DeviceType.Native)
            yield break;
        yield return ("--device-io-backend", Backend.ToString());
        yield return ("--device-completion-threads", CompletionThreads.ToString(CultureInfo.InvariantCulture));
        yield return ("--device-io-contexts", IoContexts.ToString(CultureInfo.InvariantCulture));
        yield return ("--device-queue-depth", QueueDepth.ToString(CultureInfo.InvariantCulture));
        yield return ("--device-aio-max-devices", AioMaxDevices.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>Forwards the required profile without substituting an older build's implicit defaults.</summary>
    /// <param name="arguments">Owned process argument list to append option/value pairs to.</param>
    /// <param name="supported">Switch names without leading dashes, obtained from that build's help process.</param>
    /// <param name="linux">Platform used when selecting applicable startup options.</param>
    internal void AddServerArguments(ICollection<string> arguments, HashSet<string> supported, bool linux)
    {
        foreach (var (name, value) in ServerArguments(linux))
        {
            if (!supported.Contains(name[2..]))
                throw new ArgumentException($"Selected server build does not support required {name} {value}.");
            arguments.Add(name);
            arguments.Add(value);
        }
    }

    /// <summary>Formats forwarded settings for result/control identity, not effective kernel telemetry.</summary>
    /// <param name="linux">Platform used to include only applicable settings.</param>
    internal string Identity(bool linux)
        => string.Join(';', ServerArguments(linux).Select(pair => $"{pair.Name[2..]}={pair.Value}"));

    /// <summary>Creates a per-factory/device configuration snapshot for native ring initialization.</summary>
    internal NativeDeviceOptions NativeOptions => new()
    {
        IoBackend = Backend,
        NumIoContexts = IoContexts,
        QueueDepth = QueueDepth
    };

    /// <summary>Configures component checkpoint devices with the same settings as the generated log.</summary>
    /// <param name="buffered">Allow OS file buffering; false requests unbuffered device I/O.</param>
    internal LocalStorageNamedDeviceFactoryCreator CreateFactory(bool buffered)
        => new(deviceType: Type, numCompletionThreads: CompletionThreads,
            throttleLimit: ThrottleLimit > 0 ? ThrottleLimit : null,
            disableFileBuffering: !buffered, nativeDeviceOptions: NativeOptions);

    /// <summary>
    /// Creates a caller-owned component device and applies throttle before its first I/O.
    /// Native ring handles/drainers are initialized lazily by the device, not by the parser.
    /// </summary>
    /// <param name="prefix">Segment filename prefix, without the device's numeric segment suffix.</param>
    /// <param name="buffered">Allow OS buffering rather than request unbuffered I/O.</param>
    /// <param name="readOnly">Protect an immutable source; Linux Native does not implement this mode.</param>
    internal IDevice CreateDevice(string prefix, bool buffered, bool readOnly = false)
    {
        if (Type == DeviceType.Native && OperatingSystem.IsLinux())
        {
            if (readOnly)
                throw new ArgumentException("Linux Native component source devices do not support read-only opens; use a managed component source or the full scenario.");

            // Publish the component process's Libaio budget policy before any native handles open.
            NativeStorageDevice.AioMaxDevices = AioMaxDevices;
        }
        var device = Devices.CreateLogDevice(prefix, deviceType: Type, deleteOnClose: false,
            disableFileBuffering: !buffered, readOnly: readOnly, numCompletionThreads: CompletionThreads,
            nativeDeviceOptions: NativeOptions);
        if (ThrottleLimit > 0)
            device.ThrottleLimit = ThrottleLimit;
        return device;
    }
}