# LightEpoch store-buffer litmus harness

Runs the real `LightEpoch` use-after-free race on hardware: a reader announces its
epoch and dereferences a page while a reclaimer retires that same page. Standalone
executable, `LightEpochLitmus`.

## Workload

A buffer pool is allocated in userspace up front and pages are handed out from it. A
page is never returned to the OS; "freeing" one stamps a poison sentinel over it. A
reader that sees poison in a page it was protecting is a use-after-free.

## Running it

```
dotnet run -f net10.0 --project playground/LightEpochLitmus -- --seconds 600 --json result.json
```

The control runs first and the harness refuses to continue unless the detector
reports it. `--help` lists every option. Exit codes: `0` pass, `1` violation, `2`
inconclusive, `3` unsupported host, `64` bad arguments. `0` versus `2` is the point —
an inconclusive run is not a pass.

In Docker, with the repository root as the build context:

```
docker build -f playground/LightEpochLitmus/Dockerfile -t garnet-lightepoch-litmus .
docker run --rm garnet-lightepoch-litmus --seconds 3600 --iterations 8 --json -
```

## Comparing against the unfixed algorithm

`helpers/BuggyLightEpoch.cs` is a frozen copy of `LightEpoch`.
`--buggy` runs against it, so both algorithms can be compared on the same machine in
the same session:

```
dotnet run -f net10.0 -c Release --project playground/LightEpochLitmus -- --buggy --seconds 30
dotnet run -f net10.0 -c Release --project playground/LightEpochLitmus -- --seconds 30
```

The first is expected to exit `1` with violations, the second `0`.

## Do not run this under emulation

QEMU — `--platform linux/arm64` on an x86 host, say — does not reproduce the emulated
architecture's memory ordering. The harness checks for emulation at startup and refuses
to run.
