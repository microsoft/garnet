using System;
using System.Collections.Generic;
using System.Diagnostics;

const int WarmupIterations = 5;
const int MeasureIterations = 15;

var scenarios = new[]
{
	new Scenario("Typical", 5000, 32, 32),
	new Scenario("Stress", 20000, 48, 8),
};

Console.WriteLine("Txn key-copy perf probe (old vs new)");
Console.WriteLine();

foreach (var scenario in scenarios)
{
	var oldStats = Measure(scenario, RunOld);
	var newStats = Measure(scenario, RunNew);

	var speedup = oldStats.MeanMs / newStats.MeanMs;
	var allocReduction = 100.0 * (oldStats.MeanAllocBytes - newStats.MeanAllocBytes) / oldStats.MeanAllocBytes;

	Console.WriteLine($"Scenario: {scenario.Name}");
	Console.WriteLine($"  Old  : {oldStats.MeanMs,8:F2} ms, alloc {oldStats.MeanAllocBytes / (1024.0 * 1024.0),8:F2} MB");
	Console.WriteLine($"  New  : {newStats.MeanMs,8:F2} ms, alloc {newStats.MeanAllocBytes / (1024.0 * 1024.0),8:F2} MB");
	Console.WriteLine($"  Delta: {speedup,8:F2}x faster, alloc {allocReduction,8:F2}% lower");
	Console.WriteLine();
}

static Stats Measure(Scenario scenario, Action<Scenario> run)
{
	for (var i = 0; i < WarmupIterations; i++)
		run(scenario);

	var totalMs = 0.0;
	var totalAlloc = 0L;

	for (var i = 0; i < MeasureIterations; i++)
	{
		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();

		var allocStart = GC.GetAllocatedBytesForCurrentThread();
		var sw = Stopwatch.StartNew();
		run(scenario);
		sw.Stop();
		var allocEnd = GC.GetAllocatedBytesForCurrentThread();

		totalMs += sw.Elapsed.TotalMilliseconds;
		totalAlloc += allocEnd - allocStart;
	}

	return new Stats(totalMs / MeasureIterations, totalAlloc / MeasureIterations);
}

static void RunOld(Scenario s)
{
	var txnKeys = new List<byte[]>(s.TotalKeys);
	var scratch = new List<byte[]>(s.TotalKeys * 4);

	for (var i = 0; i < s.TotalKeys; i++)
	{
		if (i > 0 && i % s.BufferSwitchEvery == 0)
		{
			// Old behavior: copy all tracked keys each time receive buffer changes.
			for (var k = 0; k < txnKeys.Count; k++)
			{
				var src = txnKeys[k];
				var dst = new byte[src.Length];
				Buffer.BlockCopy(src, 0, dst, 0, src.Length);
				scratch.Add(dst);
				txnKeys[k] = dst;
			}
		}

		var key = new byte[s.KeySize];
		key[0] = (byte)i;
		txnKeys.Add(key);
	}
}

static void RunNew(Scenario s)
{
	var txnKeys = new List<byte[]>(s.TotalKeys);
	var scratch = new List<byte[]>(s.TotalKeys * 2);
	var firstKeyInCurrentBuffer = 0;

	for (var i = 0; i < s.TotalKeys; i++)
	{
		if (i > 0 && i % s.BufferSwitchEvery == 0)
		{
			// New behavior: copy only keys from the previous receive buffer segment.
			for (var k = firstKeyInCurrentBuffer; k < txnKeys.Count; k++)
			{
				var src = txnKeys[k];
				var dst = new byte[src.Length];
				Buffer.BlockCopy(src, 0, dst, 0, src.Length);
				scratch.Add(dst);
				txnKeys[k] = dst;
			}

			firstKeyInCurrentBuffer = txnKeys.Count;
		}

		var key = new byte[s.KeySize];
		key[0] = (byte)i;
		txnKeys.Add(key);
	}
}

readonly record struct Scenario(string Name, int TotalKeys, int KeySize, int BufferSwitchEvery);
readonly record struct Stats(double MeanMs, long MeanAllocBytes);
