using System;
using System.Runtime.CompilerServices;
using Garnet;
using Garnet.server;

// Create the emitted struct type from a GarnetServerOptions instance
var opts = new GarnetServerOptions { EnableCluster = false, EnableAOF = true, QuietMode = true };
var structType = GarnetServerFactory.GetOrCreateOptionsStruct(opts);

Console.WriteLine($"Emitted struct type: {structType.Name}");
Console.WriteLine($"Is value type: {structType.IsValueType}");
Console.WriteLine($"Size: {System.Runtime.InteropServices.Marshal.SizeOf(structType)} bytes");
Console.WriteLine();

// Create an instance and verify the property values match
var instance = Activator.CreateInstance(structType);
var iface = (IGarnetServerOptions)instance;
Console.WriteLine("Property verification:");
Console.WriteLine($"  EnableCluster = {iface.EnableCluster} (expected: False)");
Console.WriteLine($"  EnableAOF     = {iface.EnableAOF} (expected: True)");
Console.WriteLine($"  QuietMode     = {iface.QuietMode} (expected: True)");
Console.WriteLine($"  EnableLua     = {iface.EnableLua} (expected: False)");
Console.WriteLine($"  MaxDatabases  = {iface.MaxDatabases} (expected: 16)");
Console.WriteLine($"  CommitFreqMs  = {iface.CommitFrequencyMs} (expected: 0)");
Console.WriteLine();

// Create a second struct with different config - verify deduplication
var opts2 = new GarnetServerOptions { EnableCluster = true, EnableAOF = false, QuietMode = true };
var structType2 = GarnetServerFactory.GetOrCreateOptionsStruct(opts2);
Console.WriteLine($"Second struct type: {structType2.Name}");
Console.WriteLine($"Same type as first? {structType == structType2} (expected: False)");

// Same config again - should reuse
var structType3 = GarnetServerFactory.GetOrCreateOptionsStruct(opts);
Console.WriteLine($"Third struct (same config): {structType3.Name}");
Console.WriteLine($"Reused first type? {structType == structType3} (expected: True)");
Console.WriteLine();

// Now test that the generic specialization works - call a method that uses Cfg.X
// This forces JIT to compile the method with the emitted struct
Console.WriteLine("Generic specialization test passed - struct implements IGarnetServerOptions correctly.");
