// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using BDN.benchmark.Lua;
using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for SCRIPT LOAD, SCRIPT EXISTS, EVAL, and EVALSHA
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ScriptOperations
    {
        // Small script that does 1 operation and no logic
        const string SmallScriptText = @"return redis.call('GET', KEYS[1]);";

        // Large script that does 2 operations and lots of logic
        const string LargeScriptText = @"
-- based on a real UDF, with some extraneous ops removed

local userKey = KEYS[1]
local identifier = ARGV[1]
local currentTime = ARGV[2]
local newSession = ARGV[3]

local userKeyValue = redis.call(""GET"", userKey)
	
local updatedValue = nil
local returnValue = -1
	
if userKeyValue then
-- needs to be updated

	local oldestEntry = nil
	local oldestEntryUpdateTime = nil
		
	local match = nil
		
	local entryCount = 0
		
-- loop over each entry, looking for one to update
	for entry in string.gmatch(userKeyValue, ""([^%|]+)"") do
		entryCount = entryCount + 1
		
		local entryIdentifier = nil
		local entrySessionNumber = -1
		local entryRequestCount = -1
		local entryLastSessionUpdateTime = -1
			
		local ix = 0
		for part in string.gmatch(entry, ""([^:]+)"") do 
			if ix == 0 then
				entryIdentifier = part
			elseif ix == 1 then
				entrySessionNumber = tonumber(part)
			elseif ix == 2 then
				entryRequestCount = tonumber(part)
			elseif ix == 3 then
				entryLastSessionUpdateTime = tonumber(part)
			else
-- malformed, too many parts
				return -1
			end
				
			ix = ix + 1
		end
			
		if ix ~= 4 then
-- malformed, too few parts
			return -2
		end
			
		if entryIdentifier == identifier then
-- found the one to update
			local updatedEntry = nil

			if tonumber(newSession) == 1 then
				local updatedSessionNumber = entrySessionNumber + 1
				updatedEntry = entryIdentifier .. "":"" .. tostring(updatedSessionNumber) .. "":1:"" .. tostring(currentTime)
				returnValue = 3
			else
				local updatedRequestCount = entryRequestCount + 1
				updatedEntry = entryIdentifier .. "":"" .. tostring(entrySessionNumber) .. "":"" .. tostring(updatedRequestCount) .. "":"" .. tostring(currentTime)
				returnValue = 2
			end

-- have to escape the replacement, since Lua doesn't have a literal replace :/
			local escapedEntry = string.gsub(entry, ""%p"", ""%%%1"")
			updatedValue = string.gsub(userKeyValue, escapedEntry, updatedEntry)
				
			break
		end
			
		if oldestEntryUpdateTime == nil or oldestEntryUpdateTime > entryLastSessionUpdateTime then
-- remember the oldest entry, so we can replace it if needed
			oldestEntry = entry
			oldestEntryUpdateTime = entryLastSessionUpdateTime
		end
	end
		
	if updatedValue == nil then
-- we didn't update an existing value, so we need to add it

		local newEntry = identifier .. "":1:1:"" .. tostring(currentTime)

		if entryCount < 20 then
-- there's room, just append it
			updatedValue = userKeyValue .. ""|"" .. newEntry
			returnValue = 4
		else
-- there isn't room, replace the LRU entry

-- have to escape the replacement, since Lua doesn't have a literal replace :/
			local escapedOldestEntry = string.gsub(oldestEntry, ""%p"", ""%%%1"")

			updatedValue = string.gsub(userKeyValue, escapedOldestEntry, newEntry)
			returnValue = 5
		end
	end
else
-- needs to be created
	updatedValue = identifier .. "":1:1:"" .. tostring(currentTime)
		
	returnValue = 1
end
	
redis.call(""SET"", userKey, updatedValue)
	
return returnValue
";

        static ReadOnlySpan<byte> SCRIPT_LOAD => "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$8\r\nreturn 1\r\n"u8;
        Request scriptLoad;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_LOADED => "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$10\r\nreturn nil\r\n"u8;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_TRUE => "*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$40\r\n79cefb99366d8809d2e903c5f36f50c2b731913f\r\n"u8;
        Request scriptExistsTrue;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_FALSE => "*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$40\r\n0000000000000000000000000000000000000000\r\n"u8;
        Request scriptExistsFalse;

        static ReadOnlySpan<byte> EVAL => "*3\r\n$4\r\nEVAL\r\n$10\r\nreturn nil\r\n$1\r\n0\r\n"u8;
        Request eval;

        static ReadOnlySpan<byte> EVALSHA => "*3\r\n$7\r\nEVALSHA\r\n$40\r\n79cefb99366d8809d2e903c5f36f50c2b731913f\r\n$1\r\n0\r\n"u8;
        Request evalSha;

        Request evalShaSmallScript;
        Request evalShaLargeScript;

        static ReadOnlySpan<byte> ARRAY_RETURN => "*3\r\n$4\r\nEVAL\r\n$22\r\nreturn {1, 2, 3, 4, 5}\r\n$1\r\n0\r\n"u8;
        Request arrayReturn;


        /// <summary>
        /// Lua parameters
        /// </summary>
        [ParamsSource(nameof(LuaParamsProvider))]
        public LuaParams Params { get; set; }

        /// <summary>
        /// Lua parameters provider
        /// </summary>
        public static IEnumerable<LuaParams> LuaParamsProvider()
        => [
            new(LuaMemoryManagementMode.Native, false),
            new(LuaMemoryManagementMode.Tracked, false),
            new(LuaMemoryManagementMode.Tracked, true),
            new(LuaMemoryManagementMode.Managed, false),
            new(LuaMemoryManagementMode.Managed, true),
        ];

        /// <summary>
        /// Batch size per method invocation
        /// With a batchSize of 100, we have a convenient conversion of latency to throughput:
        ///   5 us = 20 Mops/sec
        ///  10 us = 10 Mops/sec
        ///  20 us =  5 Mops/sec
        ///  25 us =  4 Mops/sec
        /// 100 us =  1 Mops/sec
        /// </summary>
        internal const int batchSize = 100;
        internal EmbeddedRespServer server;
        internal RespServerSession session;

        /// <summary>
        /// Setup
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true,
                EnableLua = true,
                LuaOptions = Params.CreateOptions(),
            };

            server = new EmbeddedRespServer(opts);

            session = server.GetRespSession();

            SetupOperation(ref scriptLoad, SCRIPT_LOAD);

            Request scriptExistsLoaded = default;
            SetupOperation(ref scriptExistsLoaded, SCRIPT_EXISTS_LOADED, 1);
            Send(scriptExistsLoaded);

            SetupOperation(ref scriptExistsTrue, SCRIPT_EXISTS_TRUE);
            SetupOperation(ref scriptExistsFalse, SCRIPT_EXISTS_FALSE);
            SetupOperation(ref eval, EVAL);
            SetupOperation(ref evalSha, EVALSHA);
            SetupOperation(ref arrayReturn, ARRAY_RETURN);

            // Setup small script
            var loadSmallScript = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${SmallScriptText.Length}\r\n{SmallScriptText}\r\n";
            var loadSmallScriptBytes = Encoding.UTF8.GetBytes(loadSmallScript);
            fixed (byte* loadPtr = loadSmallScriptBytes)
            {
                Send(new Request { buffer = loadSmallScriptBytes, bufferPtr = loadPtr });
            }

            var smallScriptHash = string.Join("", SHA1.HashData(Encoding.UTF8.GetBytes(SmallScriptText)).Select(static x => x.ToString("x2")));
            var evalShaSmallScriptStr = $"*4\r\n$7\r\nEVALSHA\r\n$40\r\n{smallScriptHash}\r\n$1\r\n1\r\n$3\r\nfoo\r\n";
            SetupOperation(ref evalShaSmallScript, evalShaSmallScriptStr);

            // Setup large script
            var loadLargeScript = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${LargeScriptText.Length}\r\n{LargeScriptText}\r\n";
            var loadLargeScriptBytes = Encoding.UTF8.GetBytes(loadLargeScript);
            fixed (byte* loadPtr = loadLargeScriptBytes)
            {
                Send(new Request { buffer = loadLargeScriptBytes, bufferPtr = loadPtr });
            }

            var largeScriptHash = string.Join("", SHA1.HashData(Encoding.UTF8.GetBytes(LargeScriptText)).Select(static x => x.ToString("x2")));
            var largeScriptEvals = new List<byte>();
            for (var i = 0; i < batchSize; i++)
            {
                var evalShaLargeScript = $"*7\r\n$7\r\nEVALSHA\r\n$40\r\n{largeScriptHash}\r\n$1\r\n1\r\n$5\r\nhello\r\n";

                var id = Guid.NewGuid().ToString();
                evalShaLargeScript += $"${id.Length}\r\n";
                evalShaLargeScript += $"{id}\r\n";

                var time = (i * 10).ToString();
                evalShaLargeScript += $"${time.Length}\r\n";
                evalShaLargeScript += $"{time}\r\n";

                var newSession = i % 2;
                evalShaLargeScript += "$1\r\n";
                evalShaLargeScript += $"{newSession}\r\n";

                var asBytes = Encoding.UTF8.GetBytes(evalShaLargeScript);
                largeScriptEvals.AddRange(asBytes);
            }
            SetupOperation(ref evalShaLargeScript, largeScriptEvals);
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        [Benchmark]
        public void ScriptLoad()
        {
            Send(scriptLoad);
        }

        [Benchmark]
        public void ScriptExistsTrue()
        {
            Send(scriptExistsTrue);
        }

        [Benchmark]
        public void ScriptExistsFalse()
        {
            Send(scriptExistsFalse);
        }

        [Benchmark]
        public void Eval()
        {
            Send(eval);
        }

        [Benchmark]
        public void EvalSha()
        {
            Send(evalSha);
        }

        [Benchmark]
        public void SmallScript()
        {
            Send(evalShaSmallScript);
        }

        [Benchmark]
        public void LargeScript()
        {
            Send(evalShaLargeScript);
        }

        [Benchmark]
        public void ArrayReturn()
        {
            Send(arrayReturn);
        }

        private void Send(Request request)
        {
            _ = session.TryConsumeMessages(request.bufferPtr, request.buffer.Length);
        }

        private unsafe void SetupOperation(ref Request request, ReadOnlySpan<byte> operation, int batchSize = batchSize)
        {
            request.buffer = GC.AllocateArray<byte>(operation.Length * batchSize, pinned: true);
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
            for (int i = 0; i < batchSize; i++)
                operation.CopyTo(new Span<byte>(request.buffer).Slice(i * operation.Length));
        }

        private unsafe void SetupOperation(ref Request request, string operation, int batchSize = batchSize)
        {
            request.buffer = GC.AllocateUninitializedArray<byte>(operation.Length * batchSize, pinned: true);
            for (var i = 0; i < batchSize; i++)
            {
                var start = i * operation.Length;
                Encoding.UTF8.GetBytes(operation, request.buffer.AsSpan().Slice(start, operation.Length));
            }
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
        }

        private unsafe void SetupOperation(ref Request request, List<byte> operationBytes)
        {
            request.buffer = GC.AllocateUninitializedArray<byte>(operationBytes.Count, pinned: true);
            operationBytes.CopyTo(request.buffer);
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
        }
    }
}