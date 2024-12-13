// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for SCRIPT LOAD, SCRIPT EXISTS, EVAL, and EVALSHA
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ScriptOperations : OperationsBase
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
        byte[] scriptLoadRequestBuffer;
        byte* scriptLoadRequestBufferPointer;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_LOADED => "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$10\r\nreturn nil\r\n"u8;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_TRUE => "*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$40\r\n79cefb99366d8809d2e903c5f36f50c2b731913f\r\n"u8;
        byte[] scriptExistsTrueRequestBuffer;
        byte* scriptExistsTrueRequestBufferPointer;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_FALSE => "*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$40\r\n0000000000000000000000000000000000000000\r\n"u8;
        byte[] scriptExistsFalseRequestBuffer;
        byte* scriptExistsFalseRequestBufferPointer;

        static ReadOnlySpan<byte> EVAL => "*3\r\n$4\r\nEVAL\r\n$10\r\nreturn nil\r\n$1\r\n0\r\n"u8;
        byte[] evalRequestBuffer;
        byte* evalRequestBufferPointer;

        static ReadOnlySpan<byte> EVALSHA => "*3\r\n$7\r\nEVALSHA\r\n$40\r\n79cefb99366d8809d2e903c5f36f50c2b731913f\r\n$1\r\n0\r\n"u8;
        byte[] evalShaRequestBuffer;
        byte* evalShaRequestBufferPointer;

        byte[] evalShaSmallScriptBuffer;
        byte* evalShaSmallScriptBufferPointer;

        byte[] evalShaLargeScriptBuffer;
        byte* evalShaLargeScriptBufferPointer;

        static ReadOnlySpan<byte> ARRAY_RETURN => "*3\r\n$4\r\nEVAL\r\n$22\r\nreturn {1, 2, 3, 4, 5}\r\n$1\r\n0\r\n"u8;
        byte[] arrayReturnRequestBuffer;
        byte* arrayReturnRequestBufferPointer;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref scriptLoadRequestBuffer, ref scriptLoadRequestBufferPointer, SCRIPT_LOAD);

            byte[] scriptExistsLoadedBuffer = null;
            byte* scriptExistsLoadedPointer = null;
            SetupOperation(ref scriptExistsLoadedBuffer, ref scriptExistsLoadedPointer, SCRIPT_EXISTS_LOADED);
            _ = session.TryConsumeMessages(scriptExistsLoadedPointer, scriptExistsLoadedBuffer.Length);
            SetupOperation(ref scriptExistsTrueRequestBuffer, ref scriptExistsTrueRequestBufferPointer, SCRIPT_EXISTS_TRUE);

            SetupOperation(ref scriptExistsFalseRequestBuffer, ref scriptExistsFalseRequestBufferPointer, SCRIPT_EXISTS_FALSE);

            SetupOperation(ref evalRequestBuffer, ref evalRequestBufferPointer, EVAL);

            SetupOperation(ref evalShaRequestBuffer, ref evalShaRequestBufferPointer, EVALSHA);

            SetupOperation(ref arrayReturnRequestBuffer, ref arrayReturnRequestBufferPointer, ARRAY_RETURN);

            // Setup small script
            var loadSmallScript = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${SmallScriptText.Length}\r\n{SmallScriptText}\r\n";
            var loadSmallScriptBytes = Encoding.UTF8.GetBytes(loadSmallScript);
            fixed (byte* loadPtr = loadSmallScriptBytes)
            {
                _ = session.TryConsumeMessages(loadPtr, loadSmallScriptBytes.Length);
            }

            var smallScriptHash = string.Join("", SHA1.HashData(Encoding.UTF8.GetBytes(SmallScriptText)).Select(static x => x.ToString("x2")));
            var evalShaSmallScript = $"*4\r\n$7\r\nEVALSHA\r\n$40\r\n{smallScriptHash}\r\n$1\r\n1\r\n$3\r\nfoo\r\n";
            evalShaSmallScriptBuffer = GC.AllocateUninitializedArray<byte>(evalShaSmallScript.Length * batchSize, pinned: true);
            for (var i = 0; i < batchSize; i++)
            {
                var start = i * evalShaSmallScript.Length;
                Encoding.UTF8.GetBytes(evalShaSmallScript, evalShaSmallScriptBuffer.AsSpan().Slice(start, evalShaSmallScript.Length));
            }
            evalShaSmallScriptBufferPointer = (byte*)Unsafe.AsPointer(ref evalShaSmallScriptBuffer[0]);

            // Setup large script
            var loadLargeScript = $"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n${LargeScriptText.Length}\r\n{LargeScriptText}\r\n";
            var loadLargeScriptBytes = Encoding.UTF8.GetBytes(loadLargeScript);
            fixed (byte* loadPtr = loadLargeScriptBytes)
            {
                _ = session.TryConsumeMessages(loadPtr, loadLargeScriptBytes.Length);
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
            evalShaLargeScriptBuffer = GC.AllocateUninitializedArray<byte>(largeScriptEvals.Count, pinned: true);
            largeScriptEvals.CopyTo(evalShaLargeScriptBuffer);
            evalShaLargeScriptBufferPointer = (byte*)Unsafe.AsPointer(ref evalShaLargeScriptBuffer[0]);
        }

        [Benchmark]
        public void ScriptLoad()
        {
            _ = session.TryConsumeMessages(scriptLoadRequestBufferPointer, scriptLoadRequestBuffer.Length);
        }

        [Benchmark]
        public void ScriptExistsTrue()
        {
            _ = session.TryConsumeMessages(scriptExistsTrueRequestBufferPointer, scriptExistsTrueRequestBuffer.Length);
        }

        [Benchmark]
        public void ScriptExistsFalse()
        {
            _ = session.TryConsumeMessages(scriptExistsFalseRequestBufferPointer, scriptExistsFalseRequestBuffer.Length);
        }

        [Benchmark]
        public void Eval()
        {
            _ = session.TryConsumeMessages(evalRequestBufferPointer, evalRequestBuffer.Length);
        }

        [Benchmark]
        public void EvalSha()
        {
            _ = session.TryConsumeMessages(evalShaRequestBufferPointer, evalShaRequestBuffer.Length);
        }

        [Benchmark]
        public void SmallScript()
        {
            _ = session.TryConsumeMessages(evalShaSmallScriptBufferPointer, evalShaSmallScriptBuffer.Length);
        }

        [Benchmark]
        public void LargeScript()
        {
            _ = session.TryConsumeMessages(evalShaLargeScriptBufferPointer, evalShaLargeScriptBuffer.Length);
        }

        [Benchmark]
        public void ArrayReturn()
        {
            _ = session.TryConsumeMessages(arrayReturnRequestBufferPointer, arrayReturnRequestBuffer.Length);
        }
    }
}