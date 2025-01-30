// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;

namespace BDN.benchmark.Lua
{
    /// <summary>
    /// Benchmark for non-script running operations in LuaRunner
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class LuaRunnerOperations
    {
        private const string SmallScript = "return nil";

        private const string LargeScript = @"
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

        /// <summary>
        /// Lua parameters
        /// </summary>
        [ParamsSource(nameof(LuaParamsProvider))]
        public LuaParams Params { get; set; }

        /// <summary>
        /// Lua parameters provider
        /// </summary>
        public IEnumerable<LuaParams> LuaParamsProvider()
        => [
            new(LuaMemoryManagementMode.Native, false),
            new(LuaMemoryManagementMode.Tracked, false),
            new(LuaMemoryManagementMode.Tracked, true),
            new(LuaMemoryManagementMode.Managed, false),
            new(LuaMemoryManagementMode.Managed, true),
        ];

        private EmbeddedRespServer server;
        private RespServerSession session;

        private LuaRunner paramsRunner;

        private LuaRunner smallCompileRunner;
        private LuaRunner largeCompileRunner;

        private LuaOptions opts;

        [GlobalSetup]
        public void GlobalSetup()
        {
            opts = Params.CreateOptions();

            server = new EmbeddedRespServer(new GarnetServerOptions() { EnableLua = true, QuietMode = true, LuaOptions = opts });

            session = server.GetRespSession();

            paramsRunner = new LuaRunner(opts, "return nil");

            smallCompileRunner = new LuaRunner(opts, SmallScript);
            largeCompileRunner = new LuaRunner(opts, LargeScript);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
            paramsRunner.Dispose();
        }

        [IterationSetup]
        public void IterationSetup()
        {
            session.EnterAndGetResponseObject();
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            session.ExitAndReturnResponseObject();
        }

        [Benchmark]
        public void ResetParametersSmall()
        {
            // First force up
            paramsRunner.ResetParameters(1, 1);

            // Then require a small amount of clearing (1 key, 1 arg)
            paramsRunner.ResetParameters(0, 0);
        }

        [Benchmark]
        public void ResetParametersLarge()
        {
            // First force up
            paramsRunner.ResetParameters(10, 10);

            // Then require a large amount of clearing (10 keys, 10 args)
            paramsRunner.ResetParameters(0, 0);
        }

        [Benchmark]
        public void ConstructSmall()
        {
            using var runner = new LuaRunner(opts, SmallScript);
        }

        [Benchmark]
        public void ConstructLarge()
        {
            using var runner = new LuaRunner(opts, LargeScript);
        }

        [Benchmark]
        public void CompileForSessionSmall()
        {
            smallCompileRunner.ResetCompilation();
            smallCompileRunner.CompileForSession(session);
        }

        [Benchmark]
        public void CompileForSessionLarge()
        {
            largeCompileRunner.ResetCompilation();
            largeCompileRunner.CompileForSession(session);
        }
    }
}