﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.server.Auth.Settings;

namespace BDN.benchmark.Resp
{
    [MemoryDiagnoser]
    public unsafe class RespParseACLStress : RespParseStress
    {
        [Params(false, true)]
        public bool UseACLs { get; set; }

        public new void GlobalSetup()
        {
            string aclFile = null;

            try
            {
                if (UseACLs)
                {
                    aclFile = Path.GetTempFileName();
                    File.WriteAllText(aclFile, @"user default on nopass -@all +ping +set +get");
                    authSettings = new AclAuthenticationPasswordSettings(aclFile);
                }
                base.GlobalSetup();
            }
            finally
            {
                if (aclFile != null)
                {
                    File.Delete(aclFile);
                }
            }
        }
    }
}