// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace GarnetClientStress
{
    public enum StressTestType : byte
    {
        TaskScaling,
        PingDispose
    }

    public class StressTestUtils
    {
        static readonly string ascii_chars = "abcdefghijklmnopqrstvuwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public static string RandomValue(Random r, int valueSize)
        {
            return new string([.. Enumerable.Repeat(ascii_chars, valueSize).Select(s => s[r.Next(s.Length)])]);
        }

        public static T NotNull<T>(T argument, string parameterName) where T : class => argument ?? throw new ArgumentNullException(parameterName);

        private static X509Certificate2 GetClientCertificate(string filename, string password)
        {
#if NET9_0_OR_GREATER
            return X509CertificateLoader.LoadPkcs12FromFile(filename, password);
#else
            return new X509Certificate2(filename, password);
#endif
        }

        public static SslClientAuthenticationOptions GetTlsOptions(string tlsHost)
        {
            return new SslClientAuthenticationOptions
            {
                ClientCertificates = [GetClientCertificate("testcert.pfx", "placeholder")],
                TargetHost = tlsHost,
                AllowRenegotiation = false,
                RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true,
            };
        }

        readonly Options opts;

        public StressTestUtils(Options opts)
        {
            this.opts = NotNull(opts, nameof(opts));
        }

        public void RunTest()
        {
            switch (opts.StressType)
            {
                case StressTestType.TaskScaling:
                    TaskScaling stressTest = new(opts);
                    stressTest.Run();
                    break;
                case StressTestType.PingDispose:
                    SimpleStressTests.RunPingDispose(opts);
                    break;
                default:
                    break;
            }

        }
    }
}