// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;

namespace BDN.benchmark
{
    public class BenchUtils
    {
        static readonly byte[] ascii_chars = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstvuwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        Random rnd;

        public BenchUtils()
        {
            rnd = new Random(674386);
        }

        public void RandomBytes(ref byte[] data, int startOffset = -1, int endOffset = -1)
        {
            startOffset = startOffset == -1 ? 0 : startOffset;
            endOffset = endOffset == -1 ? data.Length : endOffset;
            for (int i = startOffset; i < endOffset; i++)
                data[i] = ascii_chars[rnd.Next(ascii_chars.Length)];
        }
    }
}
