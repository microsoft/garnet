// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common.Parsing;

namespace Garnet.test
{
    public static class TestSimpleReadRESP
    {
        public static object[] ReadRESP(byte[] inputArray)
        {
            var pos = 0;
            var input = Encoding.ASCII.GetString(inputArray);

            return Read(input, ref pos);
        }

        public static object[] ReadRESP(string input)
        {
            var pos = 0;
            return Read(input, ref pos);
        }

        static object[] Read(string input, ref int pos)
        {
            if (input.Length < 3)
                return default;

            switch (input[pos])
            {
                case '+':
                    pos++;
                    var resultString = ReadSimpleString(input, ref pos);
                    return [resultString];

                case ':':
                    pos++;
                    var resultInt = ReadIntegerAsString(input, ref pos);
                    return [resultInt];

                case '-':
                    pos++;
                    var errorString = ReadErrorAsString(input, ref pos);
                    return [errorString];

                case '$':
                    pos++;
                    var resultBulk = ReadStringWithLengthHeader(input, ref pos);
                    return [resultBulk];

                case '*':
                    pos++;
                    var resultArray = ReadStringArrayWithLengthHeader(input, ref pos);
                    return resultArray;

                default:
                    RespParsingException.Throw($"Unexpected character {input[0]}");
                    throw new NotImplementedException();
            }
        }

        private static object[] ReadStringArrayWithLengthHeader(string input, ref int loc)
        {
            var pos = input.IndexOf("\r\n", loc);
            if (pos == -1)
                RespParsingException.Throw("No newline");

            var arraylen = int.Parse(input[loc..pos]);
            loc = pos + 2;

            if (arraylen < 0)
                RespParsingException.ThrowInvalidLength(arraylen);

            List<object> lo = new();
            for (var i = 0; i < arraylen; i++)
            {
                var res = Read(input, ref loc);
                lo.AddRange(res);
            }

            return [.. lo];
        }

        private static string ReadStringWithLengthHeader(string input, ref int loc)
        {
            var pos = input.IndexOf("\r\n", loc);
            if (pos == -1)
                RespParsingException.Throw("No newline");

            var len = int.Parse(input[loc..pos]);
            if (len < -1)
                RespParsingException.ThrowInvalidStringLength(len);

            loc = input.IndexOf("\r\n", pos + 2) + 2;
            if (loc < 0)
                RespParsingException.Throw("No newline!");

            if (len == -1)
            {
                return null;
            }

            if (loc != pos + 2 + len + 2)
                RespParsingException.Throw("Invalid length!");

            return input[(pos + 2)..(pos + 2 + len)];
        }

        private static string ReadErrorAsString(string input, ref int loc)
        {
            var pos = input.IndexOf("\r\n", loc);
            if (pos == -1)
                RespParsingException.Throw("No newline");

            var ret = input[loc..pos];
            loc = pos + 2;

            return ret;
        }

        private static long ReadIntegerAsString(string input, ref int loc)
        {
            var pos = input.IndexOf("\r\n", loc);
            if (pos == -1)
                RespParsingException.Throw("No newline");

            var ret = long.Parse(input[loc..pos]);
            loc = pos + 2;

            return ret;
        }

        private static string ReadSimpleString(string input, ref int loc)
        {
            var pos = input.IndexOf("\r\n", loc);
            if (pos == -1)
                RespParsingException.Throw("No newline");

            var ret = input[loc..pos];
            loc = pos + 2;

            return ret;
        }
    }
}