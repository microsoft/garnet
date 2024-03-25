// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Security.Cryptography;
using System.Text;

namespace Garnet.test
{
    /// <summary>
    /// Server credential instance, used to generate ACL file or interact with server.
    /// </summary>
    public struct ServerCredential(string user, string password, bool IsAdmin, bool IsClearText)
    {
        public string user = user;
        public string password = password;
        public byte[] hash = SHA256.HashData(Encoding.ASCII.GetBytes(password));
        public bool IsAdmin = IsAdmin;
        public bool IsClearText = IsClearText;
    }
}
