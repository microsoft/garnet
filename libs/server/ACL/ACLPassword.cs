// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Garnet.server.ACL
{
    /// <summary>
    /// Represents an individual ACL Password stored as a cryptographic (SHA-265) hash
    /// </summary>
    public class ACLPassword : IEquatable<ACLPassword>
    {
        /// <summary>
        /// The hash associated with this ACL password
        /// </summary>
        public byte[] PasswordHash { get; }

        /// <summary>
        /// Private constructor to initialize a new ACL Password with the given hash
        /// </summary>
        /// <param name="passwordHash">Password hash as byte array.</param>
        private ACLPassword(byte[] passwordHash)
        {
            PasswordHash = passwordHash;
        }

        /// <summary>
        /// Initializes a new ACLPassword from the given cleartext string.
        /// </summary>
        /// <param name="password">Cleartext password used to initialize the password hash.</param>
        /// <returns>ACLPassword object for the given cleartext password.</returns>
        public static ACLPassword ACLPasswordFromString(string password)
        {
            byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(password));
            return new ACLPassword(hash);
        }

        /// <summary>
        /// Initializes a new ACLPassword from the given string representation of a password hash.
        /// </summary>
        /// <param name="hashString">A hex-string containing a valid SHA-265 password hash.</param>
        /// <returns>ACLPassword object with the given hash.</returns>
        /// <exception cref="ACLPasswordException">Thrown when the given input string cannot be parsed.</exception>
        public static ACLPassword ACLPasswordFromHash(string hashString)
        {
            if (hashString.Length != 2 * NumHashBytes)
            {
                throw new ACLPasswordException("Unable to parse input password hash. The input is of wrong length.");
            }

            // Parse input byte by byte
            byte[] hash = new byte[NumHashBytes];
            try
            {
                for (int i = 0; i < hash.Length; i++)
                {
                    string byteString = hashString.Substring(i * 2, 2);
                    hash[i] = byte.Parse(byteString, NumberStyles.HexNumber, CultureInfo.InvariantCulture);
                }
            }
            catch (FormatException)
            {
                throw new ACLPasswordException("Unable to parse input password hash. The input is not of the correct format.");
            }

            return new ACLPassword(hash);
        }

        /// <summary>
        /// Outputs the hexadecimal representation of the password hash.
        /// </summary>
        /// <returns>Password hash as hex-string.</returns>
        public override string ToString()
        {
            var stringBuilder = new StringBuilder();

            for (int i = 0; i < PasswordHash.Length; i++)
            {
                stringBuilder.Append(PasswordHash[i].ToString("x2"));
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Compares the password hash with the given password hash.
        /// </summary>
        /// <param name="password">Password hash to compare to.</param>
        /// <returns>True if equal, otherwise false.</returns>
        public bool Equals(ACLPassword password)
        {
            if (PasswordHash.Length != password.PasswordHash.Length)
            {
                return false;
            }

            for (int i = 0; i < PasswordHash.Length; i++)
            {
                if (PasswordHash[i] != password.PasswordHash[i])
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Return a short non-cryptographic hash to speedup indexing.
        /// </summary>
        /// <returns>The hash value generated for this ACLPassword.</returns>
        public override int GetHashCode()
        {
            return PasswordHash[0];
        }

        /// <summary>
        /// Checks if the current ACLPassword is equal to the given object.
        /// </summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns>true if the current ACLPassword is equal to the other object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            ACLPassword objAsACLPassword = obj as ACLPassword;
            if (objAsACLPassword == null) return false;
            else return Equals(objAsACLPassword);
        }

        /// <summary>
        /// The number of bytes per hash
        /// </summary>
        const int NumHashBytes = 32;
    }
}