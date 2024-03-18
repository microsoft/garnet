// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server.ACL
{
    /// <summary>
    /// ACL exception base
    /// </summary>
    class ACLException : Exception
    {
        public ACLException(string message)
        : base(message) { }
    }

    /// <summary>
    /// Exception when parsing ACL rules
    /// </summary>
    class ACLParsingException : ACLException
    {
        /// <summary>
        /// Creates a new ACL Parsing Exception
        /// </summary>
        /// <param name="message">A message describing the exception that occurred.</param>
        /// <param name="filename">The name of the file in which the error occurred.</param>
        /// <param name="line">The line within the file on which the error occurred.</param>
        public ACLParsingException(string message, string filename = null, int line = -1)
        : base(message)
        {
            Filename = filename;
            Line = line;
        }

        /// <summary>
        /// The name of the file in which the error occurred.
        /// </summary>
        public readonly string Filename;

        /// <summary>
        /// The line in the input file, in which the error occurred.
        /// </summary>
        public readonly int Line;
    }

    /// <summary>
    /// Exception when interacting with ACL passwords
    /// </summary>
    class ACLPasswordException : ACLException
    {
        public ACLPasswordException(string message)
        : base(message) { }
    }

    /// <summary>
    /// Exception indicating an undefined ACL operation
    /// </summary>
    class ACLUnknownOperationException : ACLException
    {
        public ACLUnknownOperationException(string operation)
        : base($"Unknown operation '{operation}'") { }
    }

    /// <summary>
    /// Exception indicating the given category does not exist
    /// </summary>
    class ACLCategoryDoesNotExistException : ACLException
    {
        public ACLCategoryDoesNotExistException(string category)
        : base($"ACL Category '{category}' does not exist") { }
    }

    /// <summary>
    /// Exception indicating the given user does not exist
    /// </summary>
    class ACLUserDoesNotExistException : ACLException
    {
        public ACLUserDoesNotExistException(string user)
        : base($"A user with name '{user}' does not exist") { }
    }

    /// <summary>
    /// Exception indicating a user with the given name already exists
    /// </summary>
    class ACLUserAlreadyExistsException : ACLException
    {
        public ACLUserAlreadyExistsException(string user)
        : base($"A user with name '{user}' already exists.") { }
    }
}