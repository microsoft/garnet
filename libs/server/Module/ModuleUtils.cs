// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using Garnet.common;

namespace Garnet.server
{
    public class ModuleUtils
    {
        public static bool LoadAssemblies(
            IEnumerable<string> binaryPaths,
            string[] allowedExtensionPaths,
            bool allowUnsignedAssemblies,
            out IEnumerable<Assembly> loadedAssemblies,
            out ReadOnlySpan<byte> errorMessage)
        {
            loadedAssemblies = null;
            errorMessage = default;

            // Get all binary file paths from inputs binary paths
            if (!FileUtils.TryGetFiles(binaryPaths, out var files, out _, [".dll", ".exe"], SearchOption.AllDirectories))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_GETTING_BINARY_FILES;
                return false;
            }

            // Check that all binary files are contained in allowed binary paths
            var binaryFiles = files.ToArray();
            if (allowedExtensionPaths != null)
            {
                if (binaryFiles.Any(f =>
                        allowedExtensionPaths.All(p => !FileUtils.IsFileInDirectory(f, p))))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_BINARY_FILES_NOT_IN_ALLOWED_PATHS;
                    return false;
                }
            }

            // If necessary, check that all assemblies are digitally signed
            if (!allowUnsignedAssemblies)
            {
                foreach (var filePath in files)
                {
                    using var fs = File.OpenRead(filePath);
                    using var peReader = new PEReader(fs);

                    var metadataReader = peReader.GetMetadataReader();
                    var assemblyPublicKeyHandle = metadataReader.GetAssemblyDefinition().PublicKey;

                    if (assemblyPublicKeyHandle.IsNil)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_ASSEMBLY_NOT_SIGNED;
                        return false;
                    }

                    var publicKeyBytes = metadataReader.GetBlobBytes(assemblyPublicKeyHandle);
                    if (publicKeyBytes == null || publicKeyBytes.Length == 0)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_ASSEMBLY_NOT_SIGNED;
                        return false;
                    }
                }
            }

            // Get all assemblies from binary files
            if (!FileUtils.TryLoadAssemblies(binaryFiles, out loadedAssemblies, out _))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_LOADING_ASSEMBLIES;
                return false;
            }

            return true;
        }
    }
}