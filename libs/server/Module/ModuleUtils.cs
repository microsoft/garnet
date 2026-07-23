// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Runtime.Loader;
using Garnet.common;

namespace Garnet.server
{
    public class ModuleUtils
    {
        /// <summary>
        /// Parses a single module specification (as provided to the <c>--loadmodulecs</c> option) into its
        /// module path and optional arguments. The specification has the form
        /// <c>&lt;module-path&gt; [arg0 arg1 ...]</c>, where the path and each argument are separated by spaces.
        ///
        /// The path is delimited from its arguments deterministically:
        /// <list type="number">
        /// <item>If the specification begins with a double quote, the path is the text between that quote and the
        /// next double quote (so it may contain spaces), and any remaining space-separated text is treated as the
        /// module arguments. Such a specification is invalid (returns false) if the quote is not terminated or the
        /// quoted path is empty or whitespace-only.</item>
        /// <item>Otherwise, the first space-separated token is the module path and the remaining tokens are its
        /// arguments. A path that contains spaces must therefore be wrapped in double quotes.</item>
        /// </list>
        /// </summary>
        /// <param name="moduleSpec">The raw module specification string.</param>
        /// <param name="modulePath">The parsed module path.</param>
        /// <param name="moduleArgs">The parsed module arguments (empty if none).</param>
        /// <returns>True if a non-empty module path was parsed; otherwise false (e.g. empty, whitespace-only or
        /// malformed quoted specification).</returns>
        public static bool TryParseModuleSpec(string moduleSpec, out string modulePath, out string[] moduleArgs)
        {
            modulePath = null;
            moduleArgs = [];

            if (string.IsNullOrWhiteSpace(moduleSpec))
                return false;

            var spec = moduleSpec.Trim();

            // A double-quoted path may contain spaces: "<path>" [args...]. The quote must be terminated and the
            // quoted path must be non-empty and non-whitespace, otherwise the specification is invalid.
            if (spec[0] == '"')
            {
                var closingQuoteIdx = spec.IndexOf('"', 1);
                if (closingQuoteIdx < 0)
                    return false;

                var quotedPath = spec[1..closingQuoteIdx];
                if (string.IsNullOrWhiteSpace(quotedPath))
                    return false;

                modulePath = quotedPath;
                moduleArgs = SplitModuleArgs(spec[(closingQuoteIdx + 1)..]);
                return true;
            }

            // Otherwise the path must not contain spaces: the first space-separated token is the path and the
            // remaining tokens are its arguments. To use a path containing spaces, wrap it in double quotes.
            var tokens = spec.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            modulePath = tokens[0];
            moduleArgs = tokens.Length > 1 ? tokens[1..] : [];
            return true;
        }

        private static string[] SplitModuleArgs(string args)
            => args.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        /// <summary>
        /// Loads only the assemblies that the module at <paramref name="modulePath"/> references (transitively)
        /// from <paramref name="binPath"/>, instead of loading every DLL in the directory.
        /// </summary>
        /// <param name="modulePath">Full path to the module assembly file</param>
        /// <param name="binPath">Directory containing the module and its dependencies</param>
        /// <param name="allowedExtensionPaths">List of allowed paths for loading assemblies from</param>
        /// <param name="allowUnsignedAssemblies">True if loading unsigned assemblies is allowed</param>
        /// <param name="loadedAssemblies">Assemblies that were loaded</param>
        /// <param name="errorMessage">Error message on failure</param>
        /// <returns>True if all required dependencies were loaded successfully</returns>
        public static bool LoadModuleDependencies(
            string modulePath,
            string binPath,
            string[] allowedExtensionPaths,
            bool allowUnsignedAssemblies,
            out IEnumerable<Assembly> loadedAssemblies,
            out ReadOnlySpan<byte> errorMessage)
        {
            loadedAssemblies = null;
            errorMessage = default;

            // Read referenced assembly names from the module without loading it into the runtime
            AssemblyName[] referencedNames;
            try
            {
                referencedNames = GetReferencedAssemblyNames(modulePath);
            }
            catch
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_LOADING_ASSEMBLIES;
                return false;
            }

            if (referencedNames.Length == 0)
                return true;

            // Build a map of available DLLs in binPath (filename without extension → full path)
            var availableFiles = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var file in Directory.GetFiles(binPath, "*.dll", SearchOption.TopDirectoryOnly))
                _ = availableFiles.TryAdd(Path.GetFileNameWithoutExtension(file), file);

            // Collect the set of dependency files to load (transitive closure)
            var alreadyLoaded = new HashSet<string>(
                AssemblyLoadContext.Default.Assemblies
                    .Where(a => !a.IsDynamic && a.GetName().Name != null)
                    .Select(a => a.GetName().Name!),
                StringComparer.Ordinal);

            var toLoad = new List<string>();
            var visited = new HashSet<string>(StringComparer.Ordinal);
            var queue = new Queue<AssemblyName>(referencedNames);

            while (queue.TryDequeue(out var asmName))
            {
                var name = asmName.Name;
                if (name == null || !visited.Add(name))
                    continue;

                // Skip if already loaded in the runtime or not available in binPath (it may be a framework assembly)
                if (alreadyLoaded.Contains(name) || !availableFiles.TryGetValue(name, out var filePath))
                    continue;

                toLoad.Add(filePath);

                // Walk transitive dependencies
                try
                {
                    foreach (var t in GetReferencedAssemblyNames(filePath))
                        queue.Enqueue(t);
                }
                catch
                {
                    // If we can't read metadata, we'll still try to load the file
                }
            }

            if (toLoad.Count == 0)
                return true;

            return LoadAssemblies(toLoad, allowedExtensionPaths, allowUnsignedAssemblies,
                out loadedAssemblies, out errorMessage, ignoreAssemblyLoadErrors: true, ignorePathCheckWhenUndefined: true);
        }

        /// <summary>
        /// Reads referenced assembly names from an assembly file using metadata, without loading it into the runtime.
        /// </summary>
        private static AssemblyName[] GetReferencedAssemblyNames(string assemblyPath)
        {
            using var fs = File.OpenRead(assemblyPath);
            using var peReader = new PEReader(fs);

            if (!peReader.HasMetadata)
                return [];

            var metadataReader = peReader.GetMetadataReader();
            var refs = new List<AssemblyName>();

            foreach (var refHandle in metadataReader.AssemblyReferences)
            {
                var asmRef = metadataReader.GetAssemblyReference(refHandle);
                refs.Add(new AssemblyName
                {
                    Name = metadataReader.GetString(asmRef.Name),
                    Version = asmRef.Version
                });
            }

            return [.. refs];
        }

        /// <summary>
        /// Load assemblies from specified binary paths
        /// </summary>
        /// <param name="binaryPaths">Source paths for assemblies (can be either files or directories)</param>
        /// <param name="allowedExtensionPaths">List of allowed paths for loading assemblies from</param>
        /// <param name="allowUnsignedAssemblies">True if loading unsigned assemblies is allowed</param>
        /// <param name="loadedAssemblies">Loaded assemblies</param>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ignoreFileNames">File names to ignore (optional)</param>
        /// <param name="searchOption">In case path is a directory, determines whether to search only top directory or all subdirectories</param>
        /// <param name="ignoreAssemblyLoadErrors">False if method should return an error when at least one assembly was not loaded correctly (false by default)</param>
        /// <param name="ignorePathCheckWhenUndefined">Ignore path check when path is undefined (default false).</param>
        /// <returns></returns>
        public static bool LoadAssemblies(
            IEnumerable<string> binaryPaths,
            string[] allowedExtensionPaths,
            bool allowUnsignedAssemblies,
            out IEnumerable<Assembly> loadedAssemblies,
            out ReadOnlySpan<byte> errorMessage,
            string[] ignoreFileNames = null,
            SearchOption searchOption = SearchOption.AllDirectories,
            bool ignoreAssemblyLoadErrors = false,
            bool ignorePathCheckWhenUndefined = false)
        {
            loadedAssemblies = null;
            errorMessage = default;

            // Get all binary file paths from inputs binary paths
            if (!FileUtils.TryGetFiles(binaryPaths, out var binaryFiles, out _, [".dll", ".exe"], ignoreFileNames, SearchOption.AllDirectories))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_GETTING_BINARY_FILES;
                return false;
            }

            if ((allowedExtensionPaths == null) && !ignorePathCheckWhenUndefined)
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_MUST_DEFINE_ASSEMBLY_BINPATH;
                return false;
            }
            // Check that all binary files are contained in allowed binary paths
            else if (allowedExtensionPaths != null)
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
                foreach (var filePath in binaryFiles)
                {
                    try
                    {
                        var isSigned = false;

                        using var fs = File.OpenRead(filePath);
                        using var peReader = new PEReader(fs);

                        if (peReader.HasMetadata)
                        {
                            var metadataReader = peReader.GetMetadataReader();
                            var assemblyPublicKeyHandle = metadataReader.GetAssemblyDefinition().PublicKey;

                            isSigned = !assemblyPublicKeyHandle.IsNil &&
                                       metadataReader.GetBlobBytes(assemblyPublicKeyHandle).Length > 0;
                        }

                        if (!isSigned)
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_ASSEMBLY_NOT_SIGNED;
                            return false;
                        }
                    }
                    catch (Exception)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_ACCESSING_ASSEMBLIES;
                        return false;
                    }
                }
            }

            // Get all assemblies from binary files
            if (!FileUtils.TryLoadAssemblies(binaryFiles, out loadedAssemblies, out _) && !ignoreAssemblyLoadErrors)
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_LOADING_ASSEMBLIES;
                return false;
            }

            return true;
        }
    }
}