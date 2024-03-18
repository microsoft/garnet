// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Text;
using System.Text.RegularExpressions;

namespace Garnet.common
{
    /// <summary>
    /// Utility class with helper methods for handling local files
    /// </summary>
    public static class FileUtils
    {
        /// <summary>
        /// Receives a list of paths and searches for files matching the extensions given in the input
        /// </summary>
        /// <param name="paths">Paths to files or directories</param>
        /// <param name="extensions">Extensions to match files (defaults to any)</param>
        /// <param name="searchOption">In case path is a directory, determines whether to search only top directory or all subdirectories</param>
        /// <param name="files">Files that match the extensions</param>
        /// <param name="errorMessage">Error message, if applicable</param>
        /// <returns>True if successfully enumerated all directories</returns>
        public static bool TryGetFiles(IEnumerable<string> paths, out IEnumerable<string> files, out string errorMessage, string[] extensions = null, SearchOption searchOption = SearchOption.TopDirectoryOnly)
        {
            var validExtensionPattern = "^\\.[A-Za-z0-9]+$";
            var anyExtension = false;

            errorMessage = string.Empty;
            var sbErrorMessage = new StringBuilder();
            files = null;

            string extensionPattern = null;
            if (extensions == null || extensions.Length == 0)
            {
                anyExtension = true;
            }
            else
            {
                foreach (var extension in extensions)
                {
                    if (!Regex.IsMatch(extension, validExtensionPattern))
                    {
                        sbErrorMessage.AppendLine($"Illegal extension: {extension}");
                    }
                }

                errorMessage = sbErrorMessage.ToString();
                if (!errorMessage.IsNullOrEmpty())
                    return false;

                extensionPattern = $"$(?<=({string.Join("|", extensions.Select(ext => ext.Replace(".", "\\.")))}))";
            }

            var tmpFiles = new HashSet<string>();

            foreach (var path in paths)
            {
                if (File.Exists(path))
                {
                    if (anyExtension || Regex.IsMatch(path, extensionPattern))
                    {
                        tmpFiles.Add(path);
                    }

                    continue;
                }

                if (!Directory.Exists(path))
                {
                    sbErrorMessage.AppendLine($"Path does not exist: {path}");
                    continue;
                }

                string[] filePaths;

                try
                {
                    filePaths = Directory.GetFiles(path, $"*.*", searchOption);
                }
                catch (Exception ex) when (ex is SecurityException || ex is UnauthorizedAccessException)
                {
                    sbErrorMessage.AppendLine(
                        $"Unable to enumerate files in directory: {path}. Error: {ex.Message}");
                    break;
                }

                foreach (var filePath in filePaths)
                {
                    if (anyExtension || Regex.IsMatch(filePath, extensionPattern))
                    {
                        tmpFiles.Add(filePath);
                    }
                }
            }

            errorMessage = sbErrorMessage.ToString();
            if (!errorMessage.IsNullOrEmpty())
                return false;

            files = tmpFiles;
            return true;
        }

        /// <summary>
        /// Checks if specified file path is contained in directory at any level
        /// </summary>
        /// <param name="filePath">Path to file</param>
        /// <param name="dirPath">Path to directory</param>
        /// <returns>True if path is contained in directory at any level</returns>
        public static bool IsFileInDirectory(string filePath, string dirPath)
        {
            var fileInfo = new FileInfo(filePath);
            var fileDirInfo = fileInfo.Directory;
            var dirInfo = new DirectoryInfo(dirPath);

            while (fileDirInfo != null)
            {
                if (fileDirInfo.FullName == dirInfo.FullName)
                    return true;

                fileDirInfo = fileDirInfo.Parent;
            }

            return false;
        }

        /// <summary>
        /// Receives a list of file paths and attempts to load assemblies from these paths
        /// </summary>
        /// <param name="assemblyPaths">List of file paths pointing to assembly files</param>
        /// <param name="loadedAssemblies">Returned list of loaded assemblies</param>
        /// <param name="errorMessage">Error message, if applicable</param>
        /// <returns>True if all assemblies loaded successfully</returns>
        public static bool TryLoadAssemblies(IEnumerable<string> assemblyPaths, out IEnumerable<Assembly> loadedAssemblies, out string errorMessage)
        {
            errorMessage = string.Empty;
            var sbErrorMessage = new StringBuilder();
            loadedAssemblies = null;

            var tmpAssemblies = new List<Assembly>();
            foreach (var path in assemblyPaths)
            {
                Assembly assembly;
                try
                {
                    var data = File.ReadAllBytes(path);
                    assembly = Assembly.Load(data);
                }
                catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException ||
                                           ex is NotSupportedException || ex is BadImageFormatException ||
                                           ex is SecurityException)
                {
                    sbErrorMessage.AppendLine($"Unable to load assembly from path: {path}. Error: {ex.Message}");
                    continue;
                }

                tmpAssemblies.Add(assembly);
            }

            errorMessage = sbErrorMessage.ToString();
            if (!errorMessage.IsNullOrEmpty())
                return false;

            loadedAssemblies = tmpAssemblies;
            return true;
        }
    }
}