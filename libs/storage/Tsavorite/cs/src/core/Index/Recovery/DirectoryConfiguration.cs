// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Tsavorite.core
{
    class DirectoryConfiguration
    {
        internal readonly string checkpointDir;

        public DirectoryConfiguration(string checkpointDir)
        {
            this.checkpointDir = checkpointDir;
        }

        public const string index_base_folder = "index-checkpoints";
        public const string index_meta_file = "info";
        public const string hash_table_file = "ht";
        public const string overflow_buckets_file = "ofb";
        public const string snapshot_file = "snapshot";
        public const string delta_file = "delta";

        public const string cpr_base_folder = "cpr-checkpoints";
        public const string cpr_meta_file = "info";

        public void CreateIndexCheckpointFolder(Guid token)
        {
            var directory = GetIndexCheckpointFolder(token);
            Directory.CreateDirectory(directory);
            DirectoryInfo directoryInfo = new(directory);
            foreach (FileInfo file in directoryInfo.GetFiles())
                file.Delete();
        }
        public void CreateHybridLogCheckpointFolder(Guid token)
        {
            var directory = GetHybridLogCheckpointFolder(token);
            Directory.CreateDirectory(directory);
            DirectoryInfo directoryInfo = new(directory);
            foreach (FileInfo file in directoryInfo.GetFiles())
                file.Delete();
        }

        public string GetIndexCheckpointFolder(Guid token = default(Guid))
        {
            if (token != default(Guid))
                return GetMergedFolderPath(checkpointDir, index_base_folder, token.ToString());
            else
                return GetMergedFolderPath(checkpointDir, index_base_folder);
        }

        public string GetHybridLogCheckpointFolder(Guid token = default(Guid))
        {
            if (token != default(Guid))
                return GetMergedFolderPath(checkpointDir, cpr_base_folder, token.ToString());
            else
                return GetMergedFolderPath(checkpointDir, cpr_base_folder);
        }

        public string GetIndexCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    index_base_folder,
                                    token.ToString(),
                                    index_meta_file,
                                    ".dat");
        }

        public string GetPrimaryHashTableFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    index_base_folder,
                                    token.ToString(),
                                    hash_table_file,
                                    ".dat");
        }

        public string GetOverflowBucketsFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    index_base_folder,
                                    token.ToString(),
                                    overflow_buckets_file,
                                    ".dat");
        }

        public string GetHybridLogCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    cpr_base_folder,
                                    token.ToString(),
                                    cpr_meta_file,
                                    ".dat");
        }

        public string GetHybridLogCheckpointContextFileName(Guid checkpointToken, Guid sessionToken)
        {
            return GetMergedFolderPath(checkpointDir,
                                    cpr_base_folder,
                                    checkpointToken.ToString(),
                                    sessionToken.ToString(),
                                    ".dat");
        }

        public string GetLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir, cpr_base_folder, token.ToString(), snapshot_file, ".dat");
        }

        public string GetObjectLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir, cpr_base_folder, token.ToString(), snapshot_file, ".obj.dat");
        }

        public string GetDeltaLogFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir, cpr_base_folder, token.ToString(), delta_file, ".dat");
        }

        private static string GetMergedFolderPath(params string[] paths)
        {
            string fullPath = paths[0];

            for (int i = 1; i < paths.Length; i++)
            {
                if (i == paths.Length - 1 && paths[i].Contains("."))
                {
                    fullPath += paths[i];
                }
                else
                {
                    fullPath += Path.DirectorySeparatorChar + paths[i];
                }
            }

            return fullPath;
        }
    }
}