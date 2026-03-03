// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    /// <summary>
    /// Extension methods for <see cref="IDatabase"/> and <see cref="ITransaction"/>
    /// that provide convenience wrappers around the ETag meta-commands.
    /// </summary>
    public static class StackExchangeRedisExtensions
    {
        /// <summary>
        /// Executes a command with ETag tracking enabled.
        /// The response includes the ETag along with the command result.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A <see cref="RedisResult"/> containing the ETag and command result.</returns>
        public static RedisResult ExecWithEtag(this IDatabase db, string command, params object[] args)
        {
            var fullArgs = new object[] { command }.Concat(args).ToArray();
            return db.Execute("EXECWITHETAG", fullArgs);
        }

        /// <summary>
        /// Asynchronously executes a command with ETag tracking enabled.
        /// The response includes the ETag along with the command result.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the ETag and command result.</returns>
        public static Task<RedisResult> ExecWithEtagAsync(this IDatabase db, string command, params object[] args)
        {
            var fullArgs = new object[] { command }.Concat(args).ToArray();
            return db.ExecuteAsync("EXECWITHETAG", fullArgs);
        }

        /// <summary>
        /// Executes a command only if the provided ETag matches the current ETag of the key.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="etag">The ETag value to match against.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result on mismatch.</returns>
        public static RedisResult ExecIfMatch(this IDatabase db, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return db.Execute("EXECIFMATCH", fullArgs);
        }

        /// <summary>
        /// Asynchronously executes a command only if the provided ETag matches the current ETag of the key.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="etag">The ETag value to match against.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result on mismatch.</returns>
        public static Task<RedisResult> ExecIfMatchAsync(this IDatabase db, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return db.ExecuteAsync("EXECIFMATCH", fullArgs);
        }

        /// <summary>
        /// Executes a command only if the provided ETag does NOT match the current ETag of the key.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="etag">The ETag value that must not match.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result on match.</returns>
        public static RedisResult ExecIfNotMatch(this IDatabase db, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return db.Execute("EXECIFNOTMATCH", fullArgs);
        }

        /// <summary>
        /// Asynchronously executes a command only if the provided ETag does NOT match the current ETag of the key.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="etag">The ETag value that must not match.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result on match.</returns>
        public static Task<RedisResult> ExecIfNotMatchAsync(this IDatabase db, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return db.ExecuteAsync("EXECIFNOTMATCH", fullArgs);
        }

        /// <summary>
        /// Executes a command only if the provided ETag is greater than the current ETag of the key.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="etag">The ETag value that must be greater than the current one.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result if not greater.</returns>
        public static RedisResult ExecIfGreater(this IDatabase db, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return db.Execute("EXECIFGREATER", fullArgs);
        }

        /// <summary>
        /// Asynchronously executes a command only if the provided ETag is greater than the current ETag of the key.
        /// </summary>
        /// <param name="db">The database instance.</param>
        /// <param name="etag">The ETag value that must be greater than the current one.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result if not greater.</returns>
        public static Task<RedisResult> ExecIfGreaterAsync(this IDatabase db, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return db.ExecuteAsync("EXECIFGREATER", fullArgs);
        }

        /// <summary>
        /// Enqueues a command with ETag tracking enabled within a transaction.
        /// </summary>
        /// <param name="tran">The transaction instance.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the ETag and command result.</returns>
        public static Task<RedisResult> ExecWithEtagAsync(this ITransaction tran, string command, params object[] args)
        {
            var fullArgs = new object[] { command }.Concat(args).ToArray();
            return tran.ExecuteAsync("EXECWITHETAG", fullArgs);
        }

        /// <summary>
        /// Enqueues a command within a transaction, to be executed only if the provided ETag matches the current ETag of the key.
        /// </summary>
        /// <param name="tran">The transaction instance.</param>
        /// <param name="etag">The ETag value to match against.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result on mismatch.</returns>
        public static Task<RedisResult> ExecIfMatchAsync(this ITransaction tran, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return tran.ExecuteAsync("EXECIFMATCH", fullArgs);
        }

        /// <summary>
        /// Enqueues a command within a transaction, to be executed only if the provided ETag does NOT match the current ETag of the key.
        /// </summary>
        /// <param name="tran">The transaction instance.</param>
        /// <param name="etag">The ETag value that must not match.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result on match.</returns>
        public static Task<RedisResult> ExecIfNotMatchAsync(this ITransaction tran, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return tran.ExecuteAsync("EXECIFNOTMATCH", fullArgs);
        }

        /// <summary>
        /// Enqueues a command within a transaction, to be executed only if the provided ETag is greater than the current ETag of the key.
        /// </summary>
        /// <param name="tran">The transaction instance.</param>
        /// <param name="etag">The ETag value that must be greater than the current one.</param>
        /// <param name="command">The RESP command to execute.</param>
        /// <param name="args">Arguments for the command.</param>
        /// <returns>A task representing the <see cref="RedisResult"/> containing the updated ETag and command result, or the current ETag with a null result if not greater.</returns>
        public static Task<RedisResult> ExecIfGreaterAsync(this ITransaction tran, long etag, string command, params object[] args)
        {
            var fullArgs = new object[] { etag, command }.Concat(args).ToArray();
            return tran.ExecuteAsync("EXECIFGREATER", fullArgs);
        }
    }
}
