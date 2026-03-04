// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class FileLoggerOutputTests : AllureTestBase
    {
        private string logFilePath;

        [SetUp]
        public void Setup()
        {
            logFilePath = Path.Combine(TestUtils.MethodTestDir, "test.log");
            Directory.CreateDirectory(TestUtils.MethodTestDir);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        public void LogMessageWithNewlinesIsSanitized()
        {
            // Verify that embedded newlines in log messages are escaped to prevent log injection
            using (var loggerOutput = new FileLoggerOutput(logFilePath))
            {
                loggerOutput.Log(
                    LogLevel.Information,
                    new EventId(0),
                    "Normal message\nInjected line\r\nAnother injection",
                    null,
                    (state, ex) => state.ToString(),
                    "TestCategory");
            }

            var logContent = File.ReadAllText(logFilePath);
            var logLines = logContent.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

            // Should be exactly one log line (no injected lines)
            ClassicAssert.AreEqual(1, logLines.Length,
                "Log injection: embedded newlines in message should not create additional log lines");

            // Verify the newlines are escaped in the output
            ClassicAssert.IsTrue(logContent.Contains("\\n"), "Newline characters should be escaped as \\n");
            ClassicAssert.IsTrue(logContent.Contains("\\r\\n"), "CRLF characters should be escaped as \\r\\n");
        }

        [Test]
        public void LogMessageWithExceptionPreservesNewlines()
        {
            // Verify that exception stack traces still contain real newlines
            var exception = new InvalidOperationException("Test exception");

            using (var loggerOutput = new FileLoggerOutput(logFilePath))
            {
                loggerOutput.Log(
                    LogLevel.Error,
                    new EventId(0),
                    "Error occurred",
                    exception,
                    (state, ex) => state.ToString(),
                    "TestCategory");
            }

            var logContent = File.ReadAllText(logFilePath);

            // The exception should be present with real newlines (not escaped)
            ClassicAssert.IsTrue(logContent.Contains("Test exception"),
                "Exception message should be present in log output");
            ClassicAssert.IsTrue(logContent.Contains(Environment.NewLine),
                "Exception stack trace should contain real newlines");
        }

        [Test]
        public void LogMessageWithoutNewlinesIsUnchanged()
        {
            // Verify that normal messages without newlines are not modified
            using (var loggerOutput = new FileLoggerOutput(logFilePath))
            {
                loggerOutput.Log(
                    LogLevel.Information,
                    new EventId(0),
                    "Normal log message without special characters",
                    null,
                    (state, ex) => state.ToString(),
                    "TestCategory");
            }

            var logContent = File.ReadAllText(logFilePath);

            ClassicAssert.IsTrue(logContent.Contains("Normal log message without special characters"),
                "Normal messages should be preserved as-is");
        }
    }
}
