// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Text;
using System.Text.RegularExpressions;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet
{
    /// <summary>
    /// Basic validation logic for Options property
    /// Valid if value is required and has value or if value is not required
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal class OptionValidationAttribute : ValidationAttribute
    {
        /// <summary>
        /// Logger to use for validation
        /// </summary>
        public ILogger Logger { get; set; }

        /// <summary>
        /// Determines if current property is required to have a value
        /// </summary>
        protected readonly bool IsRequired;

        /// <summary>
        /// Determine object default value at runtime
        /// </summary>
        protected static object GetDefault(Type t)
        {
            if (t.IsValueType && Nullable.GetUnderlyingType(t) == null)
                return Activator.CreateInstance(t);
            else
                return null;
        }

        protected static object GetDefault<T>(T t) => default(T);

        internal OptionValidationAttribute(bool isRequired = true)
        {
            IsRequired = isRequired;
        }

        /// <summary>
        /// Checks if current property is valid by checking if value is required and not default or if value is not required
        /// </summary>
        /// <param name="value">Property value to validate</param>
        /// <param name="validationContext">Current validation context</param>
        /// <returns>Validation result</returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (!IsRequired || value != GetDefault(value) || (value is string strVal && !string.IsNullOrEmpty(strVal)))
                return ValidationResult.Success;

            var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
            var errorMessage = $"{baseError} Required value was not specified.";
            return new ValidationResult(errorMessage, [validationContext.MemberName]);
        }

        /// <summary>
        /// Initial validation logic to check if further validation can be skipped -
        /// Valid result if value is not required value is default
        /// Invalid result if value is not of the expected type
        /// If neither conditions are true, further validation is required
        /// </summary>
        /// <typeparam name="T">Type expected by the validator</typeparam>
        /// <param name="value">Value to validate</param>
        /// <param name="validationContext">Validation context</param>
        /// <param name="validationResult">Validation result - only set if initial validation suffices</param>
        /// <param name="convertedValue">Value converted to type T</param>
        /// <returns>True if further validation can be skipped and validation result is set</returns>
        protected bool TryInitialValidation<T>(object value, ValidationContext validationContext, out ValidationResult validationResult, out T convertedValue)
        {
            validationResult = null;
            convertedValue = default;

            if (!IsRequired)
            {
                var isDefaultValue =
                    (value == null && !typeof(T).IsValueType) ||
                    (value?.Equals(GetDefault(typeof(T))) ?? false) ||
                    (value is string strVal && string.IsNullOrEmpty(strVal));

                if (isDefaultValue)
                {
                    validationResult = ValidationResult.Success;
                    return true;
                }
            }

            if (value is not T tValue)
            {
                var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
                var errorMessage = $"{baseError} Invalid type. Expected: {typeof(T)}. Actual: {value?.GetType()}";
                validationResult = new ValidationResult(errorMessage, [validationContext.MemberName]);
                return true;
            }

            convertedValue = tValue;

            return false;
        }
    }

    /// <summary>
    /// Validation logic for path of type string representing a local directory
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal class DirectoryPathValidationAttribute : OptionValidationAttribute
    {
        /// <summary>
        /// Determines if current directory must exist
        /// </summary>
        private readonly bool _mustExist;

        internal DirectoryPathValidationAttribute(bool mustExist, bool isRequired) : base(isRequired)
        {
            this._mustExist = mustExist;
        }

        /// <summary>
        /// Directory validation logic, checks access to directory by instantiating a DirectoryInfo object
        /// Invalid if exception thrown during DirectoryInfo instantiation or if directory must exist and doesn't (not applicable for Azure Storage)
        /// </summary>
        /// <param name="value">Path to the local directory</param>
        /// <param name="validationContext">Validation context</param>
        /// <returns></returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (TryInitialValidation<string>(value, validationContext, out var initValidationResult, out var directoryPath))
                return initValidationResult;

            DirectoryInfo directoryInfo;
            var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;

            try
            {
                directoryInfo = new DirectoryInfo(directoryPath);
            }
            catch (Exception e) when (e is SecurityException or PathTooLongException)
            {
                var errorMessage = $"{baseError} An exception of type {e.GetType()} has occurred while trying to access directory. Directory path: {directoryPath}.";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            var options = (Options)validationContext.ObjectInstance;

            // Ignore directory exists if pertains to path on Azure Storage
            if ((validationContext.MemberName == nameof(Options.ConfigImportPath) && options.UseAzureStorageForConfigImport.GetValueOrDefault()) ||
                (validationContext.MemberName == nameof(Options.ConfigExportPath) && options.UseAzureStorageForConfigExport.GetValueOrDefault()) ||
                (validationContext.MemberName != nameof(Options.ConfigImportPath) && validationContext.MemberName != nameof(Options.ConfigExportPath) && options.GetDeviceType() == Tsavorite.core.DeviceType.AzureStorage))
                return ValidationResult.Success;

            if (this._mustExist && !directoryInfo.Exists)
            {
                var errorMessage = $"{baseError} Specified directory does not exist. Directory path: {directoryPath}.";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            return ValidationResult.Success;
        }
    }

    /// <summary>
    /// Validation logic for multiple paths of type string representing local directories
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class DirectoryPathsValidationAttribute : OptionValidationAttribute
    {
        /// <summary>
        /// Determines if all directories must exist
        /// </summary>
        private readonly bool _mustExist;

        internal DirectoryPathsValidationAttribute(bool mustExist, bool isRequired) : base(isRequired)
        {
            this._mustExist = mustExist;
        }

        /// <summary>
        /// Directories validation logic, calls DirectoryPathValidationAttribute for each directory
        /// </summary>
        /// <param name="value">Paths to the local directories</param>
        /// <param name="validationContext">Validation context</param>
        /// <returns></returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (TryInitialValidation<IEnumerable<string>>(value, validationContext, out var initValidationResult, out var directoryPaths))
                return initValidationResult;

            var errorSb = new StringBuilder();
            var isValid = true;
            var directoryValidator = new DirectoryPathValidationAttribute(this._mustExist, this.IsRequired);
            foreach (var directoryPath in directoryPaths)
            {
                var result = directoryValidator.GetValidationResult(directoryPath, validationContext);
                if (result != null && result != ValidationResult.Success)
                {
                    isValid = false;
                    errorSb.AppendLine(result.ErrorMessage);
                }
            }

            if (!isValid)
            {
                var errorMessage = $"Error(s) validating one or more directories:{Environment.NewLine}{errorSb}";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            return ValidationResult.Success;
        }
    }

    /// <summary>
    /// Validation logic for path of type string representing a local file
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal class FilePathValidationAttribute : OptionValidationAttribute
    {
        /// <summary>
        /// Determines if current file must exist
        /// </summary>
        private readonly bool _fileMustExist;
        /// <summary>
        /// Determines if current directory must exist
        /// </summary>
        private readonly bool _directoryMustExist;

        /// <summary>
        /// Determines which file extensions are expected for this file
        /// </summary>
        private readonly string[] _acceptedFileExtensions;

        internal FilePathValidationAttribute(bool fileMustExist, bool directoryMustExist, bool isRequired, string[] acceptedFileExtensions = null) : base(isRequired)
        {
            this._fileMustExist = fileMustExist;
            this._directoryMustExist = directoryMustExist;
            this._acceptedFileExtensions = acceptedFileExtensions;
        }

        /// <summary>
        /// File validation logic, checks access to file by instantiating a FileInfo object
        /// Invalid if exception thrown during FileInfo instantiation or if directory must exist and doesn't or if file must exist and doesn't (not applicable for Azure Storage)
        /// </summary>
        /// <param name="value">Path to the local file</param>
        /// <param name="validationContext">Validation context</param>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (TryInitialValidation<string>(value, validationContext, out var initValidationResult, out var filePath))
                return initValidationResult;

            FileInfo fileInfo;
            var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;

            try
            {
                fileInfo = new FileInfo(filePath);
            }
            catch (Exception e) when (e is SecurityException or UnauthorizedAccessException or NotSupportedException or PathTooLongException)
            {
                var errorMessage = $"{baseError} An exception of type {e.GetType()} has occurred while trying to access file. File path: {filePath}.";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            var options = (Options)validationContext.ObjectInstance;

            // Ignore file exists / directory exists if pertains to path on Azure Storage
            if ((validationContext.MemberName == nameof(Options.ConfigImportPath) && options.UseAzureStorageForConfigImport.GetValueOrDefault()) ||
                (validationContext.MemberName == nameof(Options.ConfigExportPath) && options.UseAzureStorageForConfigExport.GetValueOrDefault()) ||
                (validationContext.MemberName != nameof(Options.ConfigImportPath) && validationContext.MemberName != nameof(Options.ConfigExportPath) && options.GetDeviceType() == Tsavorite.core.DeviceType.AzureStorage))
                return ValidationResult.Success;

            if (this._fileMustExist && !fileInfo.Exists)
            {
                var errorMessage = $"{baseError} Specified file does not exist. File path: {filePath}.";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            if (this._directoryMustExist && (fileInfo.Directory == null || !fileInfo.Directory.Exists))
            {
                var errorMessage = $"{baseError} Directory containing specified file does not exist. File path: {filePath}.";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            if (this._acceptedFileExtensions != null && !this._acceptedFileExtensions.Any(filePath.EndsWith))
            {
                var errorMessage =
                    $"{baseError} Unexpected extension for specified file. Expected: {string.Join(" / ", this._acceptedFileExtensions)}.";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            return ValidationResult.Success;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    internal class ModuleFilePathValidationAttribute : FilePathValidationAttribute
    {
        internal ModuleFilePathValidationAttribute(bool fileMustExist, bool directoryMustExist, bool isRequired, string[] acceptedFileExtensions = null) : base(fileMustExist, directoryMustExist, isRequired, acceptedFileExtensions)
        {
        }

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (TryInitialValidation<IEnumerable<string>>(value, validationContext, out var initValidationResult, out var filePaths))
                return initValidationResult;

            var errorSb = new StringBuilder();
            var isValid = true;
            foreach (var filePathArg in filePaths)
            {
                var filePath = filePathArg.Split(' ')[0];
                var result = base.IsValid(filePath, validationContext);
                if (result != null && result != ValidationResult.Success)
                {
                    isValid = false;
                    errorSb.AppendLine(result.ErrorMessage);
                }
            }

            if (!isValid)
            {
                var errorMessage = $"Error(s) validating one or more file paths:{Environment.NewLine}{errorSb}";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            return ValidationResult.Success;
        }
    }

    /// <summary>
    /// Validation logic for a string representing an IP address (either IPv4 or IPv6)
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class IpAddressValidationAttribute : OptionValidationAttribute
    {
        private const string Localhost = "localhost";

        internal IpAddressValidationAttribute(bool isRequired = true) : base(isRequired)
        {
        }

        /// <summary>
        /// IP validation logic, checks if string matches either IPv4 or IPv6 regex patterns
        /// </summary>
        /// <param name="value">String containing IP address</param>
        /// <param name="validationContext">Validation Logic</param>
        /// <returns></returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (TryInitialValidation<string>(value, validationContext, out var initValidationResult, out var ipAddresses))
                return initValidationResult;

            var logger = ((Options)validationContext.ObjectInstance).runtimeLogger;
            if (!Format.TryParseAddressList(ipAddresses, 0, out _, out var errorHostnameOrAddress, logger: logger))
            {
                var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
                var errorMessage = $"{baseError} Expected string in IPv4 / IPv6 format (e.g. 127.0.0.1 / 0:0:0:0:0:0:0:1) or 'localhost' or valid hostname. Actual value: {errorHostnameOrAddress}";
                return new ValidationResult(errorMessage, [validationContext.MemberName]);
            }

            return ValidationResult.Success;
        }
    }

    /// <summary>
    /// Validation logic for a string representing a memory size (1k, 1kb, 5M, 5Mb, 10g, 10GB etc.)
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class MemorySizeValidationAttribute : OptionValidationAttribute
    {
        private const string MemorySizePattern = @"^\d+([K|k|M|m|G|g][B|b]{0,1})?$";

        internal MemorySizeValidationAttribute(bool isRequired = true) : base(isRequired)
        {
        }

        /// <summary>
        /// Memory size validation logic, checks if string matches memory size regex pattern
        /// </summary>
        /// <param name="value">String containing memory size</param>
        /// <param name="validationContext">Validation context</param>
        /// <returns></returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (TryInitialValidation<string>(value, validationContext, out var initValidationResult, out var memorySize))
                return initValidationResult;

            if (Regex.IsMatch(memorySize, MemorySizePattern))
                return ValidationResult.Success;

            var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
            var errorMessage = $"{baseError} Expected string in memory size format (e.g. 1k, 1kb, 10m, 10mb, 50g, 50gb etc). Actual value: {memorySize}";
            return new ValidationResult(errorMessage, [validationContext.MemberName]);
        }
    }

    /// <summary>
    /// Validation logic for an integer representing a percentage (range between 0 and 100)
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class PercentageValidationAttribute : RangeValidationAttribute
    {
        internal PercentageValidationAttribute(bool isRequired = true) : base(typeof(int), 0, 100, true, true, isRequired)
        {
        }
    }

    /// <summary>
    /// Validation logic for an object of specified type that implements IComparable, checks if value is contained in a specified range
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal class RangeValidationAttribute : OptionValidationAttribute
    {
        // Type of min, max and value to validate
        private readonly Type _rangeType;
        // Range minimum
        private readonly object _min;
        // Range maximum
        private readonly object _max;
        // True if range includes minimum value
        private readonly bool _includeMin;
        // True if range includes maximum value
        private readonly bool _includeMax;

        private readonly MethodInfo _tryInitValidationMethod;

        internal RangeValidationAttribute(Type rangeType, object min, object max, bool includeMin = true, bool includeMax = true, bool isRequired = true) : base(isRequired)
        {
            var icType = typeof(IComparable<>).MakeGenericType(rangeType);

            if (!rangeType.IsAssignableTo(icType))
                throw new ArgumentException($"rangeType parameter is not assignable to {icType}", nameof(rangeType));
            if (min.GetType() != rangeType)
                throw new ArgumentException($"min parameter is not of type specified by rangeType", nameof(min));
            if (max.GetType() != rangeType)
                throw new ArgumentException($"max parameter is not of type specified by rangeType", nameof(includeMax));

            this._rangeType = rangeType;
            this._min = min;
            this._includeMin = includeMin;
            this._max = max;
            this._includeMax = includeMax;

            this._tryInitValidationMethod = this.GetType().GetMethod(nameof(TryInitialValidation), BindingFlags.Instance | BindingFlags.NonPublic)
                ?.MakeGenericMethod(this._rangeType);
        }

        /// <summary>
        /// Integer validation logic, valid if integer is contained in a specified range
        /// </summary>
        /// <param name="value">Integer value</param>
        /// <param name="validationContext">Validation context</param>
        /// <returns></returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var initValParams = new[] { value, validationContext, null, null };
            if ((bool)this._tryInitValidationMethod.Invoke(this, initValParams)!)
            {
                return initValParams[2] as ValidationResult;
            }

            var icVal = value as IComparable;
            var minComp = icVal.CompareTo(this._min);
            var maxComp = icVal.CompareTo(this._max);
            if ((minComp > 0 || (this._includeMin && minComp == 0)) &&
                (maxComp < 0 || (this._includeMax && maxComp == 0)))
            {
                return ValidationResult.Success;
            }

            var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
            var errorMessage = $"{baseError} Expected to be in range {(this._includeMin ? "[" : "(")}{this._min}, {this._max}{(this._includeMax ? "]" : ")")}. Actual value: {value}";
            return new ValidationResult(errorMessage, [validationContext.MemberName]);
        }
    }

    /// <summary>
    /// Validation logic for an integer, checks if integer is contained in a specified range
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class IntRangeValidationAttribute : RangeValidationAttribute
    {
        internal IntRangeValidationAttribute(int min, int max, bool includeMin = true, bool includeMax = true,
            bool isRequired = true) : base(typeof(int), min, max, includeMin, includeMax, isRequired)
        {
        }
    }

    /// <summary>
    /// Validation logic for an double, checks if double is contained in a specified range
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class DoubleRangeValidationAttribute : RangeValidationAttribute
    {
        internal DoubleRangeValidationAttribute(double min, double max, bool includeMin = true, bool includeMax = true,
            bool isRequired = true) : base(typeof(double), min, max, includeMin, includeMax, isRequired)
        {
        }
    }

    /// <summary>
    /// Validation logic for Log Directory
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal class LogDirValidationAttribute : DirectoryPathValidationAttribute
    {
        internal LogDirValidationAttribute(bool mustExist, bool isRequired) : base(mustExist, isRequired)
        {
        }

        /// <summary>
        /// Validation logic for Log Directory, valid if <see cref="Options.DeviceType"/> is AzureStorage or if <see cref="Options.EnableStorageTier"/> is not specified in parent Options object
        /// If neither applies, reverts to <see cref="OptionValidationAttribute"/> validation
        /// </summary>
        /// <param name="value">Value of Log Directory</param>
        /// <param name="validationContext">Validation context</param>
        /// <returns>Validation result</returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var options = (Options)validationContext.ObjectInstance;
            if (options.GetDeviceType() == Tsavorite.core.DeviceType.AzureStorage || !options.EnableStorageTier.GetValueOrDefault())
                return ValidationResult.Success;

            return base.IsValid(value, validationContext);
        }
    }

    /// <summary>
    /// Validation logic for Checkpoint Directory
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class CheckpointDirValidationAttribute : DirectoryPathValidationAttribute
    {
        internal CheckpointDirValidationAttribute(bool mustExist, bool isRequired) : base(mustExist, isRequired)
        {
        }

        /// <summary>
        /// Validation logic for <see cref="Options.CheckpointDir"/>, valid if <see cref="Options.DeviceType"/> is AzureStorage in parent Options object
        /// If not, reverts to <see cref="OptionValidationAttribute"/> validation
        /// </summary>
        /// <param name="value">Value of Log Directory</param>
        /// <param name="validationContext">Validation context</param>
        /// <returns>Validation result</returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var options = (Options)validationContext.ObjectInstance;
            if (options.GetDeviceType() == Tsavorite.core.DeviceType.AzureStorage)
                return ValidationResult.Success;

            return base.IsValid(value, validationContext);
        }
    }

    /// <summary>
    /// Validation logic for <see cref="Options.CertFileName"/>
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class CertFileValidationAttribute : FilePathValidationAttribute
    {
        internal CertFileValidationAttribute(bool fileMustExist, bool directoryMustExist, bool isRequired) : base(
            fileMustExist, directoryMustExist, isRequired, [".pfx"])
        {
        }

        /// <summary>
        /// Validation logic for CertFileName, valid if EnableTLS is false in parent Options object
        /// If not, reverts to FilePathValidationAttribute validation
        /// </summary>
        /// <param name="value">Value of CertFileName</param>
        /// <param name="validationContext">Validation context</param>
        /// <returns>Validation result</returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var options = (Options)validationContext.ObjectInstance;
            if (!options.EnableTLS.GetValueOrDefault())
                return ValidationResult.Success;

            return base.IsValid(value, validationContext);
        }
    }

    /// <summary>
    /// Forbids a config option from being set if the another option has particular values.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class ForbiddenWithOptionAttribute : ValidationAttribute
    {
        private readonly string otherOptionName;
        private readonly string[] forbiddenValues;

        internal ForbiddenWithOptionAttribute(string otherOptionName, string forbiddenValue, params string[] otherForbiddenValues)
        {
            this.otherOptionName = otherOptionName;
            forbiddenValues = [forbiddenValue, .. otherForbiddenValues];
        }

        /// <inheritdoc/>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var optionIsSet = value != null && !(value is string valueStr && string.IsNullOrEmpty(valueStr));
            if (optionIsSet)
            {
                var propAccessor = validationContext.ObjectInstance?.GetType()?.GetProperty(otherOptionName, BindingFlags.Instance | BindingFlags.Public);
                if (propAccessor != null)
                {
                    var otherOptionValue = propAccessor.GetValue(validationContext.ObjectInstance);
                    var otherOptionValueAsString = otherOptionValue is string strVal ? strVal : otherOptionValue?.ToString();

                    if (forbiddenValues.Contains(otherOptionValueAsString, StringComparer.OrdinalIgnoreCase))
                    {
                        return new ValidationResult($"{validationContext.DisplayName} cannot be set with {otherOptionName} has value '{otherOptionValueAsString}'");
                    }
                }
            }

            return ValidationResult.Success;
        }
    }

    /// <summary>
    /// Validate that, when annotated property is set, another option has a least a minimum memory value.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class RequiresMinimumMemory : OptionValidationAttribute
    {
        private readonly string otherOptionName;
        private readonly string minimumValue;
        private readonly long minimumValueBytes;

        internal RequiresMinimumMemory(string otherOptionName, string minimumValue)
        {
            this.otherOptionName = otherOptionName;
            this.minimumValue = minimumValue;

            minimumValueBytes = GarnetServerOptions.ParseSize(this.minimumValue, out var readBytes);
            if (readBytes != minimumValue.Length)
            {
                // If we can't parse config, disable validation
                minimumValueBytes = long.MinValue;
            }
        }

        /// <inheritdoc/>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var optionIsSet = value is bool valueBool && valueBool;
            if (optionIsSet)
            {
                var propAccessor = validationContext.ObjectInstance?.GetType()?.GetProperty(otherOptionName, BindingFlags.Instance | BindingFlags.Public);
                if (propAccessor != null)
                {
                    var otherOptionValue = propAccessor.GetValue(validationContext.ObjectInstance);
                    var otherOptionValueAsString = (otherOptionValue is string strVal ? strVal : otherOptionValue?.ToString())?.Trim();

                    var otherOptionValueBytes = GarnetServerOptions.ParseSize(otherOptionValueAsString, out var readBytes);
                    if (readBytes == otherOptionValueAsString.Length && otherOptionValueBytes < minimumValueBytes)
                    {
                        return new ValidationResult($"{validationContext.DisplayName} requires {otherOptionName} be at least '{minimumValue}'");
                    }
                }
            }

            return ValidationResult.Success;
        }
    }

    /// <summary>
    /// Forbids a config option from being set if the current OS platform is not supported.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class SupportedOSValidationAttribute : OptionValidationAttribute
    {
        private readonly string[] supportedPlatforms;

        internal SupportedOSValidationAttribute(bool isRequired, params string[] supportedPlatforms) : base(isRequired)
        {
            this.supportedPlatforms = supportedPlatforms;
        }

        /// <inheritdoc/>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (!IsRequired && (Equals(value, GetDefault(value.GetType())) || (value is string strVal && string.IsNullOrEmpty(strVal))))
                return ValidationResult.Success;

            foreach (var platform in supportedPlatforms)
            {
                if (OperatingSystem.IsOSPlatform(platform))
                    return ValidationResult.Success;
            }

            return new ValidationResult($"{validationContext.DisplayName} can only bet set on following platforms: {string.Join(',', supportedPlatforms)}");
        }
    }

    /// <summary>
    /// Represents an attribute used for validating HTTPS URLs as options.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class HttpsUrlValidationAttribute : OptionValidationAttribute
    {
        internal HttpsUrlValidationAttribute(bool isRequired = false) : base(isRequired)
        {
        }

        /// <summary>
        /// HTTPS URLs validation logic, checks if string is a valid HTTPS URL.
        /// </summary>
        /// <param name="value">URL string</param>
        /// <param name="validationContext">Validation Logic</param>
        /// <returns>Validation result</returns>
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (TryInitialValidation<string>(value, validationContext, out var initValidationResult, out var url))
                return initValidationResult;

            if (Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme == Uri.UriSchemeHttps)
                return ValidationResult.Success;

            var baseError = validationContext.MemberName != null ? base.FormatErrorMessage(validationContext.MemberName) : string.Empty;
            var errorMessage = $"{baseError} Expected string in URI format. Actual value: {url}";
            return new ValidationResult(errorMessage, [validationContext.MemberName]);
        }
    }
}