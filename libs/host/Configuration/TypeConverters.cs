// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Globalization;
using Microsoft.Extensions.Logging;

namespace Garnet
{
    /// <summary>
    /// Custom converter that converts between a RedisBoolean and a nullable boolean, boolean or string
    /// </summary>
    internal class RedisBooleanTypeConverter : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return sourceType == typeof(string) || sourceType == typeof(bool) || sourceType == typeof(bool?);
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return destinationType == typeof(string) || destinationType == typeof(bool) || destinationType == typeof(bool?);
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            if (value != null && (value is not bool && value is not string))
                throw new NotSupportedException();

            switch (value)
            {
                case bool boolVal:
                    return boolVal ? RedisBoolean.Yes : RedisBoolean.No;
                case string strVal:
                    return Enum.Parse(typeof(RedisBoolean), strVal, true);
                case null:
                    return RedisBoolean.No;
                default:
                    throw new NotImplementedException();
            }
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (value is not RedisBoolean rbValue)
                throw new NotSupportedException();

            if (destinationType == typeof(bool) || destinationType == typeof(bool?))
            {
                return rbValue == RedisBoolean.Yes;
            }

            if (destinationType == typeof(string))
            {
                return rbValue.ToString().ToLower();
            }

            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Custom converter that converts between a RedisLogLevel and a Microsoft.Extensions.Logging.LogLevel or string
    /// </summary>
    internal class RedisLogLevelTypeConverter : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return sourceType == typeof(string) || sourceType == typeof(LogLevel);
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return destinationType == typeof(string) || destinationType == typeof(LogLevel);
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            if (value is LogLevel logLevel)
            {
                switch (logLevel)
                {
                    case LogLevel.Trace:
                        return RedisLogLevel.Verbose;
                    case LogLevel.Debug:
                        return RedisLogLevel.Debug;
                    case LogLevel.Information:
                        return RedisLogLevel.Notice;
                    case LogLevel.Warning:
                        return RedisLogLevel.Warning;
                    case LogLevel.Error:
                        return RedisLogLevel.Warning;
                    case LogLevel.Critical:
                        return RedisLogLevel.Warning;
                    case LogLevel.None:
                        return RedisLogLevel.Nothing;
                }
            }
            else if (value is string strVal)
            {
                return Enum.Parse(typeof(RedisLogLevel), strVal, true);
            }

            throw new NotSupportedException();
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (value is not RedisLogLevel rbValue) return null;

            if (destinationType == typeof(LogLevel))
            {
                switch (value)
                {
                    case RedisLogLevel.Verbose:
                        return LogLevel.Trace;
                    case RedisLogLevel.Debug:
                        return LogLevel.Debug;
                    case RedisLogLevel.Notice:
                        return LogLevel.Information;
                    case RedisLogLevel.Warning:
                        return LogLevel.Warning;
                    case RedisLogLevel.Nothing:
                        return LogLevel.None;
                    default:
                        throw new NotImplementedException();
                }
            }

            if (destinationType == typeof(string))
            {
                return rbValue.ToString().ToLower();
            }

            throw new NotSupportedException();
        }
    }

    /// <summary>
    /// Custom converter that converts between a RedisTlsAuthClients enum and a nullable boolean, boolean or string
    /// </summary>
    internal class RedisTlsClientsTypeConverter : TypeConverter
    {
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return sourceType == typeof(string) || sourceType == typeof(bool) || sourceType == typeof(bool?);
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return destinationType == typeof(string) || destinationType == typeof(bool) || destinationType == typeof(bool?);
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            switch (value)
            {
                case bool boolVal:
                    return boolVal ? RedisTlsAuthClients.Yes : RedisTlsAuthClients.Optional;
                case string strVal:
                    return Enum.Parse(typeof(RedisTlsAuthClients), strVal, true);
                case null:
                    return RedisTlsAuthClients.Optional;
                default:
                    throw new NotSupportedException();
            }
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (value is not RedisTlsAuthClients rtacValue) return null;

            if (destinationType == typeof(bool) || destinationType == typeof(bool?))
            {
                return rtacValue == RedisTlsAuthClients.Yes;
            }

            if (destinationType == typeof(string))
            {
                return rtacValue.ToString().ToLower();
            }

            throw new NotSupportedException();
        }
    }
}