using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DataFusionSharp.Interop;
using Microsoft.Extensions.Logging;

namespace DataFusionSharp;

/// <summary>
/// Provides logging capabilities for the native DataFusion library.
/// </summary>
public static partial class DataFusionNativeLogger
{
    private static ILogger? _logger;

    static DataFusionNativeLogger()
    {
        unsafe
        {
            _ = NativeMethods.SetLogger(&LogCallback);
        }
    }

    /// <summary>
    /// Configures the logger for the native DataFusion library.
    /// This method must be called before any other DataFusion operations to ensure that native log messages are captured and forwarded to the provided logger.
    /// </summary>
    /// <remarks>
    /// This method should be called once during application initialization.
    /// </remarks>
    /// <param name="logger">The logger instance to which native log messages will be forwarded.</param>
    /// <param name="minLevel">The minimum log level for messages to be forwarded. Messages below this level will be ignored. Default is <see cref="LogLevel.Information"/>.</param>
    /// <exception cref="DataFusionException">Thrown when configuring the native logger fails.</exception>
    public static void ConfigureLogger(ILogger logger, LogLevel minLevel = LogLevel.Error)
    {
        _logger = logger;

        SetLogLevel(minLevel);
    }

    /// <summary>
    /// Sets the minimum log level for the native DataFusion library.
    /// This can be called at any time to change the log level without re-registering the logger.
    /// </summary>
    /// <param name="minLevel">The minimum log level for messages to be forwarded.</param>
    /// <exception cref="DataFusionException">Thrown when setting the log level fails.</exception>
    public static void SetLogLevel(LogLevel minLevel)
    {
        var nativeLogLevel = minLevel switch
        {
            LogLevel.Trace => NativeMethods.NativeLogLevel.Trace,
            LogLevel.Debug => NativeMethods.NativeLogLevel.Debug,
            LogLevel.Information => NativeMethods.NativeLogLevel.Info,
            LogLevel.Warning => NativeMethods.NativeLogLevel.Warn,
            LogLevel.Error => NativeMethods.NativeLogLevel.Error,
            _ => NativeMethods.NativeLogLevel.None
        };

        var errorCode = NativeMethods.SetLogLevel(nativeLogLevel);
        DataFusionException.ThrowIfError(errorCode, "Failed to set native log level");
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void LogCallback(NativeMethods.NativeLogLevel level, BytesData targetBytes, BytesData messageBytes)
    {
        var logger = _logger;
        if (logger == null)
            return;

        var logLevel = level switch
        {
            NativeMethods.NativeLogLevel.Error => LogLevel.Error,
            NativeMethods.NativeLogLevel.Warn => LogLevel.Warning,
            NativeMethods.NativeLogLevel.Info => LogLevel.Information,
            NativeMethods.NativeLogLevel.Debug => LogLevel.Debug,
            NativeMethods.NativeLogLevel.Trace => LogLevel.Trace,
            _ => LogLevel.None
        };
        
        if (logLevel == LogLevel.None || !logger.IsEnabled(logLevel))
            return;
        
        var target = targetBytes.ToUtf8String();
        var message = messageBytes.ToUtf8String();

        using var scope = logger.BeginScope(KeyValuePair.Create("NativeTarget", target));
        logger.LogNativeMessage(logLevel, message);
    }

    [LoggerMessage("{message}")]
    private static partial void LogNativeMessage(this ILogger logger, LogLevel logLevel, string message);
}