using DataFusionSharp.Interop;
using Microsoft.Extensions.Logging;

namespace DataFusionSharp;

/// <summary>
/// Provides logging capabilities for the native DataFusion library.
/// </summary>
public static partial class DataFusionNativeLogger
{
    private static ILogger? _logger;
    
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
    public static void ConfigureLogger(ILogger logger, LogLevel minLevel = LogLevel.Information)
    {
        _logger = logger;
        
        var nativeLogLevel = minLevel switch
        {
            LogLevel.Trace => NativeMethods.LogLevel.Trace,
            LogLevel.Debug => NativeMethods.LogLevel.Debug,
            LogLevel.Information => NativeMethods.LogLevel.Info,
            LogLevel.Warning => NativeMethods.LogLevel.Warn,
            LogLevel.Error => NativeMethods.LogLevel.Error,
            _ => NativeMethods.LogLevel.None
        };
        
        var errorCode = NativeMethods.ConfigureLogger(LogCallbackHandle, nativeLogLevel);
        DataFusionException.ThrowIfError(errorCode, "Failed to configure native logger");
    }
    
    private static void LogCallback(NativeMethods.LogLevel level, BytesData targetBytes, BytesData messageBytes)
    {
        var logger = _logger;
        if (logger == null)
            return;

        var logLevel = level switch
        {
            NativeMethods.LogLevel.Error => LogLevel.Error,
            NativeMethods.LogLevel.Warn => LogLevel.Warning,
            NativeMethods.LogLevel.Info => LogLevel.Information,
            NativeMethods.LogLevel.Debug => LogLevel.Debug,
            NativeMethods.LogLevel.Trace => LogLevel.Trace,
            _ => LogLevel.None
        };
        
        if (logLevel == LogLevel.None || !logger.IsEnabled(logLevel))
            return;
        
        var target = targetBytes.ToUtf8String();
        var message = messageBytes.ToUtf8String();

        using var scope = logger.BeginScope(KeyValuePair.Create("NativeTarget", target));
        logger.LogNativeMessage(logLevel, message);
    }
    private static readonly NativeMethods.LogCallback LogCallbackDelegate = LogCallback;
    private static readonly IntPtr LogCallbackHandle = System.Runtime.InteropServices.Marshal.GetFunctionPointerForDelegate(LogCallbackDelegate);

    [LoggerMessage("{message}")]
    private static partial void LogNativeMessage(this ILogger logger, LogLevel logLevel, string message);
}