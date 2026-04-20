using System.Globalization;
using System.Text;
using Meziantou.Extensions.Logging.Xunit;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

#pragma warning disable CA2008

namespace DataFusionSharp.Tests;

public sealed class CancellationTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();
    
    [Fact(Timeout = 1000)]
    public async Task TokenCancel_CancelsInFlightOperation()
    {
        // Arrange
        var logOutput = new LogOutput();
        DataFusionNativeLogger.ConfigureLogger(XUnitLogger.CreateLogger<CancellationTests>(logOutput), LogLevel.Debug);
        
        using var cts = new CancellationTokenSource();
        
        // Act
        var pingTask = _runtime.PingAsync(TimeSpan.FromMilliseconds(1000), cts.Token);
        var cancellationTask = Task.Delay(TimeSpan.FromMilliseconds(100)).ContinueWith(_ => cts.Cancel());
        
        // Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() => Task.WhenAll(pingTask, cancellationTask));
        
        Assert.True(pingTask.IsCanceled);
        Assert.True(cancellationTask.IsCompletedSuccessfully);
        
        Assert.Contains("Ping cancelled for user_data=", logOutput.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("Ping completed for user_data=", logOutput.Text, StringComparison.Ordinal);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }

    private sealed class LogOutput : ITestOutputHelper
    {
        private readonly StringBuilder _logBuilder = new();
        
        public string Text => _logBuilder.ToString();
        
        public void WriteLine(string message)
        {
            _logBuilder.AppendLine(message);
        }

        public void WriteLine(string format, params object[] args)
        {
            _logBuilder.AppendFormat(CultureInfo.InvariantCulture, format, args);
            _logBuilder.AppendLine();
        }
    }
}
