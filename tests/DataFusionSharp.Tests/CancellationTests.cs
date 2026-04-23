using System.Globalization;
using System.Text;
using Meziantou.Extensions.Logging.Xunit;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

#pragma warning disable CA2008

namespace DataFusionSharp.Tests;

public sealed class CancellationTests(ITestOutputHelper outputHelper) : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();
    
    [Theory(Timeout = 30_000)]
    [InlineData(0)]
    [InlineData(10)]
    [InlineData(25)]
    [InlineData(50)]
    [InlineData(100)]
    public async Task TokenCancel_DuringInFlightOperation_ThrowsCancellationException(double cancellationDelayMs)
    {
        foreach (var _ in
                 Enumerable.Range(0,
                     100)) // Run the test multiple times to increase the chance of catching timing-related issues.
        {
            // Arrange
            var logOutput = new LogOutput();
            DataFusionNativeLogger.ConfigureLogger(XUnitLogger.CreateLogger<CancellationTests>(logOutput),
                LogLevel.Debug);

            using var cts = new CancellationTokenSource();

            // Act
            var pingTask = _runtime.PingAsync(TimeSpan.FromMilliseconds(30_000), cts.Token);
            var cancellationTask = Task.Delay(TimeSpan.FromMilliseconds(cancellationDelayMs))
                .ContinueWith(_ => cts.Cancel());

            // Assert
            try
            {
                await Task.WhenAll(pingTask, cancellationTask);
            }
            catch (Exception ex)
            {
                Assert.IsType<TaskCanceledException>(ex);
            }

            try
            {
                Assert.True(pingTask.IsCanceled);
                Assert.True(cancellationTask.IsCompletedSuccessfully);

                await AssertCancelledLogMessageWithWait(logOutput);
            }
            catch
            {
                outputHelper.WriteLine("Test failed with cancellation delay of {0} ms. Logs:\n{1}", cancellationDelayMs, logOutput.GetText());
                throw;
            }
        }
    }

    [Fact(Timeout = 3_000)]
    public async Task TokenCancel_BeforeOperationStart_ThrowsCancellationException()
    {
        // Arrange
        var logOutput = new LogOutput();
        DataFusionNativeLogger.ConfigureLogger(XUnitLogger.CreateLogger<CancellationTests>(logOutput), LogLevel.Debug);
        
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        
        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await _runtime.PingAsync(TimeSpan.FromMilliseconds(1_000), cts.Token));
        await AssertCancelledLogMessageWithWait(logOutput);
    }
    
    [Fact(Timeout = 3_000)]
    public async Task TokenCancel_AfterOperationFinish_Ignored()
    {
        // Arrange
        var logOutput = new LogOutput();
        DataFusionNativeLogger.ConfigureLogger(XUnitLogger.CreateLogger<CancellationTests>(logOutput), LogLevel.Debug);
        
        using var cts = new CancellationTokenSource();
        
        // Act
        await _runtime.PingAsync(TimeSpan.FromMilliseconds(25), cts.Token);
        await cts.CancelAsync();
        
        // Assert
        var log = logOutput.GetText();
        Assert.Contains("Ping completed for user_data=", log, StringComparison.Ordinal);
        Assert.DoesNotContain("No cancellation token found for user_data=", log, StringComparison.Ordinal);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
    
    private static async Task AssertCancelledLogMessageWithWait(LogOutput logOutput)
    {
        var tries = 20; // Total wait time of 1 second (20 tries * 50ms delay)
        while (tries-- > 0) // Wait for the cancellation log message to appear
        {
            var log = logOutput.GetText();
            if (log.Contains("Ping cancelled for user_data=", StringComparison.Ordinal))
            {
                Assert.DoesNotContain("Ping completed for user_data=", log, StringComparison.Ordinal);
                break;
            }
                
            await Task.Delay(50);
        }
            
        Assert.True(tries > 0, "Expected cancellation log message not found after multiple attempts. Logs: " + logOutput.GetText());
    }

    private sealed class LogOutput : ITestOutputHelper
    {
        private readonly StringBuilder _logBuilder = new();
        
        public string GetText()
        {
            lock (_logBuilder)
                return _logBuilder.ToString();
        }

        public void WriteLine(string message)
        {
            lock (_logBuilder) 
                _logBuilder.AppendLine(message);
        }

        public void WriteLine(string format, params object[] args)
        {
            lock (_logBuilder)
            {
                _logBuilder.AppendFormat(CultureInfo.InvariantCulture, format, args);
                _logBuilder.AppendLine();
            }
        }
    }
}
