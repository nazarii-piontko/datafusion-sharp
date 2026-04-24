#pragma warning disable CA2008

namespace DataFusionSharp.Tests;

public sealed class CancellationTests : IDisposable
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
        foreach (var _ in Enumerable.Range(0, 100)) // Run the test multiple times to increase the chance of catching timing-related issues.
        {
            // Arrange
            using var cts = new CancellationTokenSource();

            // Act
            var pingTask = _runtime.PingAsync(TimeSpan.FromMilliseconds(20_000), cts.Token);
            cts.CancelAfter(TimeSpan.FromMilliseconds(cancellationDelayMs));

            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await pingTask);
            Assert.True(pingTask.IsCanceled);
        }
    }

    [Fact(Timeout = 1_000)]
    public async Task TokenCancel_BeforeOperationStart_ThrowsCancellationException()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        
        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await _runtime.PingAsync(TimeSpan.FromMilliseconds(5_000), cts.Token));
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}
