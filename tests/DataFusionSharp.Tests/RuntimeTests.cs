namespace DataFusionSharp.Tests;

public sealed class RuntimeTests
{
    [Fact]
    public void Create_WithSyncDispose_ReturnsRuntime()
    {
        // Act
        using var runtime = DataFusionRuntime.Create();

        // Assert
        Assert.NotNull(runtime);
    }

    [Fact]
    public void Create_MultipleRuntimes_AllValid()
    {
        // Act
        using var runtime1 = DataFusionRuntime.Create();
        using var runtime2 = DataFusionRuntime.Create();

        // Assert
        Assert.NotNull(runtime1);
        Assert.NotNull(runtime2);
    }
    
    [Theory]
    [InlineData(0u, 1u)]
    [InlineData(1u, 0u)]
    public void Create_WithInvalidParameters_Throws(uint workerThreads, uint maxBlockingThreads)
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => DataFusionRuntime.Create(workerThreads, maxBlockingThreads));
    }
    
    [Fact]
    public void CreateSession_WithDisposed_Throws()
    {
        // Arrange
        var runtime = DataFusionRuntime.Create();
        runtime.Dispose();

        // Act & Assert
        Assert.Throws<ObjectDisposedException>(() => runtime.CreateSessionContext());
    }
    
    [Fact]
    public void Shutdown_SuccessfullyShutsDownRuntime()
    {
        // Arrange
        using var runtime = DataFusionRuntime.Create();

        // Act
        runtime.Shutdown();

        // Assert
        Assert.Throws<ObjectDisposedException>(() => runtime.CreateSessionContext());
    }
}