using System.Data;

namespace DataFusionSharp.Data.Tests;

public sealed class DataFusionSharpConnectionTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();
    private readonly SessionContext _session;

    public DataFusionSharpConnectionTests()
    {
        _session = _runtime.CreateSessionContext();
    }

    public void Dispose()
    {
        _session.Dispose();
        _runtime.Dispose();
    }

    [Fact]
    public void DefaultState_IsClosed()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);

        // Verify
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void Open_SetsStateToOpen()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);

        // Act
        connection.Open();

        // Verify
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public void Close_AfterOpen_SetsStateToClosed()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);
        connection.Open();

        // Act
        connection.Close();

        // Verify
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public async Task OpenAsync_SetsStateToOpen()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);

        // Act
        await connection.OpenAsync();

        // Verify
        Assert.Equal(ConnectionState.Open, connection.State);
    }
    
    [Fact]
    public async Task CloseAsync_AfterOpenAsync_SetsStateToClosed()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);
        await connection.OpenAsync();

        // Act
        await connection.CloseAsync();

        // Verify
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public async Task Execute_OnClosedConnection_ThrowsInvalidOperationException()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";

        // Act & Verify
        await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteReaderAsync());
    }

    [Fact]
    public async Task Dispose_WithLeaveOpenFalse_DisposesUnderlyingSession()
    {
        // Arrange
        using var runtime = DataFusionRuntime.Create();
        var session = runtime.CreateSessionContext(); // Connection should dispose it

        var connection = new DataFusionSharpConnection(session, leaveOpen: false);
        connection.Dispose();

        // Act & Verify
        await Assert.ThrowsAnyAsync<ObjectDisposedException>(async () =>
        {
            using var df = await session.SqlAsync("SELECT 1");
        });
    }

    [Fact]
    public async Task Dispose_WithLeaveOpenTrue_SessionRemainsUsable()
    {
        // Arrange
        using var runtime = DataFusionRuntime.Create();
        using var session = runtime.CreateSessionContext();

        var connection = new DataFusionSharpConnection(session, leaveOpen: true);
        await connection.OpenAsync();

        // Act
        connection.Dispose(); // must NOT dispose the session
        using var df = await session.SqlAsync("SELECT 1");
        var count = await df.CountAsync();

        // Verify
        Assert.Equal(1UL, count);
    }

    [Fact]
    public void BeginTransaction_ThrowsNotSupportedException()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);
        connection.Open();

        // Act & Verify
        Assert.Throws<NotSupportedException>(() => connection.BeginTransaction());
    }

    [Fact]
    public void ChangeDatabase_ThrowsNotSupportedException()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);

        // Act & Verify
        Assert.Throws<NotSupportedException>(() => connection.ChangeDatabase("other"));
    }

    [Fact]
    public void CreateCommand_ReturnsDataFusionSharpCommand()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);

        // Act
        using var cmd = connection.CreateCommand();

        // Verify
        Assert.IsType<DataFusionSharpCommand>(cmd);
    }

    [Fact]
    public void Database_ReturnsDefault()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);

        // Verify
        Assert.Equal("default", connection.Database);
    }

    [Fact]
    public void DataSource_ReturnsDataFusion()
    {
        // Arrange
        using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);

        // Verify
        Assert.Equal("DataFusion", connection.DataSource);
    }

    [Fact]
    public void AsConnection_ExtensionMethod_ReturnsConnection()
    {
        // Arrange & Act
        using var connection = _session.AsConnection(leaveOpen: true);

        // Verify
        Assert.NotNull(connection);
        Assert.IsType<DataFusionSharpConnection>(connection);
    }
}
