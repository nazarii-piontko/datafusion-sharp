using System.Data;

namespace DataFusionSharp.Data.Tests;

public sealed class DataFusionSharpCommandTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();
    private readonly SessionContext _session;
    private readonly DataFusionSharpConnection _connection;

    public DataFusionSharpCommandTests()
    {
        _session = _runtime.CreateSessionContext();
        
        _connection = _session.AsConnection(leaveOpen: true);
        _connection.Open();
    }

    public void Dispose()
    {
        _connection.Dispose();
        _session.Dispose();
        _runtime.Dispose();
    }

    [Fact]
    public async Task Connection_ReturnsValidConnection()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand();
        cmd.Connection = _connection;

        // Act
        var connection = cmd.Connection;
        
        // Verify
        Assert.NotNull(connection);
        Assert.Equal(_connection, connection);
    }
    
    [Fact]
    public async Task ExecuteReaderAsync_ReturnsRows()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT 1 AS val"
        };
        
        // Act
        await using var reader = await cmd.ExecuteReaderAsync();
        var read = await reader.ReadAsync();
        
        // Verify
        Assert.True(read);
        Assert.True(reader.FieldCount > 0);
    }

    [Fact]
    public async Task ExecuteScalarAsync_ReturnsFirstColumnFirstRow()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT 1 AS val"
        };

        // Act
        var result = await cmd.ExecuteScalarAsync();

        // Verify
        Assert.NotNull(result);
        Assert.IsType<long>(result);
        Assert.Equal(1L, (long)result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithEmptyResult_ReturnsNull()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT 1 AS val WHERE false"
        };

        // Act
        var result = await cmd.ExecuteScalarAsync();

        // Verify
        Assert.Null(result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithNullFirstValue_ReturnsNull()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT CAST(NULL AS INT) AS val"
        };

        // Act
        var result = await cmd.ExecuteScalarAsync();

        // Verify
        Assert.NotNull(result);
        Assert.IsType<DBNull>(result);
    }

    [Fact]
    public void ExecuteNonQuery_ReturnsMinus1()
    {
        // Arrange
        using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT 1 AS val"
        };
        
        // Act
        var result = cmd.ExecuteNonQuery();

        // Verify
        Assert.Equal(-1, result);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_ReturnsMinus1()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT 1 AS val"
        };

        // Act
        var result = await cmd.ExecuteNonQueryAsync(CancellationToken.None);

        // Verify
        Assert.Equal(-1, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithAtPrefixedParam_FiltersResults()
    {
        // Arrange
        // @status in SQL is translated to $status; parameter named @status has NormalizedName = status
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT @status"
        };
        cmd.Parameters.Add(new DataFusionSharpParameter("@status", "Completed"));

        // Act
        var result = await cmd.ExecuteScalarAsync();

        // Verify
        Assert.NotNull(result);
        Assert.IsType<string>(result);
        Assert.Equal("Completed", result);
    }
    
    public static IEnumerable<object[]> MapTypesCorrectlyData() =>
    [
        [true, true], // bool
        [(byte)1, (byte)1], // byte
        [(sbyte)-2, (sbyte)-2], // sbyte
        [(ushort)3, (ushort)3], // ushort
        [(short)-3, (short)-3], // short
        [(uint)4, (uint)4], // uint
        [-5, -5], // int
        [(ulong)6, (ulong)6], // ulong
        [(long)-7, (long)-7], // long
        [(float)8.1, (float)8.1], // float
        [9.1, 9.1], // double
        [9.1m, 9.1m], // decimal
        ["hello world", "hello world"], // string
        ['h', "h"], // char return string
        [new DateOnly(2026, 3, 27), new DateOnly(2026, 3, 27)], // date
        [new TimeOnly(7, 30, 45), new TimeOnly(7, 30, 45)], // time
        [new DateTime(2026, 3, 27, 7, 30, 45, DateTimeKind.Utc), new DateTimeOffset(2026, 3, 27, 7, 30, 45, TimeSpan.Zero)], // datetime
        [new byte[] {0x03, 0x04}, new byte[] {0x03, 0x04}], // binary
        [new ScalarValue.Binary([0x01, 0x02]), new byte[] {0x01, 0x02}], // raw scalar value
    ];

    [Theory]
    [MemberData(nameof(MapTypesCorrectlyData))]
    public async Task ExecuteScalarAsync_WithParameter_MapTypesCorrectly(object? parameterValue, object expectedResult)
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT @value"
        };
        cmd.Parameters.Add(new DataFusionSharpParameter("@value", parameterValue));

        // Act
        var result = await cmd.ExecuteScalarAsync();

        // Verify
        Assert.NotNull(result);
        Assert.IsType(expectedResult.GetType(), result);
        Assert.Equal(expectedResult, result);
    }
    
    [Fact]
    public async Task ExecuteScalarAsync_MapNullCorrectly()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT @value"
        };
        cmd.Parameters.Add(new DataFusionSharpParameter("@value", null));

        // Act
        var result = await cmd.ExecuteScalarAsync();

        // Verify
        Assert.NotNull(result);
        Assert.IsType<DBNull>(result);
        Assert.Equal(DBNull.Value, result);
    }

    [Fact]
    public async Task ExecuteScalarAsync_WithMultipleParams_FiltersResults()
    {
        // Arrange
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = "SELECT @status WHERE 10000 > @min_amount"
        };
        cmd.Parameters.Add(new DataFusionSharpParameter("@status", "Completed"));
        cmd.Parameters.Add(new DataFusionSharpParameter("@min_amount", 5000L));

        // Act
        var result = await cmd.ExecuteScalarAsync();

        // Verify
        Assert.NotNull(result);
        Assert.IsType<string>(result);
        Assert.Equal("Completed", result);
    }
    
    [Fact]
    public async Task ExecuteReaderAsync_WithClosedConnection_ThrowsInvalidOperationException()
    {
        // Arrange
        await using var connection = new DataFusionSharpConnection(_session, leaveOpen: true);
        await using var cmd = new DataFusionSharpCommand(connection)
        {
            CommandText = "SELECT 1 AS val"
        };

        // Act & Verify
        await Assert.ThrowsAsync<InvalidOperationException>(() => cmd.ExecuteReaderAsync());
    }

    [Fact]
    public void CommandType_SetToStoredProcedure_ThrowsNotSupportedException()
    {
        // Arrange
        using var cmd = new DataFusionSharpCommand(_connection);

        // Act & Verify
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.StoredProcedure);
    }

    [Fact]
    public void Cancel_ThrowsNotSupportedException()
    {
        // Arrange
        using var cmd = new DataFusionSharpCommand(_connection);

        // Act & Verify
        Assert.Throws<NotSupportedException>(() => cmd.Cancel());
    }
}
