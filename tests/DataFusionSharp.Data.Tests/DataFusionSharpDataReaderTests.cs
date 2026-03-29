namespace DataFusionSharp.Data.Tests;

public sealed class DataFusionSharpDataReaderTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();
    private readonly SessionContext _session;
    private readonly DataFusionSharpConnection _connection;

    public DataFusionSharpDataReaderTests()
    {
        _session = _runtime.CreateSessionContext();
        _connection = new DataFusionSharpConnection(_session, leaveOpen: true);
        _connection.Open();
    }

    public void Dispose()
    {
        _connection.Dispose();
        _session.Dispose();
        _runtime.Dispose();
    }

    [Fact]
    public async Task FieldCount_ReturnsNumberOfColumns()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1 AS a, 2 AS b, 3 AS c");

        // Act
        var fieldCount = reader.FieldCount;
        
        // Verify
        Assert.Equal(3, fieldCount);
    }

    [Fact]
    public async Task GetName_ReturnsColumnName()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(1 AS INT) AS my_col");

        // Act
        var name = reader.GetName(0);
        
        // Verify
        Assert.Equal("my_col", name);
    }

    [Fact]
    public async Task GetDataTypeName_ReturnsArrowTypeName()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(1 AS INT) AS col");

        // Act
        var typeName = reader.GetDataTypeName(0);
        
        // Verify
        Assert.Equal("int32", typeName);
    }

    [Fact]
    public async Task GetFieldType_ReturnsNetType()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(1 AS INT) AS col");

        // Act
        var fieldType = reader.GetFieldType(0);
        
        // Verify
        Assert.Equal(typeof(int), fieldType);
    }

    [Fact]
    public async Task Depth_ReturnsZero()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1");

        // Act
        var depth = reader.Depth;
        
        // Verify
        Assert.Equal(0, depth);
    }

    [Fact]
    public async Task RecordsAffected_ReturnsMinus1()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1");

        // Act
        var recordsAffected = reader.RecordsAffected;
        
        // Verify
        Assert.Equal(-1, recordsAffected);
    }

    [Fact]
    public async Task NextResult_ReturnsFalse()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1");

        // Act
        var nextResult = await reader.NextResultAsync();
        
        // Verify
        Assert.False(nextResult);
    }

    [Fact]
    public async Task GetOrdinal_ExactName_ReturnsCorrectOrdinal()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1 AS order_id, 2 AS order_amount");

        // Act
        var orderIdOrdinal = reader.GetOrdinal("order_id");
        var orderAmountOrdinal = reader.GetOrdinal("order_amount");
        
        // Verify
        Assert.Equal(0, orderIdOrdinal);
        Assert.Equal(1, orderAmountOrdinal);
    }

    [Fact]
    public async Task GetOrdinal_DifferentCase_ReturnsSameOrdinal()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1 AS customer_name");

        // Act & Verify
        Assert.Equal(0, reader.GetOrdinal("customer_name"));
        Assert.Equal(0, reader.GetOrdinal("CUSTOMER_NAME"));
        Assert.Equal(0, reader.GetOrdinal("Customer_Name"));
    }

    [Fact]
    public async Task GetOrdinal_MultiWordColumnAlias_WithSpace_ReturnsOrdinal()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("""SELECT CAST(7 AS BIGINT) AS "order count", CAST('x' AS VARCHAR) AS "customer name" """);

        // Act
        var orderCountOrdinal = reader.GetOrdinal("order count");
        var customerNameOrdinal = reader.GetOrdinal("customer name");
        
        // Verify
        Assert.Equal(0, orderCountOrdinal);
        Assert.Equal(1, customerNameOrdinal);
    }

    [Fact]
    public async Task GetOrdinal_UnknownColumn_ThrowsIndexOutOfRangeException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1 AS id");

        // Act & Verify
        Assert.Throws<ArgumentException>(() => reader.GetOrdinal("nonexistent"));
    }

    [Fact]
    public async Task GetBoolean_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT true AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetBoolean(0);
        
        // Verify
        Assert.True(value);
    }
    
    [Fact]
    public async Task GetBoolean_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetBoolean(0));
    }

    [Fact]
    public async Task GetInt16_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(32 AS SMALLINT) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetInt16(0);
        
        // Verify
        Assert.Equal((short)32, value);
    }
    
    [Fact]
    public async Task GetInt16_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetInt16(0));
    }

    [Fact]
    public async Task GetInt32_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(42 AS INT) AS val");
        await reader.ReadAsync();
        
        // Act
        var value = reader.GetInt32(0);
        
        // Verify
        Assert.Equal(42, value);
    }
    
    [Fact]
    public async Task GetInt32_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
    }

    [Fact]
    public async Task GetInt64_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(9876543210 AS BIGINT) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetInt64(0);
        
        // Verify
        Assert.Equal(9876543210L, value);
    }
    
    [Fact]
    public async Task GetInt64_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetInt64(0));
    }

    [Fact]
    public async Task GetFloat_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(1.5 AS FLOAT) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetFloat(0);
        
        // Verify
        Assert.Equal(1.5f, value, precision: 5);
    }
    
    [Fact]
    public async Task GetFloat_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetFloat(0));
    }

    [Fact]
    public async Task GetDouble_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(3.14 AS DOUBLE) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetDouble(0);
        
        // Verify
        Assert.Equal(3.14, value, precision: 10);
    }
    
    [Fact]
    public async Task GetDouble_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetDouble(0));
    }

    [Fact]
    public async Task GetDecimal_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(99.99 AS DECIMAL(10,2)) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetDecimal(0);
        
        // Verify
        Assert.Equal(99.99m, value);
    }
    
    [Fact]
    public async Task GetDecimal_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetDecimal(0));
    }

    [Fact]
    public async Task GetString_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello world' AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetString(0);
        
        // Verify
        Assert.Equal("hello world", value);
    }
    
    [Fact]
    public async Task GetString_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1 AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetString(0));
    }
    
    [Fact]
    public async Task GetGuid_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT '76a5d4d0-f0b3-4ace-a8a3-e235e161a2f1' AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetGuid(0);
        
        // Verify
        Assert.Equal(Guid.Parse("76a5d4d0-f0b3-4ace-a8a3-e235e161a2f1"), value);
    }
    
    [Fact]
    public async Task GetGuid_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetGuid(0));
    }

    [Fact]
    public async Task GetDateTime_DateColumn_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST('2026-03-28' AS DATE) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetDateTime(0);
        
        // Verify
        Assert.Equal(new DateTime(2026, 03, 28), value);
    }
    
    [Fact]
    public async Task GetDateTime_ThrowsInvalidCastException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 'hello' AS val");
        await reader.ReadAsync();

        // Act & Verify
        Assert.Throws<InvalidCastException>(() => reader.GetDateTime(0));
    }

    [Fact]
    public async Task IndexerByOrdinal_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(55 AS INT) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader[0];

        // Verify
        Assert.IsType<int>(value);
        Assert.Equal(55, (int)value);
    }

    [Fact]
    public async Task IndexerByName_ReturnsValue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(55 AS INT) AS \"order count\"");
        await reader.ReadAsync();

        // Act
        var value = reader["order count"];

        // Verify
        Assert.IsType<int>(value);
        Assert.Equal(55, (int)value);
    }

    [Fact]
    public async Task GetValues_FillsArrayWithCurrentRow()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(1 AS SMALLINT) AS a, CAST(2 AS INT) AS b, CAST(3 AS BIGINT) AS c");
        await reader.ReadAsync();

        // Act
        var values = new object[3];
        var count = reader.GetValues(values);

        // Verify
        Assert.Equal(3, count);
        Assert.IsType<short>(values[0]);
        Assert.Equal(1, (short)values[0]);
        Assert.IsType<int>(values[1]);
        Assert.Equal(2, (int)values[1]);
        Assert.IsType<long>(values[2]);
        Assert.Equal(3, (long)values[2]);
    }

    [Fact]
    public async Task IsDBNull_NullValue_ReturnsTrue()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(NULL AS INT) AS val");
        await reader.ReadAsync();

        // Act
        var isNull = await reader.IsDBNullAsync(0);
        
        // Verify
        Assert.True(isNull);
    }

    [Fact]
    public async Task IsDBNull_NonNullValue_ReturnsFalse()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(42 AS INT) AS val");
        await reader.ReadAsync();

        // Act
        var isNull = await reader.IsDBNullAsync(0);
        
        // Verify
        Assert.False(isNull);
    }

    [Fact]
    public async Task GetValue_NullValue_ReturnsDBNull()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(NULL AS VARCHAR) AS val");
        await reader.ReadAsync();

        // Act
        var value = reader.GetValue(0);
        
        Assert.Equal(DBNull.Value, value);
    }

    [Fact]
    public async Task ReadAsync_IteratesAllRows()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT s.value FROM generate_series(1, 20000) AS s");

        // Act
        var count = 0;
        while (await reader.ReadAsync())
            count++;

        // Verify
        Assert.Equal(20000, count);
    }

    [Fact]
    public async Task ReadAsync_EmptyResult_ReturnsFalseImmediately()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1 AS val WHERE false");

        // Act
        var read = await reader.ReadAsync();
        
        // Verify
        Assert.False(read);
    }

    [Fact]
    public async Task GetValue_BeforeFirstRead_ThrowsInvalidOperationException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT CAST(1 AS INT) AS val");

        // Act & Verify
        Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
    }

    [Fact]
    public async Task Read_AfterClose_ThrowsInvalidOperationException()
    {
        // Arrange
        await using var reader = await OpenReaderAsync("SELECT 1");
        await reader.CloseAsync();

        // Act & Verify
        Assert.Throws<InvalidOperationException>(() => reader.Read());
    }
    
    private async Task<DataFusionSharpDataReader> OpenReaderAsync(string sql)
    {
        await using var cmd = new DataFusionSharpCommand(_connection)
        {
            CommandText = sql
        };
        return (DataFusionSharpDataReader)await cmd.ExecuteReaderAsync();
    }
}
