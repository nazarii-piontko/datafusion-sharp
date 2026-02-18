using Apache.Arrow;

namespace DataFusionSharp.Tests;

public sealed class DataFrameTests : IDisposable
{
    private readonly DataFusionRuntime _runtime;
    private readonly SessionContext _context;

    public DataFrameTests()
    {
        _runtime = DataFusionRuntime.Create();
        _context = _runtime.CreateSessionContext();
    }
    
    [Fact]
    public async Task CountAsync_ReturnsRowCount()
    {
        // Arrange
        using var df = await _context.SqlAsync(GetIdValueTableSelectSql(3));

        // Act
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(3UL, count);
    }

    [Fact]
    public async Task CountAsync_EmptyResult_ReturnsZero()
    {
        // Arrange
        using var df = await _context.SqlAsync(GetIdValueTableSelectSql(0));

        // Act
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(0UL, count);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(2ul)]
    public async Task ShowAsync_CompletesSuccessfully(ulong? limit)
    {
        // Arrange
        using var df = await _context.SqlAsync(GetIdValueTableSelectSql(1));

        // Act & Assert
        await df.ShowAsync(limit);
    }
    
    [Fact]
    public async Task ToStringAsync_ReturnsString()
    {
        // Arrange
        using var df = await _context.SqlAsync(GetIdValueTableSelectSql(1));

        // Act
        var str = await df.ToStringAsync();

        // Assert
        Assert.NotNull(str);
        Assert.NotEmpty(str);
        Assert.Contains("| id | value ", str, StringComparison.Ordinal);
        Assert.Contains("| 1  | 0.8414", str, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetSchemaAsync_ReturnsSchema()
    {
        // Arrange
        using var df = await _context.SqlAsync(GetIdValueTableSelectSql(1));

        // Act
        var schema = await df.GetSchemaAsync();

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("id", schema.FieldsList[0].Name);
        Assert.Equal("value", schema.FieldsList[1].Name);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    [InlineData(100000)]
    public async Task CollectAsync_ReturnsData(int rowsCount)
    {
        // Arrange
        using var df = await _context.SqlAsync(GetIdValueTableSelectSql(rowsCount));

        // Act
        var collected = await df.CollectAsync();

        // Assert
        Assert.NotNull(collected);
        
        Assert.NotNull(collected.Schema);
        Assert.Equal(2, collected.Schema.FieldsList.Count);
        Assert.Equal("id", collected.Schema.FieldsList[0].Name);
        Assert.Equal("value", collected.Schema.FieldsList[1].Name);
        
        var rows = GetRows(collected.Batches);
        Assert.Equal(rowsCount, rows.Count);
        
        var expectedRows = GetExpectedRows(rowsCount);
        for (int i = 0; i < rowsCount; i++)
        {
            Assert.Equal(expectedRows[i].Id, rows[i].Id);
            Assert.Equal(expectedRows[i].Value, rows[i].Value, precision: 5);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    [InlineData(100000)]
    public async Task ExecuteStreamAsync_ReturnsData(int rowsCount)
    {
        // Arrange
        using var df = await _context.SqlAsync(GetIdValueTableSelectSql(rowsCount));

        // Act
        using var stream = await df.ExecuteStreamAsync();
        
        var batches = new List<RecordBatch>();
        await foreach (var batch in stream)
            batches.Add(batch);

        // Assert
        var rows = GetRows(batches);
        Assert.Equal(rowsCount, rows.Count);
        
        var expectedRows = GetExpectedRows(rowsCount);
        for (int i = 0; i < rowsCount; i++)
        {
            Assert.Equal(expectedRows[i].Id, rows[i].Id);
            Assert.Equal(expectedRows[i].Value, rows[i].Value, precision: 5);
        }
    }

    public void Dispose()
    {
        _context.Dispose();
        _runtime.Dispose();
    }

    private static string GetIdValueTableSelectSql(int rowsCount)
    {
        return $"SELECT s.value AS id, sin(s.value) AS value FROM generate_series(1, {Math.Max(1, rowsCount)}) AS s WHERE {rowsCount > 0}";
    }
    

    private static List<(long Id, double Value)> GetRows(RecordBatch batch)
    {
        var rows = new List<(long Id, double Value)>(batch.Length);
        
        for (int i = 0; i < batch.Length; ++i)
        {
            var id = ((Int64Array)batch.Column("id")).GetValue(i)!.Value;
            var value = ((DoubleArray)batch.Column("value")).GetValue(i)!.Value;
            rows.Add((id, value));
        }
        rows.Sort((x, y) => x.Id.CompareTo(y.Id));

        return rows;
    }
    
    private static List<(long Id, double Value)> GetRows(IEnumerable<RecordBatch> batches) => batches.SelectMany(GetRows).OrderBy(x => x.Id).ToList();

    private static List<(long Id, double Value)> GetExpectedRows(int rowsCount)
    {
        var rows = new List<(long Id, double Value)>(rowsCount);
        for (int i = 1; i <= rowsCount; ++i)
            rows.Add((i, Math.Sin(i)));
        return rows;
    }
}