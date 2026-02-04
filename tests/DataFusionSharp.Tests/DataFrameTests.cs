using Apache.Arrow;

namespace DataFusionSharp.Tests;

public sealed class DataFrameTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public async Task CountAsync_ReturnsRowCount()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        // Act
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(3UL, count);
    }

    [Fact]
    public async Task CountAsync_EmptyResult_ReturnsZero()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 WHERE false");

        // Act
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(0UL, count);
    }

    [Fact]
    public async Task ShowAsync_CompletesSuccessfully()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as value");

        // Act & Assert
        await df.ShowAsync();
    }

    [Fact]
    public async Task ShowAsync_WithLimit_CompletesSuccessfully()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        // Act & Assert
        await df.ShowAsync(limit: 2);
    }

    [Fact]
    public async Task GetSchemaAsync_ReturnsSchema()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as id, 'hello' as name");

        // Act
        var schema = await df.GetSchemaAsync();

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("id", schema.FieldsList[0].Name);
        Assert.Equal("name", schema.FieldsList[1].Name);
    }

    [Fact]
    public async Task CollectAsync_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)");

        // Act
        var collected = await df.CollectAsync();

        // Assert
        Assert.NotNull(collected);
        Assert.NotNull(collected.Schema);
        Assert.Equal(2, collected.Schema.FieldsList.Count);
        Assert.NotEmpty(collected.Batches);
        Assert.Equal(2, collected.Batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task CollectAsync_EmptyResult_ReturnsEmptyBatches()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as id WHERE false");

        // Act
        var collected = await df.CollectAsync();

        // Assert
        Assert.NotNull(collected);
        Assert.NotNull(collected.Schema);
        Assert.Equal(0, collected.Batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task ToStringAsync_ReturnsString()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as value");

        // Act
        var str = await df.ToStringAsync();

        // Assert
        Assert.NotNull(str);
        Assert.NotEmpty(str);
        Assert.Contains("value", str);
    }

    [Fact]
    public async Task ExecuteStreamAsync_ReturnsStream()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        // Act
        using var stream = await df.ExecuteStreamAsync();

        // Assert
        Assert.NotNull(stream);
        Assert.Same(df, stream.DataFrame);
    }

    [Fact]
    public async Task ExecuteStreamAsync_IteratesBatches()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)");

        // Act
        using var stream = await df.ExecuteStreamAsync();
        var batches = new List<RecordBatch>();
        await foreach (var batch in stream)
            batches.Add(batch);

        // Assert
        Assert.NotEmpty(batches);
        Assert.Equal(3, batches.Sum(b => b.Length));
        
        Assert.Equal(typeof(Int64Array), batches[0].Column("id").GetType());
        Assert.Equal(typeof(StringArray), batches[0].Column("name").GetType());
        
        var allIds = batches.SelectMany(b => ((Int64Array)b.Column("id")).Values.ToArray()).ToList();
        var allNames = batches.SelectMany(b =>
        {
            var names = new List<string>();
            for (var i = 0; i < b.Length; i++)
                names.Add(((StringArray)b.Column("name")).GetString(i)!);
            return names;
        }).ToList();
        
        Assert.Equal([1, 2, 3], allIds);
        Assert.Equal(["a", "b", "c"], allNames);
    }

    [Fact]
    public async Task ExecuteStreamAsync_WithBigTable_IteratesBatches()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT series.value AS id, sin(series.value) AS value FROM generate_series(1, 50000) AS series");

        // Act
        using var stream = await df.ExecuteStreamAsync();
        var batches = new List<RecordBatch>();
        await foreach (var batch in stream)
            batches.Add(batch);

        // Assert
        Assert.NotEmpty(batches);
        
        Assert.Equal(typeof(Int64Array), batches[0].Column("id").GetType());
        Assert.Equal(typeof(DoubleArray), batches[0].Column("value").GetType());
        
        var allIds = batches.SelectMany(b => ((Int64Array)b.Column("id")).Values.ToArray()).ToList();
        var allValues = batches.SelectMany(b => ((DoubleArray)b.Column("value")).Values.ToArray()).ToList();
        
        Assert.Equal(50000, allIds.Count);
        Assert.Equal(50000, allValues.Count);
        
        Assert.Equal(1250025000, allIds.Sum());
        Assert.Equal(0.4316858, allValues.Sum(), (x, y) => Math.Abs(x - y) < 1e-5);
    }

    [Fact]
    public async Task ExecuteStreamAsync_EmptyResult_ReturnsNoRows()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        using var df = await context.SqlAsync("SELECT 1 as id WHERE false");

        // Act
        using var stream = await df.ExecuteStreamAsync();
        var totalRows = 0;
        await foreach (var batch in stream)
            totalRows += batch.Length;

        // Assert
        Assert.Equal(0, totalRows);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}