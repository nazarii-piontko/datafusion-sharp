using Apache.Arrow;
using Apache.Arrow.Types;
using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

public sealed class CsvTests : FileFormatTests
{
    protected override string FileExtension => ".csv";

    public CsvTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync(string tableName = "customers")
    {
        return Context.RegisterCsvAsync(tableName, DataSet.CustomersCsvPath);
    }

    protected override Task RegisterOrdersTableAsync(string tableName = "orders")
    {
        return Context.RegisterCsvAsync(tableName, DataSet.OrdersCsvPath);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteCsvAsync(path);
    }

    [Fact]
    public async Task RegisterCsvAsync_WithCustomDelimiter_ParsesSuccessfully()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "id;name;value",
                "1;Alice;100",
                "2;Bob;200"
            ]);
        var options = new CsvReadOptions { DelimiterChar = ';' };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var count = await df.CountAsync();
        var schema = await df.GetSchemaAsync();

        // Assert
        Assert.Equal(2UL, count);
        Assert.Equal(3, schema.FieldsList.Count);
        Assert.Equivalent(new[] { "id", "name", "value" }, schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task RegisterCsvAsync_WithCustomQuoteChar_ParsesQuotedFieldsSuccessfully()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "id,name,value",
                "1,~Alice~,100",
                "2,~Bob~,200"
            ]);
        var options = new CsvReadOptions { QuoteChar = '~' };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var records = await df.CollectAsync();

        // Assert
        Assert.Equal(3, records.Schema.FieldsList.Count);
        Assert.Equivalent(new[] {"Alice", "Bob"}, records.Batches.SelectMany(b => b.Column("name").GetStringValues()));
    }

    [Fact]
    public async Task RegisterCsvAsync_WithCommentChar_IgnoresCommentLines()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "id,name,value",
                "1,Alice,100",
                "# This is a comment",
                "2,Bob,200"
            ]);
        var options = new CsvReadOptions { CommentChar = '#' };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(2UL, count);
    }

    [Fact]
    public async Task RegisterCsvAsync_WithoutHeader_InfersDefaultColumnNames()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "1,Alice,100",
                "2,Bob,200"
            ]);
        var options = new CsvReadOptions { HasHeader = false };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var count = await df.CountAsync();
        var schema = await df.GetSchemaAsync();

        // Assert
        Assert.Equal(2UL, count);
        Assert.Equal(3, schema.FieldsList.Count);
        Assert.Equivalent(new[] { "column_1", "column_2", "column_3" }, schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task RegisterCsvAsync_WithExplicitSchema_AppliesSchemaToHeaderlessFile()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "1,Alice,100",
                "2,Bob,200"
            ]);
        
        var fields = new[]
        {
            new Field("id", Int64Type.Default, false),
            new Field("name", StringType.Default, false),
            new Field("amount", Int64Type.Default, false)
        };
        var schema = new Schema(fields, []);
        var options = new CsvReadOptions { HasHeader = false, Schema = schema };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var records = await df.CollectAsync();

        // Assert
        Assert.Equal(3, records.Schema.FieldsList.Count);
        Assert.Equivalent(new[] {"Alice", "Bob"}, records.Batches.SelectMany(b => b.Column("name").GetStringValues()));
    }
}