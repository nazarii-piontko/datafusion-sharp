using Apache.Arrow;
using Apache.Arrow.Types;
using DataFusionSharp.Formats.Csv;
using Xunit.Abstractions;
using Field = Apache.Arrow.Field;
using Schema = Apache.Arrow.Schema;

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
    
    protected override Task RegisterTableFromPathAsync(string tableName, string path)
    {
        return Context.RegisterCsvAsync(tableName, path);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteCsvAsync(path);
    }
    
    [Fact]
    public async Task RegisterCsvAsync_WithDirectory_ReadsCombinedDataFromMultipleFiles()
    {
        // Arrange
        var tempDir = Path.Combine(Path.GetTempPath(), $"csv_dir_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);
        
        try
        {
            // Create multiple CSV files in the directory
            var file1 = Path.Combine(tempDir, "data1.csv");
            var file2 = Path.Combine(tempDir, "data2.csv");
            
            await File.WriteAllLinesAsync(file1, [
                "id,name,value",
                "1,Alice,100",
                "2,Bob,200"
            ]);
            
            await File.WriteAllLinesAsync(file2, [
                "id,name,value",
                "3,Charlie,300",
                "4,Diana,400"
            ]);

            // Act
            await Context.RegisterCsvAsync("test", tempDir);
            using var df = await Context.SqlAsync("SELECT * FROM test");
            var records = await df.CollectAsync();
            var count = await df.CountAsync();

            // Assert
            Assert.Equal(4UL, count);
            Assert.Equal(3, records.Schema.FieldsList.Count);
            Assert.Equivalent(new[] { "id", "name", "value" }, records.Schema.FieldsList.Select(f => f.Name));
            Assert.Equivalent(new[] { "Alice", "Bob", "Charlie", "Diana" }, records.Batches.SelectMany(b => b.Column("name").AsString()));
        }
        finally
        {
            // Cleanup
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
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
        var options = new CsvReadOptions
        {
            Delimiter = ';'
        };

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
        var options = new CsvReadOptions
        {
            Quote = '~'
        };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var records = await df.CollectAsync();

        // Assert
        Assert.Equal(3, records.Schema.FieldsList.Count);
        Assert.Equivalent(new[] {"Alice", "Bob"}, records.Batches.SelectMany(b => b.Column("name").AsString()));
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
        var options = new CsvReadOptions
        {
            Comment = '#'
        };

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
        var options = new CsvReadOptions
        {
            HasHeader = false
        };

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
        var options = new CsvReadOptions
        {
            HasHeader = false,
            Schema = schema
        };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var records = await df.CollectAsync();

        // Assert
        Assert.Equal(3, records.Schema.FieldsList.Count);
        Assert.Contains("name", records.Schema.FieldsList.Select(f => f.Name));
        Assert.Equivalent(new[] {"Alice", "Bob"}, records.Batches.SelectMany(b => b.Column("name").AsString()));
    }

    [Fact]
    public async Task RegisterCsvAsync_WithNullRegex_RecognizesNullPatterns()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "id,value",
                "1,NULL",
                "2,NULL",
                "3,NULL"
            ]);
        var options = new CsvReadOptions
        {
            NullRegex = "^NULL$|^$"
        };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var records = await df.CollectAsync();

        // Assert
        Assert.Equal(2, records.Schema.FieldsList.Count);
        Assert.Equal(typeof(NullArray), records.Batches[0].Column("value").GetType());
    }

    [Fact]
    public async Task RegisterCsvAsync_WithSchemaInferMaxRecords_LimitsInferenceRows()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "id,value",
                "1,100",
                "2,200"
            ]);
        var options = new CsvReadOptions
        {
            SchemaInferMaxRecords = 1
        };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var schema = await df.GetSchemaAsync();

        // Assert
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equivalent(new[] { "id", "value" }, schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task RegisterCsvAsync_WithFileExtension_SpecifiesFileFilter()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".txt",
            [
                "id,name",
                "1,Alice",
                "2,Bob"
            ]);
        var options = new CsvReadOptions
        {
            FileExtension = ".txt"
        };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(2UL, count);
    }

    [Fact]
    public async Task RegisterCsvAsync_WithTruncatedRows_AllowsIncompleteRows()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".csv",
            [
                "id,name,value",
                "1,Alice,100",
                "2,Bob"
            ]);
        var options = new CsvReadOptions
        {
            TruncatedRows = true
        };

        // Act
        await Context.RegisterCsvAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var count = await df.CountAsync();

        // Assert
        Assert.Equal(2UL, count);
    }
    
    [Fact]
    public async Task WriteAsync_WithOptions_WritesFileSuccessfully()
    {
        // Arrange
        await Context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);
        using var df = await Context.SqlAsync("SELECT * FROM customers ORDER BY customer_id DESC LIMIT 2");
        var options = new CsvWriteOptions
        {
            Delimiter = ';'
        };
        using var tempFile = await TempInputFile.CreateAsync(FileExtension);
        
        // Act
        await df.WriteCsvAsync(tempFile.Path, options);

        // Assert
        Assert.True(File.Exists(tempFile.Path), "Output file should be created");
        
        var content = await File.ReadAllTextAsync(tempFile.Path);
        Assert.Contains("customer_id;customer_name;country;city;signup_date;customer_segment", content, StringComparison.Ordinal);
        Assert.Contains("10;Vehement Capital Partners;France;Paris;2022-06-20;SMB", content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WriteAsync_WithHasHeaderFalse_WritesFileWithHeader()
    {
        // Arrange
        await Context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);
        using var df = await Context.SqlAsync("SELECT customer_id, customer_name FROM customers ORDER BY customer_id LIMIT 2");
        var options = new CsvWriteOptions
        {
            HasHeader = false
        };
        using var tempFile = await TempInputFile.CreateAsync(FileExtension);
        
        // Act
        await df.WriteCsvAsync(tempFile.Path, options);

        // Assert
        Assert.True(File.Exists(tempFile.Path), "Output file should be created");
        
        var content = await File.ReadAllTextAsync(tempFile.Path);
        Assert.DoesNotContain("customer_id", content, StringComparison.Ordinal);
        Assert.DoesNotContain("customer_name", content, StringComparison.Ordinal);
    }
}
