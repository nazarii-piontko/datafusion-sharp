using Apache.Arrow.Types;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Json;
using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

public sealed class JsonTests : FileFormatTests
{
    protected override string FileExtension => ".json";

    public JsonTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync(string tableName = "customers")
    {
        return Context.RegisterJsonAsync(tableName, DataSet.CustomersJsonPath);
    }

    protected override Task RegisterOrdersTableAsync(string tableName = "orders")
    {
        return Context.RegisterJsonAsync(tableName, DataSet.OrdersJsonPath);
    }

    protected override Task RegisterTableFromPathAsync(string tableName, string path)
    {
        return Context.RegisterJsonAsync(tableName, path);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteJsonAsync(path);
    }

    [Fact]
    public async Task RegisterJsonAsync_WithFileExtension_SpecifiesFileFilter()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(".ndjson");
        File.Copy(DataSet.CustomersJsonPath, tempFile.Path, overwrite: true);
        var options = new JsonReadOptions
        {
            FileExtension = ".ndjson"
        };

        // Act
        await Context.RegisterJsonAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
    }
    
    [Fact]
    public async Task RegisterJsonAsync_WithFileExtensionAndWrongExtension_DoesNotRegisterTable()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(".json");
        var options = new JsonReadOptions
        {
            FileExtension = ".ndjson"
        };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<DataFusionException>(async () =>
        {            
            await Context.RegisterJsonAsync("test", tempFile.Path, options);
        });
        Assert.Contains(".ndjson", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RegisterJsonAsync_WithCompression_ReadsCompressedFileSuccessfully()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(
            ".json.gz",
            [
                "{\"customer_id\": 1, \"name\": \"Alice\"}",
                "{\"customer_id\": 2, \"name\": \"Bob\"}"
            ],
            gzip: true);

        var options = new JsonReadOptions
        {
            FileCompressionType = CompressionType.Gzip,
            FileExtension = ".json.gz"
        };

        // Act
        await Context.RegisterJsonAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test ORDER BY customer_id");
        var results = await df.ToStringAsync();

        // Assert
        Assert.Contains("customer_id", results, StringComparison.Ordinal);
        Assert.Contains("Alice", results, StringComparison.Ordinal);
        Assert.Contains("Bob", results, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RegisterJsonAsync_WithTablePartitionCols_ReadsPartitionedData()
    {
        // Arrange
        var options = new JsonReadOptions
        {
            TablePartitionCols = [new PartitionColumn("category", StringType.Default)]
        };

        // Act
        await Context.RegisterJsonAsync("products", DataSet.ProductsJsonDir, options);
        using var df = await Context.SqlAsync("SELECT * FROM products ORDER BY product_id");
        var records = await df.CollectAsync();

        // Assert
        Assert.Contains("category", records.Schema.FieldsList.Select(f => f.Name));

        var categories = records.Batches.SelectMany(b => b.Column("category").AsString()).OrderBy(n => n).ToList();
        Assert.Equal(["Hardware", "Hardware", "Software", "Software"], categories);

        var names = records.Batches.SelectMany(b => b.Column("name").AsString()).OrderBy(n => n).ToList();
        Assert.Equal(["Antivirus", "Laptop", "Router", "Windows"], names);
    }

    [Fact]
    public async Task WriteJsonAsync_WithOptions_WritesFileSuccessfully()
    {
        // Arrange
        await Context.RegisterJsonAsync("customers", DataSet.CustomersJsonPath);
        using var df = await Context.SqlAsync("SELECT * FROM customers ORDER BY customer_id DESC LIMIT 2");
        var options = new JsonWriteOptions
        {
            Compression = CompressionType.Gzip
        };
        
        using var tempFile = await TempInputFile.CreateAsync(".json.gz", gzip: true);
        
        // Act
        await df.WriteJsonAsync(tempFile.Path, options);

        // Assert
        Assert.True(File.Exists(tempFile.Path), "Output file should be created");
        
        var lines = tempFile.ReadLines();
        Assert.Equal(2, lines.Count);
        Assert.Contains("customer_id", lines[0], StringComparison.Ordinal);
        Assert.Contains("customer_id", lines[^1], StringComparison.Ordinal);
    }
}
