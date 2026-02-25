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
    public async Task RegisterJsonAsync_WithSchemaInferMaxRecords_LimitsInferenceRows()
    {
        // Arrange
        var options = new JsonReadOptions
        {
            SchemaInferMaxRecords = 1
        };

        // Act
        await Context.RegisterJsonAsync("customers", DataSet.CustomersJsonPath, options);
        using var df = await Context.SqlAsync("SELECT * FROM customers");
        var schema = await df.GetSchemaAsync();

        // Assert
        Assert.True(schema.FieldsList.Count > 0);
        Assert.Contains("customer_id", schema.FieldsList.Select(f => f.Name));
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
    public async Task WriteJsonAsync_WithOptions_WritesFileSuccessfully()
    {
        // Arrange
        await Context.RegisterJsonAsync("customers", DataSet.CustomersJsonPath);
        using var df = await Context.SqlAsync("SELECT * FROM customers ORDER BY customer_id DESC LIMIT 2");
        var options = new JsonWriteOptions();
        var tempPath = GenerateTempFileName();

        try
        {
            // Act
            await df.WriteJsonAsync(tempPath, options);

            // Assert
            Assert.True(File.Exists(tempPath), "Output file should be created");
            Assert.True(new FileInfo(tempPath).Length > 0);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); }
                catch { /* ignore cleanup errors */ }
            }
        }
    }
}
