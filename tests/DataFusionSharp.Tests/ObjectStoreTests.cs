using Apache.Arrow.Types;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Parquet;
using DataFusionSharp.ObjectStore;

namespace DataFusionSharp.Tests;

public sealed class ObjectStoreTests : IDisposable
{
    private readonly DataFusionRuntime _runtime = DataFusionRuntime.Create();

    [Fact]
    public async Task RegisterLocalFileSystem_ThenQueryCsv_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        context.RegisterLocalFileSystem($"file:///");

        await context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);

        // Act
        using var df = await context.SqlAsync("SELECT * FROM customers");
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0, "Expected rows from CSV after registering local FS object store");
    }

    [Fact]
    public void DeregisterObjectStore_RegisteredStore_Succeeds()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();
        context.RegisterLocalFileSystem("file:///");

        // Act & Assert
        context.DeregisterObjectStore("file:///");
    }

    [Fact]
    public void DeregisterObjectStore_NotRegistered_Throws()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act & Assert
        Assert.Throws<DataFusionException>(() => context.DeregisterObjectStore("s3://nonexistent-bucket"));
    }

    [Fact]
    public void RegisterS3ObjectStore_WithoutOptions_ExtractsBucketFromUrl()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act & Assert
        context.RegisterS3ObjectStore("s3://my-bucket");
    }

    [Fact]
    [Trait("Category", "Internet")]
    [Trait("Category", "S3")]
    public async Task RegisterS3ObjectStore_WithExplicitOptions_ThenQueryParquet_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act
        context.RegisterS3ObjectStore(
            "s3://arrow-datasets",
            new S3ObjectStoreOptions
            {
                BucketName = "arrow-datasets",
                Region = "us-east-1",
                SkipSignature = true
            });
        await context.RegisterParquetAsync(
            "diamonds",
            "s3://arrow-datasets/diamonds/",
            new ParquetReadOptions { TablePartitionCols = [new PartitionColumn("cut", StringType.Default)] });
        
        using var df = await context.SqlAsync("SELECT cut, carat, price FROM diamonds WHERE cut = 'Good' AND price < 500 LIMIT 5");
        using var result = await df.CollectAsync();

        // Assert
        Assert.Single(result.Batches);
        
        var batch = result.Batches[0];
        Assert.True(batch.Length is > 0 and <= 5, $"Expected 1-5 rows, got {batch.Length}");
        
        var cutData = batch.Column("cut").AsString().ToList();
        Assert.Equal(["Good", "Good", "Good", "Good", "Good"], cutData);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}
