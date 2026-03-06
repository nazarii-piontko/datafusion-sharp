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

    [Theory]
    [MemberData(nameof(S3OptionsToProtoCases))]
    public void S3Options_ToProto_SetsExpectedField(
        S3ObjectStoreOptions options, Func<Proto.S3ObjectStoreOptions, bool> assertion)
    {
        var proto = options.ToProto();

        Assert.True(assertion(proto));
    }

    public static TheoryData<S3ObjectStoreOptions, Func<Proto.S3ObjectStoreOptions, bool>> S3OptionsToProtoCases => new()
    {
        { new S3ObjectStoreOptions { BucketName = "b" }, p => p is { BucketName: "b", HasRegion: false, HasAccessKeyId: false } },
        { new S3ObjectStoreOptions { BucketName = "b", Region = "us-east-1" }, p => p is { HasRegion: true, Region: "us-east-1" } },
        { new S3ObjectStoreOptions { BucketName = "b", AccessKeyId = "AK" }, p => p is { HasAccessKeyId: true, AccessKeyId: "AK" } },
        { new S3ObjectStoreOptions { BucketName = "b", SecretAccessKey = "SK" }, p => p is { HasSecretAccessKey: true, SecretAccessKey: "SK" } },
        { new S3ObjectStoreOptions { BucketName = "b", Endpoint = "http://localhost" }, p => p is { HasEndpoint: true, Endpoint: "http://localhost" } },
        { new S3ObjectStoreOptions { BucketName = "b", Token = "tok" }, p => p is { HasToken: true, Token: "tok" } },
        { new S3ObjectStoreOptions { BucketName = "b", AllowHttp = true }, p => p is { HasAllowHttp: true, AllowHttp: true } },
        { new S3ObjectStoreOptions { BucketName = "b", VirtualHostedStyleRequest = true }, p => p is { HasVirtualHostedStyleRequest: true, VirtualHostedStyleRequest: true } },
        { new S3ObjectStoreOptions { BucketName = "b", SkipSignature = true }, p => p is { HasSkipSignature: true, SkipSignature: true } },
    };

    public void Dispose()
    {
        _runtime.Dispose();
    }
}
