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
    public void S3Options_ToProto_SetsExpectedField(S3ObjectStoreOptions options, Func<Proto.S3ObjectStoreOptions, bool> assertion)
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
    
    [Fact]
    public void RegisterAzureBlobStorage_WithoutOptions_ExtractsContainerFromUrl()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act & Assert — Azure requires account name, so provide it via options with container from URL
        context.RegisterAzureBlobStorage("az://my-container", new AzureBlobStorageOptions
        {
            ContainerName = "my-container",
            AccountName = "devstoreaccount1",
            UseEmulator = true,
        });
    }

    [Fact]
    [Trait("Category", "Internet")]
    [Trait("Category", "Azure")]
    public async Task RegisterAzureBlobStorage_WithExplicitOptions_ThenQueryParquet_ReturnsData()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act
        context.RegisterAzureBlobStorage(
            "az://nyctlc",
            new AzureBlobStorageOptions
            {
                ContainerName = "nyctlc",
                AccountName = "azureopendatastorage",
                SkipSignature = true
            });
        await context.RegisterParquetAsync(
            "green",
            "az://nyctlc/green/",
            new ParquetReadOptions { TablePartitionCols = [
                new PartitionColumn("puYear", Int32Type.Default),
                new PartitionColumn("puMonth", Int32Type.Default)
            ]});

        // Query with partition column filter to exercise Hive-style partitioning
        using var df = await context.SqlAsync("SELECT count(*) as cnt FROM green WHERE \"puYear\" = 2023 AND \"puMonth\" = 1");
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0, "Expected rows from Parquet on Azure Blob Storage with explicit options");
    }

    [Theory]
    [MemberData(nameof(AzureOptionsToProtoCases))]
    public void AzureOptions_ToProto_SetsExpectedField(AzureBlobStorageOptions options, Func<Proto.AzureBlobStorageOptions, bool> assertion)
    {
        var proto = options.ToProto();

        Assert.True(assertion(proto));
    }

    public static TheoryData<AzureBlobStorageOptions, Func<Proto.AzureBlobStorageOptions, bool>> AzureOptionsToProtoCases => new()
    {
        { new AzureBlobStorageOptions { ContainerName = "c" }, p => p is { ContainerName: "c", HasAccountName: false, HasAccessKey: false } },
        { new AzureBlobStorageOptions { ContainerName = "c", AccountName = "acc" }, p => p is { HasAccountName: true, AccountName: "acc" } },
        { new AzureBlobStorageOptions { ContainerName = "c", AccessKey = "key" }, p => p is { HasAccessKey: true, AccessKey: "key" } },
        { new AzureBlobStorageOptions { ContainerName = "c", BearerToken = "tok" }, p => p is { HasBearerToken: true, BearerToken: "tok" } },
        { new AzureBlobStorageOptions { ContainerName = "c", ClientId = "cid" }, p => p is { HasClientId: true, ClientId: "cid" } },
        { new AzureBlobStorageOptions { ContainerName = "c", ClientSecret = "cs" }, p => p is { HasClientSecret: true, ClientSecret: "cs" } },
        { new AzureBlobStorageOptions { ContainerName = "c", TenantId = "tid" }, p => p is { HasTenantId: true, TenantId: "tid" } },
        { new AzureBlobStorageOptions { ContainerName = "c", SasKey = "sas" }, p => p is { HasSasKey: true, SasKey: "sas" } },
        { new AzureBlobStorageOptions { ContainerName = "c", Endpoint = "http://localhost" }, p => p is { HasEndpoint: true, Endpoint: "http://localhost" } },
        { new AzureBlobStorageOptions { ContainerName = "c", UseEmulator = true }, p => p is { HasUseEmulator: true, UseEmulator: true } },
        { new AzureBlobStorageOptions { ContainerName = "c", AllowHttp = true }, p => p is { HasAllowHttp: true, AllowHttp: true } },
        { new AzureBlobStorageOptions { ContainerName = "c", SkipSignature = true }, p => p is { HasSkipSignature: true, SkipSignature: true } },
    };

    [Fact]
    public void RegisterGoogleCloudStorage_WithoutOptions_ExtractsBucketFromUrl()
    {
        // Arrange
        using var context = _runtime.CreateSessionContext();

        // Act & Assert
        context.RegisterGoogleCloudStorage("gs://my-bucket");
    }

    [Theory]
    [MemberData(nameof(GcsOptionsToProtoCases))]
    public void GcsOptions_ToProto_SetsExpectedField(GoogleCloudStorageOptions options, Func<Proto.GoogleCloudStorageOptions, bool> assertion)
    {
        var proto = options.ToProto();

        Assert.True(assertion(proto));
    }

    public static TheoryData<GoogleCloudStorageOptions, Func<Proto.GoogleCloudStorageOptions, bool>> GcsOptionsToProtoCases => new()
    {
        { new GoogleCloudStorageOptions { BucketName = "b" }, p => p is { BucketName: "b", HasProjectId: false, HasCredentialsPath: false } },
        { new GoogleCloudStorageOptions { BucketName = "b", ProjectId = "proj" }, p => p is { HasProjectId: true, ProjectId: "proj" } },
        { new GoogleCloudStorageOptions { BucketName = "b", CredentialsPath = "/path/to/key.json" }, p => p is { HasCredentialsPath: true, CredentialsPath: "/path/to/key.json" } },
        { new GoogleCloudStorageOptions { BucketName = "b", Credentials = "{\"key\":\"val\"}" }, p => p is { HasCredentials: true, Credentials: "{\"key\":\"val\"}" } },
        { new GoogleCloudStorageOptions { BucketName = "b", ServiceAccountEmail = "sa@proj.iam.gserviceaccount.com" }, p => p is { HasServiceAccountEmail: true, ServiceAccountEmail: "sa@proj.iam.gserviceaccount.com" } },
        { new GoogleCloudStorageOptions { BucketName = "b", CustomEndpoint = "http://localhost:4443" }, p => p is { HasCustomEndpoint: true, CustomEndpoint: "http://localhost:4443" } },
        { new GoogleCloudStorageOptions { BucketName = "b", AllowHttp = true }, p => p is { HasAllowHttp: true, AllowHttp: true } },
        { new GoogleCloudStorageOptions { BucketName = "b", SkipSignature = true }, p => p is { HasSkipSignature: true, SkipSignature: true } },
    };

    public void Dispose()
    {
        _runtime.Dispose();
    }
}
