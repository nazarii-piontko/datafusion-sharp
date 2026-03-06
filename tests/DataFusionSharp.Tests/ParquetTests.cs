using Apache.Arrow;
using Apache.Arrow.Types;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Parquet;
using Xunit.Abstractions;
using Schema = Apache.Arrow.Schema;

namespace DataFusionSharp.Tests;

public sealed class ParquetTests : FileFormatTests
{
    protected override string FileExtension => ".parquet";

    public ParquetTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync(string tableName = "customers")
    {
        return Context.RegisterParquetAsync(tableName, DataSet.CustomersParquetPath);
    }

    protected override Task RegisterOrdersTableAsync(string tableName = "orders")
    {
        return Context.RegisterParquetAsync(tableName, DataSet.OrdersParquetPath);
    }

    protected override Task RegisterTableFromPathAsync(string tableName, string path)
    {
        return Context.RegisterParquetAsync(tableName, path);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteParquetAsync(path);
    }

    [Fact]
    public async Task RegisterParquetAsync_WithSchema_ProjectsOnlySpecifiedColumns()
    {
        // Arrange
        var projectedSchema = new Schema.Builder()
            .Field(f => f.Name("customer_id").DataType(Int64Type.Default).Nullable(true))
            .Field(f => f.Name("customer_name").DataType(StringType.Default).Nullable(true))
            .Build();
        var options = new ParquetReadOptions
        {
            Schema = projectedSchema
        };

        // Act
        await Context.RegisterParquetAsync("customers", DataSet.CustomersParquetPath, options);
        using var df = await Context.SqlAsync("SELECT * FROM customers");
        var schema = df.GetSchema();
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("customer_id", schema.FieldsList[0].Name);
        Assert.Equal("customer_name", schema.FieldsList[1].Name);
    }

    [Fact]
    public async Task RegisterParquetAsync_WithFileExtension_SpecifiesFileFilter()
    {
        // Arrange
        using var tempFile = await TempInputFile.CreateAsync(".parq");
        File.Copy(DataSet.CustomersParquetPath, tempFile.Path, overwrite: true);
        var options = new ParquetReadOptions
        {
            FileExtension = ".parq"
        };

        // Act
        await Context.RegisterParquetAsync("test", tempFile.Path, options);
        using var df = await Context.SqlAsync("SELECT * FROM test");
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
    }

    [Fact]
    public async Task WriteParquetAsync_WithDataFrameWriteOptions_WritesPartitionedOutput()
    {
        // Arrange
        await Context.RegisterParquetAsync("customers", DataSet.CustomersParquetPath);
        using var df = await Context.SqlAsync("SELECT * FROM customers");

        using var tempDir = TempDirectory.Create();
        var writeOptions = new DataFrameWriteOptions
        {
            PartitionBy = ["country"]
        };

        // Act
        await df.WriteParquetAsync(tempDir.Path, dataFrameWriteOptions: writeOptions);

        // Assert
        var partitionDirs = Directory.GetDirectories(tempDir.Path);
        var partitionNames = partitionDirs.Select(Path.GetFileName).OrderBy(n => n).ToList();
        Assert.Equal(["country=France", "country=Germany", "country=UK", "country=USA"], partitionNames);

        foreach (var dir in partitionDirs)
        {
            var parquetFiles = Directory.GetFiles(dir, "*.parquet");
            Assert.NotEmpty(parquetFiles);
        }
    }

    [Fact]
    public async Task WriteParquetAsync_WithSnappyCompression_ProducesSmallerFileThanUncompressed()
    {
        // Arrange — generate enough data for compression to have a measurable effect
        using var df = await Context.SqlAsync(
            """
            SELECT s.value AS id,
                   'Customer name entry number ' || s.value AS name,
                   'Some repeated description text for compression test' AS description
            FROM generate_series(1, 500) AS s
            """);

        using var uncompressedFile = await TempInputFile.CreateAsync(".parquet");
        using var snappyFile = await TempInputFile.CreateAsync(".parquet");

        // Act
        await df.WriteParquetAsync(uncompressedFile.Path, parquetWriteOptions: new ParquetWriteOptions
        {
            Compression = ParquetCompression.Uncompressed
        });
        await df.WriteParquetAsync(snappyFile.Path, parquetWriteOptions: new ParquetWriteOptions
        {
            Compression = ParquetCompression.Snappy
        });

        // Assert — Snappy file must be smaller, proving the codec was applied
        var uncompressedSize = new FileInfo(uncompressedFile.Path).Length;
        var snappySize = new FileInfo(snappyFile.Path).Length;
        Assert.True(snappySize < uncompressedSize, $"Snappy file ({snappySize} bytes) should be smaller than uncompressed ({uncompressedSize} bytes)");

        // Roundtrip: read back and verify data integrity
        await Context.RegisterParquetAsync("roundtrip_snappy", snappyFile.Path);
        using var readBackDf = await Context.SqlAsync("SELECT * FROM roundtrip_snappy");
        var count = await readBackDf.CountAsync();
        Assert.Equal(500UL, count);
    }
}
