using Apache.Arrow.Types;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Parquet;
using ParquetReadOptions = DataFusionSharp.Formats.Parquet.ParquetReadOptions;
using Schema = Apache.Arrow.Schema;

namespace DataFusionSharp.Tests;

public sealed class ProtoParquetOptionsExtensionsTests
{
    [Fact]
    public void ReadOptions_Default_ProducesEmptyProto()
    {
        // Arrange
        var options = new ParquetReadOptions();

        // Act
        var proto = options.ToProto();

        // Assert
        Assert.Null(proto.Schema);
        Assert.False(proto.HasFileExtension);
        Assert.False(proto.HasParquetPruning);
        Assert.False(proto.HasSkipMetadata);
        Assert.Empty(proto.TablePartitionCols);
    }

    [Fact]
    public void ReadOptions_WithSchema_SetsSchemaInProto()
    {
        // Arrange
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
            .Build();
        var options = new ParquetReadOptions { Schema = schema };

        // Act
        var proto = options.ToProto();

        // Assert
        Assert.NotNull(proto.Schema);
        Assert.Equal(2, proto.Schema.Columns.Count);
        Assert.Equal("id", proto.Schema.Columns[0].Name);
        Assert.Equal("name", proto.Schema.Columns[1].Name);
    }

    [Fact]
    public void ReadOptions_WithFileExtension_SetsFileExtensionInProto()
    {
        // Arrange
        var options = new ParquetReadOptions { FileExtension = ".parq" };

        // Act
        var proto = options.ToProto();

        // Assert
        Assert.True(proto.HasFileExtension);
        Assert.Equal(".parq", proto.FileExtension.ToStringUtf8());
    }

    [Fact]
    public void ReadOptions_WithTablePartitionCols_SetsPartitionColsInProto()
    {
        // Arrange
        var options = new ParquetReadOptions
        {
            TablePartitionCols =
            [
                new PartitionColumn("year", Int32Type.Default),
                new PartitionColumn("region", StringType.Default)
            ]
        };

        // Act
        var proto = options.ToProto();

        // Assert
        Assert.Equal(2, proto.TablePartitionCols.Count);
        Assert.Equal("year", proto.TablePartitionCols[0].Name);
        Assert.Equal("region", proto.TablePartitionCols[1].Name);
    }

    [Fact]
    public void ReadOptions_WithParquetPruning_SetsPruningInProto()
    {
        // Arrange
        var options = new ParquetReadOptions { ParquetPruning = false };

        // Act
        var proto = options.ToProto();

        // Assert
        Assert.True(proto.HasParquetPruning);
        Assert.False(proto.ParquetPruning);
    }

    [Fact]
    public void ReadOptions_WithSkipMetadata_SetsSkipMetadataInProto()
    {
        // Arrange
        var options = new ParquetReadOptions { SkipMetadata = true };

        // Act
        var proto = options.ToProto();

        // Assert
        Assert.True(proto.HasSkipMetadata);
        Assert.True(proto.SkipMetadata);
    }

    [Theory]
    [InlineData(ParquetCompression.Uncompressed, "uncompressed")]
    [InlineData(ParquetCompression.Snappy, "snappy")]
    [InlineData(ParquetCompression.Gzip, "gzip(4)")]
    [InlineData(ParquetCompression.Brotli, "brotli(4)")]
    [InlineData(ParquetCompression.Lz4, "lz4")]
    [InlineData(ParquetCompression.Lz4Raw, "lz4_raw")]
    [InlineData(ParquetCompression.Zstd, "zstd(4)")]
    public void WriteOptions_Compression_MapsToExpectedProtoString(ParquetCompression compression, string expectedProtoString)
    {
        // Arrange
        var options = new ParquetWriteOptions { Compression = compression };

        // Act
        var proto = options.ToProto();

        // Assert
        Assert.Equal(expectedProtoString, proto.Global.Compression);
    }

    [Fact]
    public void WriteOptions_InvalidCompression_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var options = new ParquetWriteOptions { Compression = (ParquetCompression)999 };

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => options.ToProto());
    }
}
