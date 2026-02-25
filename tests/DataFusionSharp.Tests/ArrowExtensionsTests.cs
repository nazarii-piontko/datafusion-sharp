using Apache.Arrow.Types;
using DataFusionSharp.Proto;
using ArrowType = DataFusionSharp.Proto.ArrowType;
using Field = DataFusionSharp.Proto.Field;
using IntervalUnit = DataFusionSharp.Proto.IntervalUnit;
using Schema = DataFusionSharp.Proto.Schema;
using TimeUnit = DataFusionSharp.Proto.TimeUnit;
using UnionMode = DataFusionSharp.Proto.UnionMode;

namespace DataFusionSharp.Tests;

public sealed class ArrowExtensionsTests
{
    #region Primitive types round-trip

    public static TheoryData<ArrowType, ArrowTypeId> PrimitiveTypes => new()
    {
        { new ArrowType { NONE = new EmptyMessage() }, ArrowTypeId.Null },
        { new ArrowType { BOOL = new EmptyMessage() }, ArrowTypeId.Boolean },
        { new ArrowType { UINT8 = new EmptyMessage() }, ArrowTypeId.UInt8 },
        { new ArrowType { INT8 = new EmptyMessage() }, ArrowTypeId.Int8 },
        { new ArrowType { UINT16 = new EmptyMessage() }, ArrowTypeId.UInt16 },
        { new ArrowType { INT16 = new EmptyMessage() }, ArrowTypeId.Int16 },
        { new ArrowType { UINT32 = new EmptyMessage() }, ArrowTypeId.UInt32 },
        { new ArrowType { INT32 = new EmptyMessage() }, ArrowTypeId.Int32 },
        { new ArrowType { UINT64 = new EmptyMessage() }, ArrowTypeId.UInt64 },
        { new ArrowType { INT64 = new EmptyMessage() }, ArrowTypeId.Int64 },
        { new ArrowType { FLOAT16 = new EmptyMessage() }, ArrowTypeId.HalfFloat },
        { new ArrowType { FLOAT32 = new EmptyMessage() }, ArrowTypeId.Float },
        { new ArrowType { FLOAT64 = new EmptyMessage() }, ArrowTypeId.Double },
        { new ArrowType { UTF8 = new EmptyMessage() }, ArrowTypeId.String },
        { new ArrowType { UTF8VIEW = new EmptyMessage() }, ArrowTypeId.StringView },
        { new ArrowType { LARGEUTF8 = new EmptyMessage() }, ArrowTypeId.LargeString },
        { new ArrowType { BINARY = new EmptyMessage() }, ArrowTypeId.Binary },
        { new ArrowType { BINARYVIEW = new EmptyMessage() }, ArrowTypeId.BinaryView },
        { new ArrowType { LARGEBINARY = new EmptyMessage() }, ArrowTypeId.LargeBinary },
        { new ArrowType { DATE32 = new EmptyMessage() }, ArrowTypeId.Date32 },
        { new ArrowType { DATE64 = new EmptyMessage() }, ArrowTypeId.Date64 },
    };

    [Theory]
    [MemberData(nameof(PrimitiveTypes))]
    public void PrimitiveType_RoundTrips(ArrowType proto, ArrowTypeId expectedTypeId)
    {
        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        Assert.Equal(expectedTypeId, arrow.TypeId);
        Assert.Equal(proto.ArrowTypeEnumCase, roundTripped.ArrowTypeEnumCase);
    }

    #endregion

    #region FixedSizeBinary round-trip

    [Fact]
    public void FixedSizeBinary_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType { FIXEDSIZEBINARY = 16 };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        Assert.IsType<FixedSizeBinaryType>(arrow);
        Assert.Equal(16, ((FixedSizeBinaryType)arrow).ByteWidth);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZEBINARY, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(16, roundTripped.FIXEDSIZEBINARY);
    }

    #endregion

    #region Duration round-trip

    public static TheoryData<TimeUnit, Apache.Arrow.Types.TimeUnit> DurationTimeUnits => new()
    {
        { TimeUnit.Second, Apache.Arrow.Types.TimeUnit.Second },
        { TimeUnit.Millisecond, Apache.Arrow.Types.TimeUnit.Millisecond },
        { TimeUnit.Microsecond, Apache.Arrow.Types.TimeUnit.Microsecond },
        { TimeUnit.Nanosecond, Apache.Arrow.Types.TimeUnit.Nanosecond },
    };

    [Theory]
    [MemberData(nameof(DurationTimeUnits))]
    public void Duration_RoundTrips(TimeUnit protoUnit, Apache.Arrow.Types.TimeUnit expectedUnit)
    {
        // Arrange
        var proto = new ArrowType { DURATION = protoUnit };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var durationType = Assert.IsType<DurationType>(arrow);
        Assert.Equal(expectedUnit, durationType.Unit);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.DURATION, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.DURATION);
    }

    #endregion

    #region Timestamp round-trip

    public static TheoryData<TimeUnit, string> TimestampVariants => new()
    {
        { TimeUnit.Second, "UTC" },
        { TimeUnit.Millisecond, "America/New_York" },
        { TimeUnit.Microsecond, "" },
        { TimeUnit.Nanosecond, "Europe/London" },
    };

    [Theory]
    [MemberData(nameof(TimestampVariants))]
    public void Timestamp_RoundTrips(TimeUnit protoUnit, string timezone)
    {
        // Arrange
        var proto = new ArrowType
        {
            TIMESTAMP = new Timestamp { TimeUnit = protoUnit, Timezone = timezone }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var timestampType = Assert.IsType<TimestampType>(arrow);
        Assert.Equal(timezone, timestampType.Timezone ?? string.Empty);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.TIMESTAMP.TimeUnit);
        Assert.Equal(timezone, roundTripped.TIMESTAMP.Timezone);
    }

    #endregion

    #region Time32 round-trip

    public static TheoryData<TimeUnit, Apache.Arrow.Types.TimeUnit> Time32Units => new()
    {
        { TimeUnit.Second, Apache.Arrow.Types.TimeUnit.Second },
        { TimeUnit.Millisecond, Apache.Arrow.Types.TimeUnit.Millisecond },
    };

    [Theory]
    [MemberData(nameof(Time32Units))]
    public void Time32_RoundTrips(TimeUnit protoUnit, Apache.Arrow.Types.TimeUnit expectedUnit)
    {
        // Arrange
        var proto = new ArrowType { TIME32 = protoUnit };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var time32Type = Assert.IsType<Time32Type>(arrow);
        Assert.Equal(expectedUnit, time32Type.Unit);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.TIME32, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.TIME32);
    }

    #endregion

    #region Time64 round-trip

    public static TheoryData<TimeUnit, Apache.Arrow.Types.TimeUnit> Time64Units => new()
    {
        { TimeUnit.Microsecond, Apache.Arrow.Types.TimeUnit.Microsecond },
        { TimeUnit.Nanosecond, Apache.Arrow.Types.TimeUnit.Nanosecond },
    };

    [Theory]
    [MemberData(nameof(Time64Units))]
    public void Time64_RoundTrips(TimeUnit protoUnit, Apache.Arrow.Types.TimeUnit expectedUnit)
    {
        // Arrange
        var proto = new ArrowType { TIME64 = protoUnit };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var time64Type = Assert.IsType<Time64Type>(arrow);
        Assert.Equal(expectedUnit, time64Type.Unit);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.TIME64, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.TIME64);
    }

    #endregion

    #region Interval round-trip

    public static TheoryData<IntervalUnit, Apache.Arrow.Types.IntervalUnit> IntervalUnits => new()
    {
        { IntervalUnit.YearMonth, Apache.Arrow.Types.IntervalUnit.YearMonth },
        { IntervalUnit.DayTime, Apache.Arrow.Types.IntervalUnit.DayTime },
        { IntervalUnit.MonthDayNano, Apache.Arrow.Types.IntervalUnit.MonthDayNanosecond },
    };

    [Theory]
    [MemberData(nameof(IntervalUnits))]
    public void Interval_RoundTrips(IntervalUnit protoUnit, Apache.Arrow.Types.IntervalUnit expectedUnit)
    {
        // Arrange
        var proto = new ArrowType { INTERVAL = protoUnit };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var intervalType = Assert.IsType<IntervalType>(arrow);
        Assert.Equal(expectedUnit, intervalType.Unit);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.INTERVAL, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.INTERVAL);
    }

    #endregion

    #region Decimal round-trip

    [Fact]
    public void Decimal128_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType
        {
            DECIMAL = new Proto.Decimal { Precision = 38, Scale = 10 }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var decimalType = Assert.IsType<Decimal128Type>(arrow);
        Assert.Equal(38, decimalType.Precision);
        Assert.Equal(10, decimalType.Scale);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.DECIMAL, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(38u, roundTripped.DECIMAL.Precision);
        Assert.Equal(10, roundTripped.DECIMAL.Scale);
    }

    [Fact]
    public void Decimal256_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType
        {
            DECIMAL256 = new Proto.Decimal256Type { Precision = 76, Scale = 20 }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var decimalType = Assert.IsType<Apache.Arrow.Types.Decimal256Type>(arrow);
        Assert.Equal(76, decimalType.Precision);
        Assert.Equal(20, decimalType.Scale);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.DECIMAL256, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(76u, roundTripped.DECIMAL256.Precision);
        Assert.Equal(20, roundTripped.DECIMAL256.Scale);
    }

    #endregion

    #region List round-trip

    [Fact]
    public void List_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType
        {
            LIST = new List
            {
                FieldType = new Field
                {
                    Name = "item",
                    ArrowType = new ArrowType { INT32 = new EmptyMessage() },
                    Nullable = true
                }
            }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var listType = Assert.IsType<ListType>(arrow);
        Assert.Equal("item", listType.ValueField.Name);
        Assert.Equal(ArrowTypeId.Int32, listType.ValueDataType.TypeId);
        Assert.True(listType.ValueField.IsNullable);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.LIST, roundTripped.ArrowTypeEnumCase);
        Assert.Equal("item", roundTripped.LIST.FieldType.Name);
    }

    [Fact]
    public void LargeList_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType
        {
            LARGELIST = new List
            {
                FieldType = new Field
                {
                    Name = "item",
                    ArrowType = new ArrowType { UTF8 = new EmptyMessage() },
                    Nullable = false
                }
            }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var largeListType = Assert.IsType<LargeListType>(arrow);
        Assert.Equal("item", largeListType.ValueField.Name);
        Assert.Equal(ArrowTypeId.String, largeListType.ValueDataType.TypeId);
        Assert.False(largeListType.ValueField.IsNullable);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.LARGELIST, roundTripped.ArrowTypeEnumCase);
    }

    [Fact]
    public void FixedSizeList_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType
        {
            FIXEDSIZELIST = new FixedSizeList
            {
                FieldType = new Field
                {
                    Name = "element",
                    ArrowType = new ArrowType { FLOAT64 = new EmptyMessage() },
                    Nullable = true
                },
                ListSize = 5
            }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var fixedListType = Assert.IsType<FixedSizeListType>(arrow);
        Assert.Equal("element", fixedListType.ValueField.Name);
        Assert.Equal(ArrowTypeId.Double, fixedListType.ValueDataType.TypeId);
        Assert.Equal(5, fixedListType.ListSize);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZELIST, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(5, roundTripped.FIXEDSIZELIST.ListSize);
    }

    #endregion

    #region Map round-trip

    [Fact]
    public void Map_RoundTrips()
    {
        // Arrange
        var entriesField = new Field
        {
            Name = "entries",
            ArrowType = new ArrowType
            {
                STRUCT = new Struct()
            },
            Nullable = false
        };
        entriesField.Children.Add(new Field
        {
            Name = "key",
            ArrowType = new ArrowType { UTF8 = new EmptyMessage() },
            Nullable = false
        });
        entriesField.Children.Add(new Field
        {
            Name = "value",
            ArrowType = new ArrowType { INT32 = new EmptyMessage() },
            Nullable = true
        });

        var proto = new ArrowType
        {
            MAP = new Map
            {
                FieldType = entriesField,
                KeysSorted = false
            }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        Assert.IsType<MapType>(arrow);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.MAP, roundTripped.ArrowTypeEnumCase);
        Assert.Equal("entries", roundTripped.MAP.FieldType.Name);
    }

    #endregion

    #region Struct round-trip

    [Fact]
    public void Struct_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType { STRUCT = new Struct() };
        var children = new List<Field>
        {
            new()
            {
                Name = "name",
                ArrowType = new ArrowType { UTF8 = new EmptyMessage() },
                Nullable = false
            },
            new()
            {
                Name = "age",
                ArrowType = new ArrowType { INT32 = new EmptyMessage() },
                Nullable = true
            }
        };

        // Act
        var arrow = proto.ToArrow(children);
        var roundTripped = arrow.ToProto();

        // Assert
        var structType = Assert.IsType<StructType>(arrow);
        Assert.Equal(2, structType.Fields.Count);
        Assert.Equal("name", structType.Fields[0].Name);
        Assert.Equal(ArrowTypeId.String, structType.Fields[0].DataType.TypeId);
        Assert.False(structType.Fields[0].IsNullable);
        Assert.Equal("age", structType.Fields[1].Name);
        Assert.Equal(ArrowTypeId.Int32, structType.Fields[1].DataType.TypeId);
        Assert.True(structType.Fields[1].IsNullable);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.STRUCT, roundTripped.ArrowTypeEnumCase);
    }

    #endregion

    #region Union round-trip

    [Fact]
    public void DenseUnion_RoundTrips()
    {
        // Arrange
        var union = new Union { UnionMode = UnionMode.Dense };
        union.TypeIds.AddRange([0, 1]);
        var proto = new ArrowType { UNION = union };
        var children = new List<Field>
        {
            new()
            {
                Name = "int_field",
                ArrowType = new ArrowType { INT32 = new EmptyMessage() },
                Nullable = true
            },
            new()
            {
                Name = "str_field",
                ArrowType = new ArrowType { UTF8 = new EmptyMessage() },
                Nullable = true
            }
        };

        // Act
        var arrow = proto.ToArrow(children);
        var roundTripped = arrow.ToProto();

        // Assert
        var unionType = Assert.IsType<UnionType>(arrow);
        Assert.Equal(Apache.Arrow.Types.UnionMode.Dense, unionType.Mode);
        Assert.Equal([0, 1], unionType.TypeIds);
        Assert.Equal(2, unionType.Fields.Count);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.UNION, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(UnionMode.Dense, roundTripped.UNION.UnionMode);
        Assert.Equal([0, 1], roundTripped.UNION.TypeIds);
    }

    [Fact]
    public void SparseUnion_RoundTrips()
    {
        // Arrange
        var union = new Union { UnionMode = UnionMode.Sparse };
        union.TypeIds.AddRange([0, 1, 2]);
        var proto = new ArrowType { UNION = union };
        var children = new List<Field>
        {
            new()
            {
                Name = "a",
                ArrowType = new ArrowType { BOOL = new EmptyMessage() },
                Nullable = true
            },
            new()
            {
                Name = "b",
                ArrowType = new ArrowType { FLOAT32 = new EmptyMessage() },
                Nullable = true
            },
            new()
            {
                Name = "c",
                ArrowType = new ArrowType { INT64 = new EmptyMessage() },
                Nullable = true
            }
        };

        // Act
        var arrow = proto.ToArrow(children);
        var roundTripped = arrow.ToProto();

        // Assert
        var unionType = Assert.IsType<UnionType>(arrow);
        Assert.Equal(Apache.Arrow.Types.UnionMode.Sparse, unionType.Mode);
        Assert.Equal([0, 1, 2], unionType.TypeIds);
        Assert.Equal(3, unionType.Fields.Count);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.UNION, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(UnionMode.Sparse, roundTripped.UNION.UnionMode);
    }

    #endregion

    #region Dictionary round-trip

    [Fact]
    public void Dictionary_RoundTrips()
    {
        // Arrange
        var proto = new ArrowType
        {
            DICTIONARY = new Dictionary
            {
                Key = new ArrowType { INT32 = new EmptyMessage() },
                Value = new ArrowType { UTF8 = new EmptyMessage() }
            }
        };

        // Act
        var arrow = proto.ToArrow(Enumerable.Empty<Field>());
        var roundTripped = arrow.ToProto();

        // Assert
        var dictType = Assert.IsType<DictionaryType>(arrow);
        Assert.Equal(ArrowTypeId.Int32, dictType.IndexType.TypeId);
        Assert.Equal(ArrowTypeId.String, dictType.ValueType.TypeId);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.DICTIONARY, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.INT32, roundTripped.DICTIONARY.Key.ArrowTypeEnumCase);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.UTF8, roundTripped.DICTIONARY.Value.ArrowTypeEnumCase);
    }

    #endregion

    #region Schema round-trip (Proto → Arrow → Proto)

    [Fact]
    public void Schema_ProtoToArrowToProto_RoundTrips()
    {
        // Arrange
        var protoSchema = new Schema();
        protoSchema.Metadata.Add("created_by", "test");

        var idField = new Field
        {
            Name = "id",
            ArrowType = new ArrowType { INT64 = new EmptyMessage() },
            Nullable = false
        };
        protoSchema.Columns.Add(idField);

        var nameField = new Field
        {
            Name = "name",
            ArrowType = new ArrowType { UTF8 = new EmptyMessage() },
            Nullable = true
        };
        nameField.Metadata.Add("description", "user name");
        protoSchema.Columns.Add(nameField);

        var scoresField = new Field
        {
            Name = "scores",
            ArrowType = new ArrowType
            {
                LIST = new List
                {
                    FieldType = new Field
                    {
                        Name = "item",
                        ArrowType = new ArrowType { FLOAT64 = new EmptyMessage() },
                        Nullable = true
                    }
                }
            },
            Nullable = true
        };
        protoSchema.Columns.Add(scoresField);

        // Act
        var arrowSchema = protoSchema.ToArrow();
        var roundTripped = arrowSchema.ToProto();

        // Assert
        Assert.Equal(3, arrowSchema.FieldsList.Count);
        Assert.Equal("id", arrowSchema.FieldsList[0].Name);
        Assert.Equal(ArrowTypeId.Int64, arrowSchema.FieldsList[0].DataType.TypeId);
        Assert.False(arrowSchema.FieldsList[0].IsNullable);
        Assert.Equal("name", arrowSchema.FieldsList[1].Name);
        Assert.True(arrowSchema.FieldsList[1].IsNullable);
        Assert.Equal("scores", arrowSchema.FieldsList[2].Name);
        Assert.Equal(ArrowTypeId.List, arrowSchema.FieldsList[2].DataType.TypeId);

        Assert.Equal(3, roundTripped.Columns.Count);
        Assert.Equal("id", roundTripped.Columns[0].Name);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.INT64, roundTripped.Columns[0].ArrowType.ArrowTypeEnumCase);
        Assert.False(roundTripped.Columns[0].Nullable);
        Assert.Equal("name", roundTripped.Columns[1].Name);
        Assert.True(roundTripped.Columns[1].Nullable);
        Assert.Contains("description", roundTripped.Columns[1].Metadata.Keys);
        Assert.Equal("user name", roundTripped.Columns[1].Metadata["description"]);
        Assert.Equal("scores", roundTripped.Columns[2].Name);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.LIST, roundTripped.Columns[2].ArrowType.ArrowTypeEnumCase);
        Assert.Contains("created_by", roundTripped.Metadata.Keys);
        Assert.Equal("test", roundTripped.Metadata["created_by"]);
    }

    #endregion

    #region Schema round-trip (Arrow → Proto → Arrow)

    [Fact]
    public void Schema_ArrowToProtoToArrow_RoundTrips()
    {
        // Arrange
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("value").DataType(DoubleType.Default).Nullable(true))
            .Field(f => f.Name("label").DataType(StringType.Default).Nullable(true)
                .Metadata("key1", "val1"))
            .Metadata("schema_version", "1")
            .Build();

        // Act
        var proto = arrowSchema.ToProto();
        var roundTripped = proto.ToArrow();

        // Assert
        Assert.Equal(3, roundTripped.FieldsList.Count);

        Assert.Equal("id", roundTripped.FieldsList[0].Name);
        Assert.Equal(ArrowTypeId.Int64, roundTripped.FieldsList[0].DataType.TypeId);
        Assert.False(roundTripped.FieldsList[0].IsNullable);

        Assert.Equal("value", roundTripped.FieldsList[1].Name);
        Assert.Equal(ArrowTypeId.Double, roundTripped.FieldsList[1].DataType.TypeId);
        Assert.True(roundTripped.FieldsList[1].IsNullable);

        Assert.Equal("label", roundTripped.FieldsList[2].Name);
        Assert.Equal(ArrowTypeId.String, roundTripped.FieldsList[2].DataType.TypeId);
        Assert.True(roundTripped.FieldsList[2].IsNullable);
        Assert.NotNull(roundTripped.FieldsList[2].Metadata);
        Assert.Equal("val1", roundTripped.FieldsList[2].Metadata["key1"]);

        Assert.NotNull(roundTripped.Metadata);
        Assert.Equal("1", roundTripped.Metadata["schema_version"]);
    }

    #endregion

    #region Schema with struct children round-trip

    [Fact]
    public void Schema_WithStructField_RoundTrips()
    {
        // Arrange
        var structField = new Field
        {
            Name = "address",
            ArrowType = new ArrowType { STRUCT = new Struct() },
            Nullable = true
        };
        structField.Children.Add(new Field
        {
            Name = "city",
            ArrowType = new ArrowType { UTF8 = new EmptyMessage() },
            Nullable = true
        });
        structField.Children.Add(new Field
        {
            Name = "zip",
            ArrowType = new ArrowType { INT32 = new EmptyMessage() },
            Nullable = false
        });

        var protoSchema = new Schema();
        protoSchema.Columns.Add(new Field
        {
            Name = "id",
            ArrowType = new ArrowType { INT64 = new EmptyMessage() },
            Nullable = false
        });
        protoSchema.Columns.Add(structField);

        // Act
        var arrowSchema = protoSchema.ToArrow();
        var roundTripped = arrowSchema.ToProto();

        // Assert
        Assert.Equal(2, arrowSchema.FieldsList.Count);
        var arrowStruct = Assert.IsType<StructType>(arrowSchema.FieldsList[1].DataType);
        Assert.Equal(2, arrowStruct.Fields.Count);
        Assert.Equal("city", arrowStruct.Fields[0].Name);
        Assert.Equal("zip", arrowStruct.Fields[1].Name);

        Assert.Equal(2, roundTripped.Columns.Count);
        Assert.Equal(ArrowType.ArrowTypeEnumOneofCase.STRUCT, roundTripped.Columns[1].ArrowType.ArrowTypeEnumCase);
        Assert.Equal(2, roundTripped.Columns[1].Children.Count);
        Assert.Equal("city", roundTripped.Columns[1].Children[0].Name);
        Assert.Equal("zip", roundTripped.Columns[1].Children[1].Name);
    }

    #endregion
}
