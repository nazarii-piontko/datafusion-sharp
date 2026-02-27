using Apache.Arrow.Types;

namespace DataFusionSharp.Tests;

public sealed class ProtoArrowExtensionsTests
{
    #region Primitive types round-trip

    public static TheoryData<Proto.ArrowType, ArrowTypeId> PrimitiveTypes => new()
    {
        { new Proto.ArrowType { NONE = new Proto.EmptyMessage() }, ArrowTypeId.Null },
        { new Proto.ArrowType { BOOL = new Proto.EmptyMessage() }, ArrowTypeId.Boolean },
        { new Proto.ArrowType { UINT8 = new Proto.EmptyMessage() }, ArrowTypeId.UInt8 },
        { new Proto.ArrowType { INT8 = new Proto.EmptyMessage() }, ArrowTypeId.Int8 },
        { new Proto.ArrowType { UINT16 = new Proto.EmptyMessage() }, ArrowTypeId.UInt16 },
        { new Proto.ArrowType { INT16 = new Proto.EmptyMessage() }, ArrowTypeId.Int16 },
        { new Proto.ArrowType { UINT32 = new Proto.EmptyMessage() }, ArrowTypeId.UInt32 },
        { new Proto.ArrowType { INT32 = new Proto.EmptyMessage() }, ArrowTypeId.Int32 },
        { new Proto.ArrowType { UINT64 = new Proto.EmptyMessage() }, ArrowTypeId.UInt64 },
        { new Proto.ArrowType { INT64 = new Proto.EmptyMessage() }, ArrowTypeId.Int64 },
        { new Proto.ArrowType { FLOAT16 = new Proto.EmptyMessage() }, ArrowTypeId.HalfFloat },
        { new Proto.ArrowType { FLOAT32 = new Proto.EmptyMessage() }, ArrowTypeId.Float },
        { new Proto.ArrowType { FLOAT64 = new Proto.EmptyMessage() }, ArrowTypeId.Double },
        { new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() }, ArrowTypeId.String },
        { new Proto.ArrowType { UTF8VIEW = new Proto.EmptyMessage() }, ArrowTypeId.StringView },
        { new Proto.ArrowType { LARGEUTF8 = new Proto.EmptyMessage() }, ArrowTypeId.LargeString },
        { new Proto.ArrowType { BINARY = new Proto.EmptyMessage() }, ArrowTypeId.Binary },
        { new Proto.ArrowType { BINARYVIEW = new Proto.EmptyMessage() }, ArrowTypeId.BinaryView },
        { new Proto.ArrowType { LARGEBINARY = new Proto.EmptyMessage() }, ArrowTypeId.LargeBinary },
        { new Proto.ArrowType { DATE32 = new Proto.EmptyMessage() }, ArrowTypeId.Date32 },
        { new Proto.ArrowType { DATE64 = new Proto.EmptyMessage() }, ArrowTypeId.Date64 },
    };

    [Theory]
    [MemberData(nameof(PrimitiveTypes))]
    public void PrimitiveType_RoundTrips(Proto.ArrowType proto, ArrowTypeId expectedTypeId)
    {
        // Act
        var arrow = proto.ToArrow([]);
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
        var proto = new Proto.ArrowType { FIXEDSIZEBINARY = 16 };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        Assert.IsType<FixedSizeBinaryType>(arrow);
        Assert.Equal(16, ((FixedSizeBinaryType)arrow).ByteWidth);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZEBINARY, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(16, roundTripped.FIXEDSIZEBINARY);
    }

    #endregion

    #region Duration round-trip

    public static TheoryData<Proto.TimeUnit, TimeUnit> DurationTimeUnits => new()
    {
        { Proto.TimeUnit.Second, TimeUnit.Second },
        { Proto.TimeUnit.Millisecond, TimeUnit.Millisecond },
        { Proto.TimeUnit.Microsecond, TimeUnit.Microsecond },
        { Proto.TimeUnit.Nanosecond, TimeUnit.Nanosecond },
    };

    [Theory]
    [MemberData(nameof(DurationTimeUnits))]
    public void Duration_RoundTrips(Proto.TimeUnit protoUnit, TimeUnit expectedUnit)
    {
        // Arrange
        var proto = new Proto.ArrowType { DURATION = protoUnit };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var durationType = Assert.IsType<DurationType>(arrow);
        Assert.Equal(expectedUnit, durationType.Unit);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.DURATION, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.DURATION);
    }

    #endregion

    #region Timestamp round-trip

    public static TheoryData<Proto.TimeUnit, string> TimestampVariants => new()
    {
        { Proto.TimeUnit.Second, "UTC" },
        { Proto.TimeUnit.Millisecond, "America/New_York" },
        { Proto.TimeUnit.Microsecond, "" },
        { Proto.TimeUnit.Nanosecond, "Europe/London" },
    };

    [Theory]
    [MemberData(nameof(TimestampVariants))]
    public void Timestamp_RoundTrips(Proto.TimeUnit protoUnit, string timezone)
    {
        // Arrange
        var proto = new Proto.ArrowType
        {
            TIMESTAMP = new Proto.Timestamp { TimeUnit = protoUnit, Timezone = timezone }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var timestampType = Assert.IsType<TimestampType>(arrow);
        Assert.Equal(timezone, timestampType.Timezone ?? string.Empty);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.TIMESTAMP.TimeUnit);
        Assert.Equal(timezone, roundTripped.TIMESTAMP.Timezone);
    }

    #endregion

    #region Time32 round-trip

    public static TheoryData<Proto.TimeUnit, TimeUnit> Time32Units => new()
    {
        { Proto.TimeUnit.Second, TimeUnit.Second },
        { Proto.TimeUnit.Millisecond, TimeUnit.Millisecond },
    };

    [Theory]
    [MemberData(nameof(Time32Units))]
    public void Time32_RoundTrips(Proto.TimeUnit protoUnit, TimeUnit expectedUnit)
    {
        // Arrange
        var proto = new Proto.ArrowType { TIME32 = protoUnit };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var time32Type = Assert.IsType<Time32Type>(arrow);
        Assert.Equal(expectedUnit, time32Type.Unit);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.TIME32, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.TIME32);
    }

    #endregion

    #region Time64 round-trip

    public static TheoryData<Proto.TimeUnit, TimeUnit> Time64Units => new()
    {
        { Proto.TimeUnit.Microsecond, TimeUnit.Microsecond },
        { Proto.TimeUnit.Nanosecond, TimeUnit.Nanosecond },
    };

    [Theory]
    [MemberData(nameof(Time64Units))]
    public void Time64_RoundTrips(Proto.TimeUnit protoUnit, TimeUnit expectedUnit)
    {
        // Arrange
        var proto = new Proto.ArrowType { TIME64 = protoUnit };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var time64Type = Assert.IsType<Time64Type>(arrow);
        Assert.Equal(expectedUnit, time64Type.Unit);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.TIME64, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.TIME64);
    }

    #endregion

    #region Interval round-trip

    public static TheoryData<Proto.IntervalUnit, IntervalUnit> IntervalUnits => new()
    {
        { Proto.IntervalUnit.YearMonth, IntervalUnit.YearMonth },
        { Proto.IntervalUnit.DayTime, IntervalUnit.DayTime },
        { Proto.IntervalUnit.MonthDayNano, IntervalUnit.MonthDayNanosecond },
    };

    [Theory]
    [MemberData(nameof(IntervalUnits))]
    public void Interval_RoundTrips(Proto.IntervalUnit protoUnit, IntervalUnit expectedUnit)
    {
        // Arrange
        var proto = new Proto.ArrowType { INTERVAL = protoUnit };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var intervalType = Assert.IsType<IntervalType>(arrow);
        Assert.Equal(expectedUnit, intervalType.Unit);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.INTERVAL, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(protoUnit, roundTripped.INTERVAL);
    }

    #endregion

    #region Decimal round-trip

    [Fact]
    public void Decimal128_RoundTrips()
    {
        // Arrange
        var proto = new Proto.ArrowType
        {
            DECIMAL = new Proto.Decimal { Precision = 38, Scale = 10 }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var decimalType = Assert.IsType<Decimal128Type>(arrow);
        Assert.Equal(38, decimalType.Precision);
        Assert.Equal(10, decimalType.Scale);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.DECIMAL, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(38u, roundTripped.DECIMAL.Precision);
        Assert.Equal(10, roundTripped.DECIMAL.Scale);
    }

    [Fact]
    public void Decimal256_RoundTrips()
    {
        // Arrange
        var proto = new Proto.ArrowType
        {
            DECIMAL256 = new Proto.Decimal256Type { Precision = 76, Scale = 20 }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var decimalType = Assert.IsType<Decimal256Type>(arrow);
        Assert.Equal(76, decimalType.Precision);
        Assert.Equal(20, decimalType.Scale);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.DECIMAL256, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(76u, roundTripped.DECIMAL256.Precision);
        Assert.Equal(20, roundTripped.DECIMAL256.Scale);
    }

    #endregion

    #region List round-trip

    [Fact]
    public void List_RoundTrips()
    {
        // Arrange
        var proto = new Proto.ArrowType
        {
            LIST = new Proto.List
            {
                FieldType = new Proto.Field
                {
                    Name = "item",
                    ArrowType = new Proto.ArrowType { INT32 = new Proto.EmptyMessage() },
                    Nullable = true
                }
            }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var listType = Assert.IsType<ListType>(arrow);
        Assert.Equal("item", listType.ValueField.Name);
        Assert.Equal(ArrowTypeId.Int32, listType.ValueDataType.TypeId);
        Assert.True(listType.ValueField.IsNullable);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.LIST, roundTripped.ArrowTypeEnumCase);
        Assert.Equal("item", roundTripped.LIST.FieldType.Name);
    }

    [Fact]
    public void LargeList_RoundTrips()
    {
        // Arrange
        var proto = new Proto.ArrowType
        {
            LARGELIST = new Proto.List
            {
                FieldType = new Proto.Field
                {
                    Name = "item",
                    ArrowType = new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() },
                    Nullable = false
                }
            }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var largeListType = Assert.IsType<LargeListType>(arrow);
        Assert.Equal("item", largeListType.ValueField.Name);
        Assert.Equal(ArrowTypeId.String, largeListType.ValueDataType.TypeId);
        Assert.False(largeListType.ValueField.IsNullable);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.LARGELIST, roundTripped.ArrowTypeEnumCase);
    }

    [Fact]
    public void FixedSizeList_RoundTrips()
    {
        // Arrange
        var proto = new Proto.ArrowType
        {
            FIXEDSIZELIST = new Proto.FixedSizeList
            {
                FieldType = new Proto.Field
                {
                    Name = "element",
                    ArrowType = new Proto.ArrowType { FLOAT64 = new Proto.EmptyMessage() },
                    Nullable = true
                },
                ListSize = 5
            }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var fixedListType = Assert.IsType<FixedSizeListType>(arrow);
        Assert.Equal("element", fixedListType.ValueField.Name);
        Assert.Equal(ArrowTypeId.Double, fixedListType.ValueDataType.TypeId);
        Assert.Equal(5, fixedListType.ListSize);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZELIST, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(5, roundTripped.FIXEDSIZELIST.ListSize);
    }

    #endregion

    #region Map round-trip

    [Fact]
    public void Map_RoundTrips()
    {
        // Arrange
        var entriesField = new Proto.Field
        {
            Name = "entries",
            ArrowType = new Proto.ArrowType
            {
                STRUCT = new Proto.Struct()
            },
            Nullable = false
        };
        entriesField.Children.Add(new Proto.Field
        {
            Name = "key",
            ArrowType = new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() },
            Nullable = false
        });
        entriesField.Children.Add(new Proto.Field
        {
            Name = "value",
            ArrowType = new Proto.ArrowType { INT32 = new Proto.EmptyMessage() },
            Nullable = true
        });

        var proto = new Proto.ArrowType
        {
            MAP = new Proto.Map
            {
                FieldType = entriesField,
                KeysSorted = false
            }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        Assert.IsType<MapType>(arrow);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.MAP, roundTripped.ArrowTypeEnumCase);
        Assert.Equal("entries", roundTripped.MAP.FieldType.Name);
    }

    #endregion

    #region Struct round-trip

    [Fact]
    public void Struct_RoundTrips()
    {
        // Arrange
        var proto = new Proto.ArrowType { STRUCT = new Proto.Struct() };
        var children = new List<Proto.Field>
        {
            new()
            {
                Name = "name",
                ArrowType = new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() },
                Nullable = false
            },
            new()
            {
                Name = "age",
                ArrowType = new Proto.ArrowType { INT32 = new Proto.EmptyMessage() },
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
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.STRUCT, roundTripped.ArrowTypeEnumCase);
    }

    #endregion

    #region Union round-trip

    [Fact]
    public void DenseUnion_RoundTrips()
    {
        // Arrange
        var union = new Proto.Union { UnionMode = Proto.UnionMode.Dense };
        union.TypeIds.AddRange([0, 1]);
        var proto = new Proto.ArrowType { UNION = union };
        var children = new List<Proto.Field>
        {
            new()
            {
                Name = "int_field",
                ArrowType = new Proto.ArrowType { INT32 = new Proto.EmptyMessage() },
                Nullable = true
            },
            new()
            {
                Name = "str_field",
                ArrowType = new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() },
                Nullable = true
            }
        };

        // Act
        var arrow = proto.ToArrow(children);
        var roundTripped = arrow.ToProto();

        // Assert
        var unionType = Assert.IsType<UnionType>(arrow);
        Assert.Equal(UnionMode.Dense, unionType.Mode);
        Assert.Equal([0, 1], unionType.TypeIds);
        Assert.Equal(2, unionType.Fields.Count);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.UNION, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(Proto.UnionMode.Dense, roundTripped.UNION.UnionMode);
        Assert.Equal([0, 1], roundTripped.UNION.TypeIds);
    }

    [Fact]
    public void SparseUnion_RoundTrips()
    {
        // Arrange
        var union = new Proto.Union { UnionMode = Proto.UnionMode.Sparse };
        union.TypeIds.AddRange([0, 1, 2]);
        var proto = new Proto.ArrowType { UNION = union };
        var children = new List<Proto.Field>
        {
            new()
            {
                Name = "a",
                ArrowType = new Proto.ArrowType { BOOL = new Proto.EmptyMessage() },
                Nullable = true
            },
            new()
            {
                Name = "b",
                ArrowType = new Proto.ArrowType { FLOAT32 = new Proto.EmptyMessage() },
                Nullable = true
            },
            new()
            {
                Name = "c",
                ArrowType = new Proto.ArrowType { INT64 = new Proto.EmptyMessage() },
                Nullable = true
            }
        };

        // Act
        var arrow = proto.ToArrow(children);
        var roundTripped = arrow.ToProto();

        // Assert
        var unionType = Assert.IsType<UnionType>(arrow);
        Assert.Equal(UnionMode.Sparse, unionType.Mode);
        Assert.Equal([0, 1, 2], unionType.TypeIds);
        Assert.Equal(3, unionType.Fields.Count);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.UNION, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(Proto.UnionMode.Sparse, roundTripped.UNION.UnionMode);
    }

    #endregion

    #region Dictionary round-trip

    [Fact]
    public void Dictionary_RoundTrips()
    {
        // Arrange
        var proto = new Proto.ArrowType
        {
            DICTIONARY = new Proto.Dictionary
            {
                Key = new Proto.ArrowType { INT32 = new Proto.EmptyMessage() },
                Value = new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() }
            }
        };

        // Act
        var arrow = proto.ToArrow([]);
        var roundTripped = arrow.ToProto();

        // Assert
        var dictType = Assert.IsType<DictionaryType>(arrow);
        Assert.Equal(ArrowTypeId.Int32, dictType.IndexType.TypeId);
        Assert.Equal(ArrowTypeId.String, dictType.ValueType.TypeId);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.DICTIONARY, roundTripped.ArrowTypeEnumCase);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.INT32, roundTripped.DICTIONARY.Key.ArrowTypeEnumCase);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.UTF8, roundTripped.DICTIONARY.Value.ArrowTypeEnumCase);
    }

    #endregion

    #region Schema round-trip (Proto → Arrow → Proto)

    [Fact]
    public void Schema_ProtoToArrowToProto_RoundTrips()
    {
        // Arrange
        var protoSchema = new Proto.Schema();
        protoSchema.Metadata.Add("created_by", "test");

        var idField = new Proto.Field
        {
            Name = "id",
            ArrowType = new Proto.ArrowType { INT64 = new Proto.EmptyMessage() },
            Nullable = false
        };
        protoSchema.Columns.Add(idField);

        var nameField = new Proto.Field
        {
            Name = "name",
            ArrowType = new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() },
            Nullable = true
        };
        nameField.Metadata.Add("description", "user name");
        protoSchema.Columns.Add(nameField);

        var scoresField = new Proto.Field
        {
            Name = "scores",
            ArrowType = new Proto.ArrowType
            {
                LIST = new Proto.List
                {
                    FieldType = new Proto.Field
                    {
                        Name = "item",
                        ArrowType = new Proto.ArrowType { FLOAT64 = new Proto.EmptyMessage() },
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
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.INT64, roundTripped.Columns[0].ArrowType.ArrowTypeEnumCase);
        Assert.False(roundTripped.Columns[0].Nullable);
        Assert.Equal("name", roundTripped.Columns[1].Name);
        Assert.True(roundTripped.Columns[1].Nullable);
        Assert.Contains("description", roundTripped.Columns[1].Metadata.Keys);
        Assert.Equal("user name", roundTripped.Columns[1].Metadata["description"]);
        Assert.Equal("scores", roundTripped.Columns[2].Name);
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.LIST, roundTripped.Columns[2].ArrowType.ArrowTypeEnumCase);
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
        var structField = new Proto.Field
        {
            Name = "address",
            ArrowType = new Proto.ArrowType { STRUCT = new Proto.Struct() },
            Nullable = true
        };
        structField.Children.Add(new Proto.Field
        {
            Name = "city",
            ArrowType = new Proto.ArrowType { UTF8 = new Proto.EmptyMessage() },
            Nullable = true
        });
        structField.Children.Add(new Proto.Field
        {
            Name = "zip",
            ArrowType = new Proto.ArrowType { INT32 = new Proto.EmptyMessage() },
            Nullable = false
        });

        var protoSchema = new Proto.Schema();
        protoSchema.Columns.Add(new Proto.Field
        {
            Name = "id",
            ArrowType = new Proto.ArrowType { INT64 = new Proto.EmptyMessage() },
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
        Assert.Equal(Proto.ArrowType.ArrowTypeEnumOneofCase.STRUCT, roundTripped.Columns[1].ArrowType.ArrowTypeEnumCase);
        Assert.Equal(2, roundTripped.Columns[1].Children.Count);
        Assert.Equal("city", roundTripped.Columns[1].Children[0].Name);
        Assert.Equal("zip", roundTripped.Columns[1].Children[1].Name);
    }

    #endregion
}
