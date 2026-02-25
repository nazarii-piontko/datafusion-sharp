using Apache.Arrow.Types;

namespace DataFusionSharp.Proto;

/// <summary>
/// Extension methods for converting DataFusion types to Apache Arrow types.
/// </summary>
internal static class ArrowExtensions
{
    /// <summary>
    /// Converts a DataFusion schema to an Apache Arrow schema.
    /// </summary>
    /// <param name="schema">The DataFusion schema to convert.</param>
    /// <returns>The corresponding Apache Arrow schema.</returns>
    public static Apache.Arrow.Schema ToArrow(this Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        
        var columns = schema.Columns.Select(c =>
        {
            return new Apache.Arrow.Field(
                c.Name,
                c.ArrowType.ToArrow(c.Children),
                c.Nullable,
                c.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            );
        });
        var metadata = schema.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value));
        return new Apache.Arrow.Schema(columns, metadata);
    }
    
    /// <summary>
    /// Converts a DataFusion ArrowType to an Apache Arrow field type, recursively converting any child fields as well.
    /// </summary>
    /// <param name="type">The DataFusion ArrowType to convert.</param>
    /// <param name="children">The child fields of the schema field, if any. This is used for nested types like structs and lists.</param>
    /// <returns>The corresponding Apache Arrow field type.</returns>
    public static IArrowType ToArrow(this ArrowType type, IEnumerable<Field> children)
    {
        ArgumentNullException.ThrowIfNull(type);
        
        return type.ArrowTypeEnumCase switch
        {
            ArrowType.ArrowTypeEnumOneofCase.NONE => NullType.Default,
            ArrowType.ArrowTypeEnumOneofCase.BOOL => BooleanType.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT8 => UInt8Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT8 => Int8Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT16 => UInt16Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT16 => Int16Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT32 => UInt32Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT32 => Int32Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT64 => UInt64Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT64 => Int64Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.FLOAT16 => HalfFloatType.Default,
            ArrowType.ArrowTypeEnumOneofCase.FLOAT32 => FloatType.Default,
            ArrowType.ArrowTypeEnumOneofCase.FLOAT64 => DoubleType.Default,
            ArrowType.ArrowTypeEnumOneofCase.UTF8 => StringType.Default,
            ArrowType.ArrowTypeEnumOneofCase.UTF8VIEW => StringViewType.Default,
            ArrowType.ArrowTypeEnumOneofCase.LARGEUTF8 => LargeStringType.Default,
            ArrowType.ArrowTypeEnumOneofCase.BINARY => BinaryType.Default,
            ArrowType.ArrowTypeEnumOneofCase.BINARYVIEW => BinaryViewType.Default,
            ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZEBINARY => new FixedSizeBinaryType(type.FIXEDSIZEBINARY),
            ArrowType.ArrowTypeEnumOneofCase.LARGEBINARY => LargeBinaryType.Default,
            ArrowType.ArrowTypeEnumOneofCase.DATE32 => Date32Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.DATE64 => Date64Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.DURATION => ToArrowDuration(type),
            ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP => ToArrowTimestamp(type),
            ArrowType.ArrowTypeEnumOneofCase.TIME32 => ToArrowTime32(type),
            ArrowType.ArrowTypeEnumOneofCase.TIME64 => ToArrowTime64(type),
            ArrowType.ArrowTypeEnumOneofCase.INTERVAL => ToArrowInterval(type),
            ArrowType.ArrowTypeEnumOneofCase.DECIMAL => new Decimal128Type((int)type.DECIMAL.Precision, type.DECIMAL.Scale),
            ArrowType.ArrowTypeEnumOneofCase.DECIMAL256 => new Apache.Arrow.Types.Decimal256Type((int)type.DECIMAL256.Precision, type.DECIMAL256.Scale),
            ArrowType.ArrowTypeEnumOneofCase.LIST => ToArrowList(type),
            ArrowType.ArrowTypeEnumOneofCase.LARGELIST => ToArrowLargeList(type),
            ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZELIST => ToArrowFixedSizeList(type),
            ArrowType.ArrowTypeEnumOneofCase.STRUCT => ToArrowStruct(children),
            ArrowType.ArrowTypeEnumOneofCase.UNION => ToArrowUnion(type, children),
            ArrowType.ArrowTypeEnumOneofCase.DICTIONARY => ToArrowDictionary(type),
            ArrowType.ArrowTypeEnumOneofCase.MAP => ToArrowMap(type),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.ArrowTypeEnumCase, "Unknown ArrowType enum case")
        };
    }

    private static DurationType ToArrowDuration(ArrowType type)
    {
        return type.DURATION switch
        {
            TimeUnit.Second => DurationType.Second,
            TimeUnit.Millisecond => DurationType.Millisecond,
            TimeUnit.Microsecond => DurationType.Microsecond,
            TimeUnit.Nanosecond => DurationType.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.DURATION, "Unknown Duration TimeUnit")
        };
    }

    private static TimestampType ToArrowTimestamp(ArrowType type)
    {
        return type.TIMESTAMP.TimeUnit switch
        {
            TimeUnit.Second => new TimestampType(Apache.Arrow.Types.TimeUnit.Second, type.TIMESTAMP.Timezone),
            TimeUnit.Millisecond => new TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, type.TIMESTAMP.Timezone),
            TimeUnit.Microsecond => new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, type.TIMESTAMP.Timezone),
            TimeUnit.Nanosecond => new TimestampType(Apache.Arrow.Types.TimeUnit.Nanosecond, type.TIMESTAMP.Timezone),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIMESTAMP.TimeUnit, "Unknown Timestamp TimeUnit")
        };
    }

    private static Time32Type ToArrowTime32(ArrowType type)
    {
        return type.TIME32 switch
        {
            TimeUnit.Second => TimeType.Second,
            TimeUnit.Millisecond => TimeType.Millisecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIME32, "Unknown TimeUnit for TIME32")
        };
    }

    private static Time64Type ToArrowTime64(ArrowType type)
    {
        return type.TIME64 switch
        {
            TimeUnit.Microsecond => TimeType.Microsecond,
            TimeUnit.Nanosecond => TimeType.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIME64, "Unknown TimeUnit for TIME64")
        };
    }

    private static IntervalType ToArrowInterval(ArrowType type)
    {
        return type.INTERVAL switch
        {
            IntervalUnit.YearMonth => IntervalType.YearMonth,
            IntervalUnit.DayTime => IntervalType.DayTime,
            IntervalUnit.MonthDayNano => IntervalType.MonthDayNanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.INTERVAL, "Unknown IntervalUnit for INTERVAL")
        };
    }

    private static ListType ToArrowList(ArrowType type)
    {
        return new ListType(
            new Apache.Arrow.Field(
                type.LIST.FieldType.Name,
                type.LIST.FieldType.ArrowType.ToArrow(type.LIST.FieldType.Children),
                type.LIST.FieldType.Nullable,
                type.LIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            )
        );
    }

    private static LargeListType ToArrowLargeList(ArrowType type)
    {
        return new LargeListType(
            new Apache.Arrow.Field(
                type.LARGELIST.FieldType.Name,
                type.LARGELIST.FieldType.ArrowType.ToArrow(type.LARGELIST.FieldType.Children),
                type.LARGELIST.FieldType.Nullable,
                type.LARGELIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            )
        );
    }

    private static FixedSizeListType ToArrowFixedSizeList(ArrowType type)
    {
        return new FixedSizeListType(
            new Apache.Arrow.Field(
                type.FIXEDSIZELIST.FieldType.Name,
                type.FIXEDSIZELIST.FieldType.ArrowType.ToArrow(type.FIXEDSIZELIST.FieldType.Children),
                type.FIXEDSIZELIST.FieldType.Nullable,
                type.FIXEDSIZELIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            ),
            type.FIXEDSIZELIST.ListSize
        );
    }

    private static StructType ToArrowStruct(IEnumerable<Field> children)
    {
        return new StructType(
            children.Select(f => new Apache.Arrow.Field(
                f.Name,
                f.ArrowType.ToArrow(f.Children),
                f.Nullable,
                f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            )).ToList()
        );
    }

    private static UnionType ToArrowUnion(ArrowType type, IEnumerable<Field> children)
    {
        return type.UNION.UnionMode switch
        {
            UnionMode.Dense => new UnionType(
                children.Select(f => new Apache.Arrow.Field(
                    f.Name,
                    f.ArrowType.ToArrow(f.Children),
                    f.Nullable,
                    f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                )).ToList(),
                type.UNION.TypeIds,
                Apache.Arrow.Types.UnionMode.Dense
            ),
            UnionMode.Sparse => new UnionType(
                children.Select(f => new Apache.Arrow.Field(
                    f.Name,
                    f.ArrowType.ToArrow(f.Children),
                    f.Nullable,
                    f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                )).ToList(),
                type.UNION.TypeIds
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.UNION.UnionMode, "Unknown UnionMode for UNION")
        };
    }

    private static DictionaryType ToArrowDictionary(ArrowType type)
    {
        return new DictionaryType(
            type.DICTIONARY.Key.ToArrow(Enumerable.Empty<Field>()),
            type.DICTIONARY.Value.ToArrow(Enumerable.Empty<Field>()),
            ordered: false
        );
    }

    private static MapType ToArrowMap(ArrowType type)
    {
        return new MapType(
            new Apache.Arrow.Field(
                type.MAP.FieldType.Name,
                type.MAP.FieldType.ArrowType.ToArrow(type.MAP.FieldType.Children),
                type.MAP.FieldType.Nullable,
                type.MAP.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            ),
            type.MAP.KeysSorted
        );
    }

    /// <summary>
    /// Converts an Apache Arrow schema to a DataFusion schema.
    /// </summary>
    /// <param name="schema">The Apache Arrow schema to convert.</param>
    /// <returns>The corresponding DataFusion schema.</returns>
    public static Schema ToProto(this Apache.Arrow.Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        
        var protoSchema = new Schema();
        
        foreach (var field in schema.FieldsList)
        {
            var protoField = new Field
            {
                Name = field.Name,
                ArrowType = field.DataType.ToProto(),
                Nullable = field.IsNullable
            };
            
            if (field.Metadata != null)
            {
                foreach (var kv in field.Metadata)
                {
                    protoField.Metadata.Add(kv.Key, kv.Value);
                }
            }
            
            // Add children for nested types
            AddChildrenFromArrowType(field.DataType, protoField);
            
            protoSchema.Columns.Add(protoField);
        }
        
        if (schema.Metadata != null)
        {
            foreach (var kv in schema.Metadata)
            {
                protoSchema.Metadata.Add(kv.Key, kv.Value);
            }
        }
        
        return protoSchema;
    }

    /// <summary>
    /// Converts an Apache Arrow field type to a DataFusion ArrowType proto message.
    /// </summary>
    /// <param name="arrowType">The Apache Arrow type to convert.</param>
    /// <returns>The corresponding DataFusion ArrowType proto message.</returns>
    public static ArrowType ToProto(this IArrowType arrowType)
    {
        ArgumentNullException.ThrowIfNull(arrowType);
        
        var protoType = new ArrowType();
        
        switch (arrowType.TypeId)
        {
            case ArrowTypeId.Null:
                protoType.NONE = new EmptyMessage();
                break;
            case ArrowTypeId.Boolean:
                protoType.BOOL = new EmptyMessage();
                break;
            case ArrowTypeId.UInt8:
                protoType.UINT8 = new EmptyMessage();
                break;
            case ArrowTypeId.Int8:
                protoType.INT8 = new EmptyMessage();
                break;
            case ArrowTypeId.UInt16:
                protoType.UINT16 = new EmptyMessage();
                break;
            case ArrowTypeId.Int16:
                protoType.INT16 = new EmptyMessage();
                break;
            case ArrowTypeId.UInt32:
                protoType.UINT32 = new EmptyMessage();
                break;
            case ArrowTypeId.Int32:
                protoType.INT32 = new EmptyMessage();
                break;
            case ArrowTypeId.UInt64:
                protoType.UINT64 = new EmptyMessage();
                break;
            case ArrowTypeId.Int64:
                protoType.INT64 = new EmptyMessage();
                break;
            case ArrowTypeId.HalfFloat:
                protoType.FLOAT16 = new EmptyMessage();
                break;
            case ArrowTypeId.Float:
                protoType.FLOAT32 = new EmptyMessage();
                break;
            case ArrowTypeId.Double:
                protoType.FLOAT64 = new EmptyMessage();
                break;
            case ArrowTypeId.String:
                protoType.UTF8 = new EmptyMessage();
                break;
            case ArrowTypeId.Binary:
            case ArrowTypeId.FixedSizedBinary:
                ToProtoBinary(arrowType, protoType);
                break;
            case ArrowTypeId.Date32:
                protoType.DATE32 = new EmptyMessage();
                break;
            case ArrowTypeId.Date64:
                protoType.DATE64 = new EmptyMessage();
                break;
            case ArrowTypeId.Timestamp:
                ToProtoTimestamp(arrowType, protoType);
                break;
            case ArrowTypeId.Time32:
                ToProtoTime32(arrowType, protoType);
                break;
            case ArrowTypeId.Time64:
                ToProtoTime64(arrowType, protoType);
                break;
            case ArrowTypeId.Interval:
                ToProtoInterval(arrowType, protoType);
                break;
            case ArrowTypeId.Duration:
                ToProtoDuration(arrowType, protoType);
                break;
            case ArrowTypeId.Decimal128:
                var decimal128 = (Decimal128Type)arrowType;
                protoType.DECIMAL = new Decimal
                {
                    Precision = (uint)decimal128.Precision,
                    Scale = decimal128.Scale
                };
                break;
            case ArrowTypeId.Decimal256:
                var decimal256 = (Apache.Arrow.Types.Decimal256Type)arrowType;
                protoType.DECIMAL256 = new Decimal256Type
                {
                    Precision = (uint)decimal256.Precision,
                    Scale = decimal256.Scale
                };
                break;
            case ArrowTypeId.List:
                var list = (ListType)arrowType;
                protoType.LIST = new List
                {
                    FieldType = FieldToProto(list.ValueField)
                };
                break;
            case ArrowTypeId.Struct:
                protoType.STRUCT = new Struct();
                // Children are handled separately
                break;
            case ArrowTypeId.Union:
                ToProtoUnion(arrowType, protoType);
                break;
            case ArrowTypeId.Dictionary:
                var dictionary = (DictionaryType)arrowType;
                protoType.DICTIONARY = new Dictionary
                {
                    Key = dictionary.IndexType.ToProto(),
                    Value = dictionary.ValueType.ToProto()
                };
                break;
            case ArrowTypeId.Map:
                var map = (MapType)arrowType;
                protoType.MAP = new Map
                {
                    FieldType = FieldToProto(map.Fields[0]),
                    KeysSorted = false  // Apache Arrow C# MapType doesn't expose this property
                };
                break;
            case ArrowTypeId.FixedSizeList:
                var fixedSizeList = (FixedSizeListType)arrowType;
                protoType.FIXEDSIZELIST = new FixedSizeList
                {
                    FieldType = FieldToProto(fixedSizeList.ValueField),
                    ListSize = fixedSizeList.ListSize
                };
                break;
            case ArrowTypeId.LargeBinary:
                protoType.LARGEBINARY = new EmptyMessage();
                break;
            case ArrowTypeId.LargeString:
                protoType.LARGEUTF8 = new EmptyMessage();
                break;
            case ArrowTypeId.LargeList:
                var largeList = (LargeListType)arrowType;
                protoType.LARGELIST = new List
                {
                    FieldType = FieldToProto(largeList.ValueField)
                };
                break;
            case ArrowTypeId.BinaryView:
                protoType.BINARYVIEW = new EmptyMessage();
                break;
            case ArrowTypeId.StringView:
                protoType.UTF8VIEW = new EmptyMessage();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(arrowType), arrowType.TypeId, "Unsupported ArrowTypeId");
        }
        
        return protoType;
    }

    private static void ToProtoBinary(IArrowType arrowType, ArrowType protoType)
    {
        if (arrowType is FixedSizeBinaryType fixedSizeBinary)
            protoType.FIXEDSIZEBINARY = fixedSizeBinary.ByteWidth;
        else
            protoType.BINARY = new EmptyMessage();
    }

    private static void ToProtoTimestamp(IArrowType arrowType, ArrowType protoType)
    {
        var timestamp = (TimestampType)arrowType;
        protoType.TIMESTAMP = new Timestamp
        {
            TimeUnit = timestamp.Unit switch
            {
                Apache.Arrow.Types.TimeUnit.Second => TimeUnit.Second,
                Apache.Arrow.Types.TimeUnit.Millisecond => TimeUnit.Millisecond,
                Apache.Arrow.Types.TimeUnit.Microsecond => TimeUnit.Microsecond,
                Apache.Arrow.Types.TimeUnit.Nanosecond => TimeUnit.Nanosecond,
                _ => throw new ArgumentOutOfRangeException(nameof(arrowType), timestamp.Unit, "Unknown TimeUnit for TimestampType")
            },
            Timezone = timestamp.Timezone ?? string.Empty
        };
    }

    private static void ToProtoTime32(IArrowType arrowType, ArrowType protoType)
    {
        var time32 = (Time32Type)arrowType;
        protoType.TIME32 = time32.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => TimeUnit.Second,
            Apache.Arrow.Types.TimeUnit.Millisecond => TimeUnit.Millisecond,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), time32.Unit, "Unknown TimeUnit for Time32Type")
        };
    }

    private static void ToProtoTime64(IArrowType arrowType, ArrowType protoType)
    {
        var time64 = (Time64Type)arrowType;
        protoType.TIME64 = time64.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Microsecond => TimeUnit.Microsecond,
            Apache.Arrow.Types.TimeUnit.Nanosecond => TimeUnit.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), time64.Unit, "Unknown TimeUnit for Time64Type")
        };
    }

    private static void ToProtoInterval(IArrowType arrowType, ArrowType protoType)
    {
        var interval = (IntervalType)arrowType;
        protoType.INTERVAL = interval.Unit switch
        {
            Apache.Arrow.Types.IntervalUnit.YearMonth => IntervalUnit.YearMonth,
            Apache.Arrow.Types.IntervalUnit.DayTime => IntervalUnit.DayTime,
            Apache.Arrow.Types.IntervalUnit.MonthDayNanosecond => IntervalUnit.MonthDayNano,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), interval.Unit, "Unknown IntervalUnit for IntervalType")
        };
    }

    private static void ToProtoDuration(IArrowType arrowType, ArrowType protoType)
    {
        var duration = (DurationType)arrowType;
        protoType.DURATION = duration.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => TimeUnit.Second,
            Apache.Arrow.Types.TimeUnit.Millisecond => TimeUnit.Millisecond,
            Apache.Arrow.Types.TimeUnit.Microsecond => TimeUnit.Microsecond,
            Apache.Arrow.Types.TimeUnit.Nanosecond => TimeUnit.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), duration.Unit, "Unknown TimeUnit for DurationType")
        };
    }

    private static void ToProtoUnion(IArrowType arrowType, ArrowType protoType)
    {
        var union = (UnionType)arrowType;
        protoType.UNION = new Union
        {
            UnionMode = union.Mode switch
            {
                Apache.Arrow.Types.UnionMode.Dense => UnionMode.Dense,
                Apache.Arrow.Types.UnionMode.Sparse => UnionMode.Sparse,
                _ => throw new ArgumentOutOfRangeException(nameof(arrowType), union.Mode, "Unknown UnionMode for UnionType")
            }
        };
        protoType.UNION.TypeIds.AddRange(union.TypeIds);
        // Children are handled separately
    }

    private static Field FieldToProto(Apache.Arrow.Field field)
    {
        var protoField = new Field
        {
            Name = field.Name,
            ArrowType = field.DataType.ToProto(),
            Nullable = field.IsNullable
        };
        
        if (field.Metadata != null)
        {
            foreach (var kv in field.Metadata)
            {
                protoField.Metadata.Add(kv.Key, kv.Value);
            }
        }
        
        AddChildrenFromArrowType(field.DataType, protoField);
        
        return protoField;
    }

    private static void AddChildrenFromArrowType(IArrowType arrowType, Field protoField)
    {
        switch (arrowType.TypeId)
        {
            case ArrowTypeId.Struct:
                var structType = (StructType)arrowType;
                foreach (var childField in structType.Fields)
                {
                    protoField.Children.Add(FieldToProto(childField));
                }
                break;
            case ArrowTypeId.Union:
                var unionType = (UnionType)arrowType;
                foreach (var childField in unionType.Fields)
                {
                    protoField.Children.Add(FieldToProto(childField));
                }
                break;
        }
    }
}