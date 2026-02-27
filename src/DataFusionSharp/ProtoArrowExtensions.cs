using Apache.Arrow;
using Apache.Arrow.Types;

namespace DataFusionSharp;

/// <summary>
/// Extension methods for converting DataFusion types to Apache Arrow types.
/// </summary>
internal static class ProtoArrowExtensions
{
    /// <summary>
    /// Converts a DataFusion schema to an Apache Arrow schema.
    /// </summary>
    /// <param name="schema">The DataFusion schema to convert.</param>
    /// <returns>The corresponding Apache Arrow schema.</returns>
    public static Schema ToArrow(this Proto.Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        
        var columns = schema.Columns.Select(c =>
        {
            return new Field(
                c.Name,
                c.ArrowType.ToArrow(c.Children),
                c.Nullable,
                c.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            );
        });
        var metadata = schema.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value));
        return new Schema(columns, metadata);
    }
    
    /// <summary>
    /// Converts a DataFusion ArrowType to an Apache Arrow field type, recursively converting any child fields as well.
    /// </summary>
    /// <param name="type">The DataFusion ArrowType to convert.</param>
    /// <param name="children">The child fields of the schema field, if any. This is used for nested types like structs and lists.</param>
    /// <returns>The corresponding Apache Arrow field type.</returns>
    public static IArrowType ToArrow(this Proto.ArrowType type, IEnumerable<Proto.Field> children)
    {
        ArgumentNullException.ThrowIfNull(type);
        
        return type.ArrowTypeEnumCase switch
        {
            Proto.ArrowType.ArrowTypeEnumOneofCase.NONE => NullType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.BOOL => BooleanType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.UINT8 => UInt8Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.INT8 => Int8Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.UINT16 => UInt16Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.INT16 => Int16Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.UINT32 => UInt32Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.INT32 => Int32Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.UINT64 => UInt64Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.INT64 => Int64Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.FLOAT16 => HalfFloatType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.FLOAT32 => FloatType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.FLOAT64 => DoubleType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.UTF8 => StringType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.UTF8VIEW => StringViewType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.LARGEUTF8 => LargeStringType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.BINARY => BinaryType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.BINARYVIEW => BinaryViewType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZEBINARY => new FixedSizeBinaryType(type.FIXEDSIZEBINARY),
            Proto.ArrowType.ArrowTypeEnumOneofCase.LARGEBINARY => LargeBinaryType.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.DATE32 => Date32Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.DATE64 => Date64Type.Default,
            Proto.ArrowType.ArrowTypeEnumOneofCase.DURATION => ToArrowDuration(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP => ToArrowTimestamp(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.TIME32 => ToArrowTime32(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.TIME64 => ToArrowTime64(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.INTERVAL => ToArrowInterval(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.DECIMAL => new Decimal128Type((int)type.DECIMAL.Precision, type.DECIMAL.Scale),
            Proto.ArrowType.ArrowTypeEnumOneofCase.DECIMAL256 => new Decimal256Type((int)type.DECIMAL256.Precision, type.DECIMAL256.Scale),
            Proto.ArrowType.ArrowTypeEnumOneofCase.LIST => ToArrowList(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.LARGELIST => ToArrowLargeList(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZELIST => ToArrowFixedSizeList(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.STRUCT => ToArrowStruct(children),
            Proto.ArrowType.ArrowTypeEnumOneofCase.UNION => ToArrowUnion(type, children),
            Proto.ArrowType.ArrowTypeEnumOneofCase.DICTIONARY => ToArrowDictionary(type),
            Proto.ArrowType.ArrowTypeEnumOneofCase.MAP => ToArrowMap(type),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.ArrowTypeEnumCase, "Unknown ArrowType enum case")
        };
    }

    private static DurationType ToArrowDuration(Proto.ArrowType type)
    {
        return type.DURATION switch
        {
            Proto.TimeUnit.Second => DurationType.Second,
            Proto.TimeUnit.Millisecond => DurationType.Millisecond,
            Proto.TimeUnit.Microsecond => DurationType.Microsecond,
            Proto.TimeUnit.Nanosecond => DurationType.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.DURATION, "Unknown Duration TimeUnit")
        };
    }

    private static TimestampType ToArrowTimestamp(Proto.ArrowType type)
    {
        return type.TIMESTAMP.TimeUnit switch
        {
            Proto.TimeUnit.Second => new TimestampType(TimeUnit.Second, type.TIMESTAMP.Timezone),
            Proto.TimeUnit.Millisecond => new TimestampType(TimeUnit.Millisecond, type.TIMESTAMP.Timezone),
            Proto.TimeUnit.Microsecond => new TimestampType(TimeUnit.Microsecond, type.TIMESTAMP.Timezone),
            Proto.TimeUnit.Nanosecond => new TimestampType(TimeUnit.Nanosecond, type.TIMESTAMP.Timezone),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIMESTAMP.TimeUnit, "Unknown Timestamp TimeUnit")
        };
    }

    private static Time32Type ToArrowTime32(Proto.ArrowType type)
    {
        return type.TIME32 switch
        {
            Proto.TimeUnit.Second => TimeType.Second,
            Proto.TimeUnit.Millisecond => TimeType.Millisecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIME32, "Unknown TimeUnit for TIME32")
        };
    }

    private static Time64Type ToArrowTime64(Proto.ArrowType type)
    {
        return type.TIME64 switch
        {
            Proto.TimeUnit.Microsecond => TimeType.Microsecond,
            Proto.TimeUnit.Nanosecond => TimeType.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIME64, "Unknown TimeUnit for TIME64")
        };
    }

    private static IntervalType ToArrowInterval(Proto.ArrowType type)
    {
        return type.INTERVAL switch
        {
            Proto.IntervalUnit.YearMonth => IntervalType.YearMonth,
            Proto.IntervalUnit.DayTime => IntervalType.DayTime,
            Proto.IntervalUnit.MonthDayNano => IntervalType.MonthDayNanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.INTERVAL, "Unknown IntervalUnit for INTERVAL")
        };
    }

    private static ListType ToArrowList(Proto.ArrowType type)
    {
        return new ListType(
            new Field(
                type.LIST.FieldType.Name,
                type.LIST.FieldType.ArrowType.ToArrow(type.LIST.FieldType.Children),
                type.LIST.FieldType.Nullable,
                type.LIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            )
        );
    }

    private static LargeListType ToArrowLargeList(Proto.ArrowType type)
    {
        return new LargeListType(
            new Field(
                type.LARGELIST.FieldType.Name,
                type.LARGELIST.FieldType.ArrowType.ToArrow(type.LARGELIST.FieldType.Children),
                type.LARGELIST.FieldType.Nullable,
                type.LARGELIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            )
        );
    }

    private static FixedSizeListType ToArrowFixedSizeList(Proto.ArrowType type)
    {
        return new FixedSizeListType(
            new Field(
                type.FIXEDSIZELIST.FieldType.Name,
                type.FIXEDSIZELIST.FieldType.ArrowType.ToArrow(type.FIXEDSIZELIST.FieldType.Children),
                type.FIXEDSIZELIST.FieldType.Nullable,
                type.FIXEDSIZELIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            ),
            type.FIXEDSIZELIST.ListSize
        );
    }

    private static StructType ToArrowStruct(IEnumerable<Proto.Field> children)
    {
        return new StructType(
            children.Select(f => new Field(
                f.Name,
                f.ArrowType.ToArrow(f.Children),
                f.Nullable,
                f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
            )).ToList()
        );
    }

    private static UnionType ToArrowUnion(Proto.ArrowType type, IEnumerable<Proto.Field> children)
    {
        return type.UNION.UnionMode switch
        {
            Proto.UnionMode.Dense => new UnionType(
                children.Select(f => new Field(
                    f.Name,
                    f.ArrowType.ToArrow(f.Children),
                    f.Nullable,
                    f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                )).ToList(),
                type.UNION.TypeIds,
                UnionMode.Dense
            ),
            Proto.UnionMode.Sparse => new UnionType(
                children.Select(f => new Field(
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

    private static DictionaryType ToArrowDictionary(Proto.ArrowType type)
    {
        return new DictionaryType(
            type.DICTIONARY.Key.ToArrow([]),
            type.DICTIONARY.Value.ToArrow([]),
            ordered: false
        );
    }

    private static MapType ToArrowMap(Proto.ArrowType type)
    {
        return new MapType(
            new Field(
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
    public static Proto.Schema ToProto(this Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        
        var protoSchema = new Proto.Schema();
        
        foreach (var field in schema.FieldsList)
        {
            var protoField = new Proto.Field
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
    public static Proto.ArrowType ToProto(this IArrowType arrowType)
    {
        ArgumentNullException.ThrowIfNull(arrowType);
        
        var protoType = new Proto.ArrowType();
        
        switch (arrowType.TypeId)
        {
            case ArrowTypeId.Null:
                protoType.NONE = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Boolean:
                protoType.BOOL = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.UInt8:
                protoType.UINT8 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Int8:
                protoType.INT8 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.UInt16:
                protoType.UINT16 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Int16:
                protoType.INT16 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.UInt32:
                protoType.UINT32 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Int32:
                protoType.INT32 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.UInt64:
                protoType.UINT64 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Int64:
                protoType.INT64 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.HalfFloat:
                protoType.FLOAT16 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Float:
                protoType.FLOAT32 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Double:
                protoType.FLOAT64 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.String:
                protoType.UTF8 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Binary:
            case ArrowTypeId.FixedSizedBinary:
                ToProtoBinary(arrowType, protoType);
                break;
            case ArrowTypeId.Date32:
                protoType.DATE32 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.Date64:
                protoType.DATE64 = new Proto.EmptyMessage();
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
                protoType.DECIMAL = new Proto.Decimal
                {
                    Precision = (uint)decimal128.Precision,
                    Scale = decimal128.Scale
                };
                break;
            case ArrowTypeId.Decimal256:
                var decimal256 = (Decimal256Type)arrowType;
                protoType.DECIMAL256 = new Proto.Decimal256Type
                {
                    Precision = (uint)decimal256.Precision,
                    Scale = decimal256.Scale
                };
                break;
            case ArrowTypeId.List:
                var list = (ListType)arrowType;
                protoType.LIST = new Proto.List
                {
                    FieldType = FieldToProto(list.ValueField)
                };
                break;
            case ArrowTypeId.Struct:
                protoType.STRUCT = new Proto.Struct();
                // Children are handled separately
                break;
            case ArrowTypeId.Union:
                ToProtoUnion(arrowType, protoType);
                break;
            case ArrowTypeId.Dictionary:
                var dictionary = (DictionaryType)arrowType;
                protoType.DICTIONARY = new Proto.Dictionary
                {
                    Key = dictionary.IndexType.ToProto(),
                    Value = dictionary.ValueType.ToProto()
                };
                break;
            case ArrowTypeId.Map:
                var map = (MapType)arrowType;
                protoType.MAP = new Proto.Map
                {
                    FieldType = FieldToProto(map.Fields[0]),
                    KeysSorted = false  // Apache Arrow C# MapType doesn't expose this property
                };
                break;
            case ArrowTypeId.FixedSizeList:
                var fixedSizeList = (FixedSizeListType)arrowType;
                protoType.FIXEDSIZELIST = new Proto.FixedSizeList
                {
                    FieldType = FieldToProto(fixedSizeList.ValueField),
                    ListSize = fixedSizeList.ListSize
                };
                break;
            case ArrowTypeId.LargeBinary:
                protoType.LARGEBINARY = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.LargeString:
                protoType.LARGEUTF8 = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.LargeList:
                var largeList = (LargeListType)arrowType;
                protoType.LARGELIST = new Proto.List
                {
                    FieldType = FieldToProto(largeList.ValueField)
                };
                break;
            case ArrowTypeId.BinaryView:
                protoType.BINARYVIEW = new Proto.EmptyMessage();
                break;
            case ArrowTypeId.StringView:
                protoType.UTF8VIEW = new Proto.EmptyMessage();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(arrowType), arrowType.TypeId, "Unsupported ArrowTypeId");
        }
        
        return protoType;
    }

    private static void ToProtoBinary(IArrowType arrowType, Proto.ArrowType protoType)
    {
        if (arrowType is FixedSizeBinaryType fixedSizeBinary)
            protoType.FIXEDSIZEBINARY = fixedSizeBinary.ByteWidth;
        else
            protoType.BINARY = new Proto.EmptyMessage();
    }

    private static void ToProtoTimestamp(IArrowType arrowType, Proto.ArrowType protoType)
    {
        var timestamp = (TimestampType)arrowType;
        protoType.TIMESTAMP = new Proto.Timestamp
        {
            TimeUnit = timestamp.Unit switch
            {
                TimeUnit.Second => Proto.TimeUnit.Second,
                TimeUnit.Millisecond => Proto.TimeUnit.Millisecond,
                TimeUnit.Microsecond => Proto.TimeUnit.Microsecond,
                TimeUnit.Nanosecond => Proto.TimeUnit.Nanosecond,
                _ => throw new ArgumentOutOfRangeException(nameof(arrowType), timestamp.Unit, "Unknown TimeUnit for TimestampType")
            },
            Timezone = timestamp.Timezone ?? string.Empty
        };
    }

    private static void ToProtoTime32(IArrowType arrowType, Proto.ArrowType protoType)
    {
        var time32 = (Time32Type)arrowType;
        protoType.TIME32 = time32.Unit switch
        {
            TimeUnit.Second => Proto.TimeUnit.Second,
            TimeUnit.Millisecond => Proto.TimeUnit.Millisecond,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), time32.Unit, "Unknown TimeUnit for Time32Type")
        };
    }

    private static void ToProtoTime64(IArrowType arrowType, Proto.ArrowType protoType)
    {
        var time64 = (Time64Type)arrowType;
        protoType.TIME64 = time64.Unit switch
        {
            TimeUnit.Microsecond => Proto.TimeUnit.Microsecond,
            TimeUnit.Nanosecond => Proto.TimeUnit.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), time64.Unit, "Unknown TimeUnit for Time64Type")
        };
    }

    private static void ToProtoInterval(IArrowType arrowType, Proto.ArrowType protoType)
    {
        var interval = (IntervalType)arrowType;
        protoType.INTERVAL = interval.Unit switch
        {
            IntervalUnit.YearMonth => Proto.IntervalUnit.YearMonth,
            IntervalUnit.DayTime => Proto.IntervalUnit.DayTime,
            IntervalUnit.MonthDayNanosecond => Proto.IntervalUnit.MonthDayNano,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), interval.Unit, "Unknown IntervalUnit for IntervalType")
        };
    }

    private static void ToProtoDuration(IArrowType arrowType, Proto.ArrowType protoType)
    {
        var duration = (DurationType)arrowType;
        protoType.DURATION = duration.Unit switch
        {
            TimeUnit.Second => Proto.TimeUnit.Second,
            TimeUnit.Millisecond => Proto.TimeUnit.Millisecond,
            TimeUnit.Microsecond => Proto.TimeUnit.Microsecond,
            TimeUnit.Nanosecond => Proto.TimeUnit.Nanosecond,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), duration.Unit, "Unknown TimeUnit for DurationType")
        };
    }

    private static void ToProtoUnion(IArrowType arrowType, Proto.ArrowType protoType)
    {
        var union = (UnionType)arrowType;
        protoType.UNION = new Proto.Union
        {
            UnionMode = union.Mode switch
            {
                UnionMode.Dense => Proto.UnionMode.Dense,
                UnionMode.Sparse => Proto.UnionMode.Sparse,
                _ => throw new ArgumentOutOfRangeException(nameof(arrowType), union.Mode, "Unknown UnionMode for UnionType")
            }
        };
        protoType.UNION.TypeIds.AddRange(union.TypeIds);
        // Children are handled separately
    }

    private static Proto.Field FieldToProto(Field field)
    {
        var protoField = new Proto.Field
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

    private static void AddChildrenFromArrowType(IArrowType arrowType, Proto.Field protoField)
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