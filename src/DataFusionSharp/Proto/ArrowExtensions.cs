namespace DataFusionSharp.Proto;

/// <summary>
/// Extension methods for converting DataFusion types to Apache Arrow types.
/// </summary>
public static class ArrowExtensions
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
    public static Apache.Arrow.Types.IArrowType ToArrow(this ArrowType type, IEnumerable<Field> children)
    {
        ArgumentNullException.ThrowIfNull(type);
        
        return type.ArrowTypeEnumCase switch
        {
            ArrowType.ArrowTypeEnumOneofCase.NONE => Apache.Arrow.Types.NullType.Default,
            ArrowType.ArrowTypeEnumOneofCase.BOOL => Apache.Arrow.Types.BooleanType.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT8 => Apache.Arrow.Types.UInt8Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT8 => Apache.Arrow.Types.Int8Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT16 => Apache.Arrow.Types.UInt16Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT16 => Apache.Arrow.Types.Int16Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT32 => Apache.Arrow.Types.UInt32Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT32 => Apache.Arrow.Types.Int32Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.UINT64 => Apache.Arrow.Types.UInt64Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.INT64 => Apache.Arrow.Types.Int64Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.FLOAT16 => Apache.Arrow.Types.HalfFloatType.Default,
            ArrowType.ArrowTypeEnumOneofCase.FLOAT32 => Apache.Arrow.Types.FloatType.Default,
            ArrowType.ArrowTypeEnumOneofCase.FLOAT64 => Apache.Arrow.Types.DoubleType.Default,
            ArrowType.ArrowTypeEnumOneofCase.UTF8 => Apache.Arrow.Types.StringType.Default,
            ArrowType.ArrowTypeEnumOneofCase.UTF8VIEW => Apache.Arrow.Types.StringViewType.Default,
            ArrowType.ArrowTypeEnumOneofCase.LARGEUTF8 => Apache.Arrow.Types.LargeStringType.Default,
            ArrowType.ArrowTypeEnumOneofCase.BINARY => Apache.Arrow.Types.BinaryType.Default,
            ArrowType.ArrowTypeEnumOneofCase.BINARYVIEW => Apache.Arrow.Types.BinaryViewType.Default,
            ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZEBINARY => new Apache.Arrow.Types.FixedSizeBinaryType(type.FIXEDSIZEBINARY),
            ArrowType.ArrowTypeEnumOneofCase.LARGEBINARY => Apache.Arrow.Types.LargeBinaryType.Default,
            ArrowType.ArrowTypeEnumOneofCase.DATE32 => Apache.Arrow.Types.Date32Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.DATE64 => Apache.Arrow.Types.Date64Type.Default,
            ArrowType.ArrowTypeEnumOneofCase.DURATION => type.DURATION switch
            {
                TimeUnit.Second => Apache.Arrow.Types.DurationType.Second,
                TimeUnit.Millisecond => Apache.Arrow.Types.DurationType.Millisecond,
                TimeUnit.Microsecond => Apache.Arrow.Types.DurationType.Microsecond,
                TimeUnit.Nanosecond => Apache.Arrow.Types.DurationType.Nanosecond,
                _ => throw new ArgumentOutOfRangeException(nameof(type), type.DURATION, "Unknown Duration TimeUnit")
            },
            ArrowType.ArrowTypeEnumOneofCase.TIMESTAMP => type.TIMESTAMP.TimeUnit switch
            {
                TimeUnit.Second => new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Second, type.TIMESTAMP.Timezone),
                TimeUnit.Millisecond => new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, type.TIMESTAMP.Timezone),
                TimeUnit.Microsecond => new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, type.TIMESTAMP.Timezone),
                TimeUnit.Nanosecond => new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Nanosecond, type.TIMESTAMP.Timezone),
                _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIMESTAMP.TimeUnit, "Unknown Timestamp TimeUnit")
            },
            ArrowType.ArrowTypeEnumOneofCase.TIME32 => type.TIME32 switch
            {
                TimeUnit.Second => Apache.Arrow.Types.TimeType.Second,
                TimeUnit.Millisecond => Apache.Arrow.Types.TimeType.Millisecond,
                _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIME32, "Unknown TimeUnit for TIME32")
            },
            ArrowType.ArrowTypeEnumOneofCase.TIME64 => type.TIME64 switch
            {
                TimeUnit.Microsecond => Apache.Arrow.Types.TimeType.Microsecond,
                TimeUnit.Nanosecond => Apache.Arrow.Types.TimeType.Nanosecond,
                _ => throw new ArgumentOutOfRangeException(nameof(type), type.TIME64, "Unknown TimeUnit for TIME64")
            },
            ArrowType.ArrowTypeEnumOneofCase.INTERVAL => type.INTERVAL switch
            {
                IntervalUnit.YearMonth => Apache.Arrow.Types.IntervalType.YearMonth,
                IntervalUnit.DayTime => Apache.Arrow.Types.IntervalType.DayTime,
                IntervalUnit.MonthDayNano => Apache.Arrow.Types.IntervalType.MonthDayNanosecond,
                _ => throw new ArgumentOutOfRangeException(nameof(type), type.INTERVAL, "Unknown IntervalUnit for INTERVAL")
            },
            ArrowType.ArrowTypeEnumOneofCase.DECIMAL => new Apache.Arrow.Types.Decimal128Type((int)type.DECIMAL.Precision, type.DECIMAL.Scale),
            ArrowType.ArrowTypeEnumOneofCase.DECIMAL256 => new Apache.Arrow.Types.Decimal256Type((int)type.DECIMAL256.Precision, type.DECIMAL256.Scale),
            ArrowType.ArrowTypeEnumOneofCase.LIST => new Apache.Arrow.Types.ListType(
                new Apache.Arrow.Field(
                    type.LIST.FieldType.Name,
                    type.LIST.FieldType.ArrowType.ToArrow(type.LIST.FieldType.Children),
                    type.LIST.FieldType.Nullable,
                    type.LIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                )
            ),
            ArrowType.ArrowTypeEnumOneofCase.LARGELIST => new Apache.Arrow.Types.LargeListType(
                new Apache.Arrow.Field(
                    type.LARGELIST.FieldType.Name,
                    type.LARGELIST.FieldType.ArrowType.ToArrow(type.LARGELIST.FieldType.Children),
                    type.LARGELIST.FieldType.Nullable,
                    type.LARGELIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                )
            ),
            ArrowType.ArrowTypeEnumOneofCase.FIXEDSIZELIST => new Apache.Arrow.Types.FixedSizeListType(
                new Apache.Arrow.Field(
                    type.FIXEDSIZELIST.FieldType.Name,
                    type.FIXEDSIZELIST.FieldType.ArrowType.ToArrow(type.FIXEDSIZELIST.FieldType.Children),
                    type.FIXEDSIZELIST.FieldType.Nullable,
                    type.FIXEDSIZELIST.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                ),
                type.FIXEDSIZELIST.ListSize
            ),
            ArrowType.ArrowTypeEnumOneofCase.STRUCT => new Apache.Arrow.Types.StructType(
                children.Select(f => new Apache.Arrow.Field(
                    f.Name,
                    f.ArrowType.ToArrow(f.Children),
                    f.Nullable,
                    f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                )).ToList()
            ),
            ArrowType.ArrowTypeEnumOneofCase.UNION => type.UNION.UnionMode switch
            {
                UnionMode.Dense => new Apache.Arrow.Types.UnionType(
                    children.Select(f => new Apache.Arrow.Field(
                        f.Name,
                        f.ArrowType.ToArrow(f.Children),
                        f.Nullable,
                        f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                    )).ToList(),
                    type.UNION.TypeIds,
                    Apache.Arrow.Types.UnionMode.Dense
                ),
                UnionMode.Sparse => new Apache.Arrow.Types.UnionType(
                    children.Select(f => new Apache.Arrow.Field(
                        f.Name,
                        f.ArrowType.ToArrow(f.Children),
                        f.Nullable,
                        f.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                    )).ToList(),
                    type.UNION.TypeIds
                ),
                _ => throw new ArgumentOutOfRangeException(nameof(type), type.UNION.UnionMode, "Unknown UnionMode for UNION")
            },
            ArrowType.ArrowTypeEnumOneofCase.DICTIONARY => new Apache.Arrow.Types.DictionaryType(
                type.DICTIONARY.Key.ToArrow(Enumerable.Empty<Field>()),
                type.DICTIONARY.Value.ToArrow(Enumerable.Empty<Field>()),
                ordered: false
            ),
            ArrowType.ArrowTypeEnumOneofCase.MAP => new Apache.Arrow.Types.MapType(
                new Apache.Arrow.Field(
                    type.MAP.FieldType.Name,
                    type.MAP.FieldType.ArrowType.ToArrow(type.MAP.FieldType.Children),
                    type.MAP.FieldType.Nullable,
                    type.MAP.FieldType.Metadata.Select(kv => KeyValuePair.Create(kv.Key, kv.Value))
                ),
                type.MAP.KeysSorted
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type.ArrowTypeEnumCase, "Unknown ArrowType enum case")
        };
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
    public static ArrowType ToProto(this Apache.Arrow.Types.IArrowType arrowType)
    {
        ArgumentNullException.ThrowIfNull(arrowType);
        
        var protoType = new ArrowType();
        
        switch (arrowType.TypeId)
        {
            case Apache.Arrow.Types.ArrowTypeId.Null:
                protoType.NONE = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Boolean:
                protoType.BOOL = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.UInt8:
                protoType.UINT8 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Int8:
                protoType.INT8 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.UInt16:
                protoType.UINT16 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Int16:
                protoType.INT16 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.UInt32:
                protoType.UINT32 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Int32:
                protoType.INT32 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.UInt64:
                protoType.UINT64 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Int64:
                protoType.INT64 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.HalfFloat:
                protoType.FLOAT16 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Float:
                protoType.FLOAT32 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Double:
                protoType.FLOAT64 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.String:
                protoType.UTF8 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Binary:
                if (arrowType is Apache.Arrow.Types.FixedSizeBinaryType fixedSizeBinary)
                {
                    protoType.FIXEDSIZEBINARY = fixedSizeBinary.ByteWidth;
                }
                else
                {
                    protoType.BINARY = new EmptyMessage();
                }
                break;
            case Apache.Arrow.Types.ArrowTypeId.Date32:
                protoType.DATE32 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Date64:
                protoType.DATE64 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.Timestamp:
                var timestamp = (Apache.Arrow.Types.TimestampType)arrowType;
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
                break;
            case Apache.Arrow.Types.ArrowTypeId.Time32:
                var time32 = (Apache.Arrow.Types.Time32Type)arrowType;
                protoType.TIME32 = time32.Unit switch
                {
                    Apache.Arrow.Types.TimeUnit.Second => TimeUnit.Second,
                    Apache.Arrow.Types.TimeUnit.Millisecond => TimeUnit.Millisecond,
                    _ => throw new ArgumentOutOfRangeException(nameof(arrowType), time32.Unit, "Unknown TimeUnit for Time32Type")
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.Time64:
                var time64 = (Apache.Arrow.Types.Time64Type)arrowType;
                protoType.TIME64 = time64.Unit switch
                {
                    Apache.Arrow.Types.TimeUnit.Microsecond => TimeUnit.Microsecond,
                    Apache.Arrow.Types.TimeUnit.Nanosecond => TimeUnit.Nanosecond,
                    _ => throw new ArgumentOutOfRangeException(nameof(arrowType), time64.Unit, "Unknown TimeUnit for Time64Type")
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.Interval:
                var interval = (Apache.Arrow.Types.IntervalType)arrowType;
                protoType.INTERVAL = interval.Unit switch
                {
                    Apache.Arrow.Types.IntervalUnit.YearMonth => IntervalUnit.YearMonth,
                    Apache.Arrow.Types.IntervalUnit.DayTime => IntervalUnit.DayTime,
                    Apache.Arrow.Types.IntervalUnit.MonthDayNanosecond => IntervalUnit.MonthDayNano,
                    _ => throw new ArgumentOutOfRangeException(nameof(arrowType), interval.Unit, "Unknown IntervalUnit for IntervalType")
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.Duration:
                var duration = (Apache.Arrow.Types.DurationType)arrowType;
                protoType.DURATION = duration.Unit switch
                {
                    Apache.Arrow.Types.TimeUnit.Second => TimeUnit.Second,
                    Apache.Arrow.Types.TimeUnit.Millisecond => TimeUnit.Millisecond,
                    Apache.Arrow.Types.TimeUnit.Microsecond => TimeUnit.Microsecond,
                    Apache.Arrow.Types.TimeUnit.Nanosecond => TimeUnit.Nanosecond,
                    _ => throw new ArgumentOutOfRangeException(nameof(arrowType), duration.Unit, "Unknown TimeUnit for DurationType")
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.Decimal128:
                var decimal128 = (Apache.Arrow.Types.Decimal128Type)arrowType;
                protoType.DECIMAL = new Decimal
                {
                    Precision = (uint)decimal128.Precision,
                    Scale = decimal128.Scale
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.Decimal256:
                var decimal256 = (Apache.Arrow.Types.Decimal256Type)arrowType;
                protoType.DECIMAL256 = new Decimal256Type
                {
                    Precision = (uint)decimal256.Precision,
                    Scale = decimal256.Scale
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.List:
                var list = (Apache.Arrow.Types.ListType)arrowType;
                protoType.LIST = new List
                {
                    FieldType = FieldToProto(list.ValueField)
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.Struct:
                protoType.STRUCT = new Struct();
                // Children are handled separately
                break;
            case Apache.Arrow.Types.ArrowTypeId.Union:
                var union = (Apache.Arrow.Types.UnionType)arrowType;
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
                break;
            case Apache.Arrow.Types.ArrowTypeId.Dictionary:
                var dictionary = (Apache.Arrow.Types.DictionaryType)arrowType;
                protoType.DICTIONARY = new Dictionary
                {
                    Key = dictionary.IndexType.ToProto(),
                    Value = dictionary.ValueType.ToProto()
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.Map:
                var map = (Apache.Arrow.Types.MapType)arrowType;
                protoType.MAP = new Map
                {
                    FieldType = FieldToProto(map.Fields[0]),
                    KeysSorted = false  // Apache Arrow C# MapType doesn't expose this property
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.FixedSizeList:
                var fixedSizeList = (Apache.Arrow.Types.FixedSizeListType)arrowType;
                protoType.FIXEDSIZELIST = new FixedSizeList
                {
                    FieldType = FieldToProto(fixedSizeList.ValueField),
                    ListSize = fixedSizeList.ListSize
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.LargeBinary:
                protoType.LARGEBINARY = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.LargeString:
                protoType.LARGEUTF8 = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.LargeList:
                var largeList = (Apache.Arrow.Types.LargeListType)arrowType;
                protoType.LARGELIST = new List
                {
                    FieldType = FieldToProto(largeList.ValueField)
                };
                break;
            case Apache.Arrow.Types.ArrowTypeId.BinaryView:
                protoType.BINARYVIEW = new EmptyMessage();
                break;
            case Apache.Arrow.Types.ArrowTypeId.StringView:
                protoType.UTF8VIEW = new EmptyMessage();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(arrowType), arrowType.TypeId, "Unsupported ArrowTypeId");
        }
        
        return protoType;
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

    private static void AddChildrenFromArrowType(Apache.Arrow.Types.IArrowType arrowType, Field protoField)
    {
        switch (arrowType.TypeId)
        {
            case Apache.Arrow.Types.ArrowTypeId.Struct:
                var structType = (Apache.Arrow.Types.StructType)arrowType;
                foreach (var childField in structType.Fields)
                {
                    protoField.Children.Add(FieldToProto(childField));
                }
                break;
            case Apache.Arrow.Types.ArrowTypeId.Union:
                var unionType = (Apache.Arrow.Types.UnionType)arrowType;
                foreach (var childField in unionType.Fields)
                {
                    protoField.Children.Add(FieldToProto(childField));
                }
                break;
        }
    }
}