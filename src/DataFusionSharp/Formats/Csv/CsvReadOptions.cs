using Apache.Arrow;
using FlatSharp;

namespace DataFusionSharp.Formats.Csv;

/// <summary>
/// Options for reading CSV files.
/// </summary>
public sealed class CsvReadOptions : CsvReadOptionsWire
{
    internal static readonly CsvReadOptions Default = new();

    internal new static ISerializer<CsvReadOptionsWire> Serializer => CsvReadOptionsWire.Serializer;
    
    /// <summary>
    /// An optional column delimiter character (ASCII). Defaults to ','.
    /// </summary>
    public new char? Delimiter
    {
        get => (char?) base.Delimiter;
        set => base.Delimiter = CharToByte(value);
    }

    /// <summary>
    /// An optional quote character (ASCII). Defaults to '"'`.
    /// </summary>
    public new char? Quote
    {
        get => (char?) base.Quote;
        set => base.Quote = CharToByte(value);
    }
    
    /// <summary>
    /// An optional terminator character (ASCII). Defaults to CRLF.
    /// </summary>
    public new char? Terminator
    {
        get => (char?) base.Terminator;
        set => base.Terminator = CharToByte(value);
    }
    
    /// <summary>
    /// An optional escape character (ASCII).
    /// </summary>
    public new char? Escape
    {
        get => (char?) base.Escape;
        set => base.Escape = CharToByte(value);
    }
    
    /// <summary>
    /// An optional comment character (ASCII). Lines starting with this character are ignored.
    /// </summary>
    public new char? Comment
    {
        get => (char?) base.Comment;
        set => base.Comment = CharToByte(value);
    }

    /// <summary>
    /// An optional schema for the CSV file. If not provided, the schema will be inferred from the data.
    /// </summary>
    public new Schema? Schema
    {
        get
        {
            if (field != null)
                return field;
            
            if (base.Schema == null)
                return null;
            
            using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(base.Schema.Value);
            
            field = reader.Schema;
            return field;
        }
        set
        {
            field = value;
            
            if (value == null)
            {
                base.Schema = null;
                return;
            }
            
            using var stream = new MemoryStream();
            using var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(stream, value);
            
            writer.WriteStart();
            writer.WriteEnd();
            
            base.Schema = stream.ToArray();
        }
    }
    
    private static byte? CharToByte(char? value)
    {
        return value == null || char.IsAscii(value.Value)
            ? (byte?) value
            : throw new ArgumentException("Delimiter must be an ASCII character.", nameof(value));
    }
}