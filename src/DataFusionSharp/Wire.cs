using Apache.Arrow;

namespace DataFusionSharp;

public partial class CsvReadOptions
{
    internal static readonly CsvReadOptions Default = new();
    
    /// <summary>
    /// An optional column delimiter. Defaults to ','.
    /// </summary>
    public char? DelimiterChar
    {
        get => (char?) Delimiter;
        set => Delimiter = CharToByte(value);
    }

    /// <summary>
    /// An optional quote character. Defaults to '"'.
    /// </summary>
    public char? QuoteChar
    {
        get => (char?) Quote;
        set => Quote = CharToByte(value);
    }
    
    /// <summary>
    /// An optional terminator character. Defaults to None (CRLF).
    /// </summary>
    public char? TerminatorChar
    {
        get => (char?) Terminator;
        set => Terminator = CharToByte(value);
    }
    
    /// <summary>
    /// An optional escape character. Defaults to None.
    /// </summary>
    public char? EscapeChar
    {
        get => (char?) Escape;
        set => Escape = CharToByte(value);
    }
    
    /// <summary>
    /// If enabled, lines beginning with this byte are ignored.
    /// </summary>
    public char? CommentChar
    {
        get => (char?) Comment;
        set => Comment = CharToByte(value);
    }

    /// <summary>
    /// An optional schema for the CSV file. If not provided, the schema will be inferred from the data.
    /// </summary>
    public Schema? Schema
    {
        get
        {
            if (field != null)
                return field;
            
            if (SchemaSerialized == null)
                return null;
            
            using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(SchemaSerialized.Value);
            
            field = reader.Schema;
            return field;
        }
        set
        {
            field = value;
            
            if (value == null)
            {
                SchemaSerialized = null;
                return;
            }
            
            using var stream = new MemoryStream();
            using var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(stream, value);
            
            writer.WriteStart();
            writer.WriteEnd();
            
            SchemaSerialized = stream.ToArray();
        }
    }
    
    private static byte? CharToByte(char? value)
    {
        return value == null || char.IsAscii(value.Value)
            ? (byte?) value
            : throw new ArgumentException("Delimiter must be an ASCII character.", nameof(value));
    }
}