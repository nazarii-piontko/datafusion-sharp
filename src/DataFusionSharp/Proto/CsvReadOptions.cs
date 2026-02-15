namespace DataFusionSharp.Proto;

public partial class CsvReadOptions
{
    /// <summary>
    /// The default delimiter character to use when parsing CSV files.
    /// </summary>
    public const string DefaultDelimiter = ",";
    
    /// <summary>
    /// The default quote character to use when parsing CSV files.
    /// </summary>
    public const string DefaultQuote = "\"";

    /// <summary>
    /// The default maximum number of records to read when inferring the schema of a CSV file.
    /// </summary>
    public const int DefaultSchemaInferMaxRecord = 1000;
    
    /// <summary>
    /// The default file extension to use when writing CSV files.
    /// </summary>
    public const string DefaultCsvExtension = ".csv";
    
    internal static readonly CsvReadOptions Default = new();

    partial void OnConstruction()
    {
        HasHeader = true;
        SchemaInferMaxRecords = DefaultSchemaInferMaxRecord;
        Delimiter = DefaultDelimiter;
        Quote = DefaultQuote;
        FileExtension = DefaultCsvExtension;
        FileCompressionType = CompressionTypeVariant.Uncompressed;
    }
}