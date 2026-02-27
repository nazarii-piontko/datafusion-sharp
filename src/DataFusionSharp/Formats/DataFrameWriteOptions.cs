using DataFusionSharp.Expressions;

namespace DataFusionSharp.Formats;

/// <summary>
/// Represents options that control how data is written out from a DataFrame.
/// </summary>
public sealed class DataFrameWriteOptions
{
    /// <summary>
    /// Controls the different options for how to insert data into an existing table when writing out a DataFrame.
    /// </summary>
    public InsertOp InsertOp { get; set; } = InsertOp.Append;
    
    /// <summary>
    /// Controls if all partitions should be coalesced into a single output file Generally will have slower
    /// Generally will have slower performance when set to true.
    /// </summary>
    public bool IsSingleFileOutput { get; set; }
    
    /// <summary>
    /// Sets which columns should be used for hive-style partitioned writes by name.
    /// </summary>
    public IEnumerable<string> PartitionBy { get; set; } = [];
}

internal static class ProtoDataFrameWriteOptionsExtensions
{
    internal static Proto.DataFrameWriteOptions ToProto(this DataFrameWriteOptions options)
    {
        return new Proto.DataFrameWriteOptions
        {
            InsertOp = options.InsertOp.ToProto(),
            SingleFileOutput = options.IsSingleFileOutput,
            PartitionBy = { options.PartitionBy }
        };
    }
}