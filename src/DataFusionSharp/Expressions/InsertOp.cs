namespace DataFusionSharp.Expressions;

/// <summary>
/// Represents the different options for how to insert data into an existing table when writing out a DataFrame.
/// </summary>
public enum InsertOp
{
    /// <summary>
    /// Appends new rows to the existing table without modifying any existing rows.
    /// This corresponds to the SQL `INSERT INTO` query.
    /// </summary>
    Append,
    
    /// <summary>
    /// Overwrites all existing rows in the table with the new rows.
    /// This corresponds to the SQL `INSERT OVERWRITE` query.
    /// </summary>
    Overwrite,
    
    /// <summary>
    /// If any existing rows collides with the inserted rows (typically based on a unique key or primary key), those existing rows are replaced.
    /// This corresponds to the SQL `REPLACE INTO` query and its equivalents.
    /// </summary>
    Replace
}

internal static class ProtoInsertOpExtensions
{
    internal static Proto.InsertOp ToProto(this InsertOp op) => op switch
    {
        InsertOp.Append => Proto.InsertOp.Append,
        InsertOp.Overwrite => Proto.InsertOp.Overwrite,
        InsertOp.Replace => Proto.InsertOp.Replace,
        _ => throw new ArgumentOutOfRangeException(nameof(op), op, "Invalid InsertOp value")
    };
}