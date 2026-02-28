using Apache.Arrow;
using Apache.Arrow.Types;
using DataFusionSharp;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Csv;

using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

// Register data from CSV format
await context.RegisterCsvAsync("customers", Path.Combine("Data", "orders", "csv", "customers.csv"));
await context.RegisterCsvAsync("orders", Path.Combine("Data", "orders", "csv", "orders.csv"));
await context.RegisterCsvAsync("products", Path.Combine("Data", "orders", "csv", "products"), new CsvReadOptions
{
    // Example of partitioning the "products" table by the "category" column using Hive-style partitioning
    // (i.e., the category value is encoded in the file path like "products/category=electronics/data.csv")
    TablePartitionCols = [new PartitionColumn("category", StringType.Default)]
});

// You can also register data from JSON format
// await context.RegisterJsonAsync("customers", Path.Combine("Data", "customers.json"));

// Or from Parquet format
// await context.RegisterParquetAsync("customers", Path.Combine("Data", "customers.parquet"));

using var df = await context.SqlAsync(
    """
    SELECT
        c.customer_name,
        p.category,
        sum(o.order_amount) AS total_amount
    FROM orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
        JOIN products AS p ON o.product_id = p.product_id
    WHERE o.order_status = $order_status
    GROUP BY c.customer_name, p.category
    ORDER BY c.customer_name
    """,
    [("order_status", "Completed")]);

Console.WriteLine("=== Query Results ===");
Console.WriteLine(await df.ToStringAsync());
Console.WriteLine($"Total rows: {await df.CountAsync()}");
Console.WriteLine();

Console.WriteLine("=== Schema ===");
var schema = await df.GetSchemaAsync();
foreach (var field in schema.FieldsList)
    Console.WriteLine($"  {field.Name}: {field.DataType}");
Console.WriteLine();

Console.WriteLine("=== Collected Data ===");
using var collectedData = await df.CollectAsync();
foreach (var batch in collectedData.Batches)
    PrintBatch(batch);

Console.WriteLine("=== Streamed Data ===");
using var stream = await df.ExecuteStreamAsync();
await foreach (var batch in stream)
    PrintBatch(batch);

return;

void PrintBatch(RecordBatch recordBatch)
{
    for (var r = 0; r < recordBatch.Length; r++)
    {
        for (var c = 0; c < recordBatch.ColumnCount; c++)
        {
            var v = recordBatch.Column(c) switch
            {
                StringArray a => (object)a.GetString(r),
                StringViewArray a => a.GetString(r),
                Int64Array a => a.GetValue(r),
                _ => null
            };
            Console.Write($"{v}\t");
        }
        Console.WriteLine();
    }
}
