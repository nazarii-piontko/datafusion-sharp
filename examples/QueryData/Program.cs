using Apache.Arrow;
using DataFusionSharp;

using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

// Register data from CSV format
await context.RegisterCsvAsync("customers", Path.Combine("Data", "customers.csv"));
await context.RegisterCsvAsync("orders", Path.Combine("Data", "orders.csv"));

// You can also register data from JSON format
// await context.RegisterJsonAsync("customers", Path.Combine("Data", "customers.json"));
// await context.RegisterJsonAsync("orders", Path.Combine("Data", "orders.json"));

// Or from Parquet format
// await context.RegisterParquetAsync("customers", Path.Combine("Data", "customers.parquet"));
// await context.RegisterParquetAsync("orders", Path.Combine("Data", "orders.parquet"));

using var df = await context.SqlAsync(
    """
    SELECT
        c.customer_name,
        sum(o.order_amount) AS total_amount
    FROM orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'Completed'
    GROUP BY c.customer_name
    ORDER BY c.customer_name
    """);

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
