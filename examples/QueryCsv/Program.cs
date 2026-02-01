using Apache.Arrow;
using DataFusionSharp;

await using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

await context.RegisterCsvAsync("customers", Path.Combine("Data", "customers.csv"));
await context.RegisterCsvAsync("orders", Path.Combine("Data", "orders.csv"));

using var df = await context.SqlAsync("SELECT customer_id, sum(amount) AS total_amount FROM orders WHERE status = 'completed' GROUP BY customer_id ORDER BY customer_id");
await df.ShowAsync();
Console.WriteLine($"Total rows: {await df.CountAsync()}");

var schema = await df.GetSchemaAsync();
Console.WriteLine("=== Schema ===");
foreach (var field in schema.FieldsList)
    Console.WriteLine($"- {field.Name}: {field.DataType}");

var collectedData = await df.CollectAsync();
Console.WriteLine("=== Data ===");
foreach (var batch in collectedData.Batches)
{
    for (int r = 0; r < batch.Length; r++)
    {
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var v = batch.Column(c) switch
            {
                Int64Array a => (object) a.Values[r],
                DoubleArray a => a.Values[r],
                _ => null
            };

            Console.Write(v);
            Console.Write("\t");
        }
        Console.WriteLine();
    }
}

try
{
    using var tdf = await context.SqlAsync("SELECT some_invalid_column FROM orders");
}
catch (DataFusionException ex)
{
    Console.WriteLine($"Query failed: {ex.Message}");
}

