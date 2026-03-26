using Apache.Arrow;
using Apache.Arrow.Types;
using DataFusionSharp;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Parquet;
using DataFusionSharp.ObjectStore;
using Microsoft.Extensions.Logging;

// Setup logging
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Information)
        .AddSimpleConsole(o => o.IncludeScopes = true);
});
DataFusionNativeLogger.ConfigureLogger(loggerFactory.CreateLogger("DataFusionSharp"), LogLevel.Information);

// Init runtime and single session
using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

// Register public S3 bucket with diamonds dataset (no credentials needed)
context.RegisterS3ObjectStore("s3://arrow-datasets", new S3ObjectStoreOptions
{
    BucketName = "arrow-datasets",
    Region = "us-east-1",
    SkipSignature = true,
});

// Register Hive-partitioned parquet table from S3
await context.RegisterParquetAsync("diamonds", "s3://arrow-datasets/diamonds/", new ParquetReadOptions
{
    TablePartitionCols = [new PartitionColumn("cut", StringType.Default)],
    ParquetPruning = true,
});

// Query 1: Schema exploration
Console.WriteLine("=== Table Schema ===");
using (var df = await context.SqlAsync("SELECT * FROM diamonds LIMIT 0"))
{
    var schema = df.GetSchema();
    foreach (var field in schema.FieldsList)
        Console.WriteLine($"  {field.Name}: {field.DataType.Name}");
}
Console.WriteLine();

// Query 2: Dataset overview
Console.WriteLine("=== Dataset Overview ===");
using (var df = await context.SqlAsync("SELECT count(*) AS total_rows FROM diamonds"))
    Console.WriteLine(await df.ToStringAsync());

using (var df = await context.SqlAsync("""
    SELECT cut, count(*) AS count
    FROM diamonds
    GROUP BY cut
    ORDER BY count DESC
    """))
{
    Console.WriteLine("Rows by cut (partition column):");
    Console.WriteLine(await df.ToStringAsync());
}
Console.WriteLine();

// Query 3: Price statistics by cut
Console.WriteLine("=== Price Statistics by Cut ===");
using (var df = await context.SqlAsync("""
    SELECT
        cut,
        count(*) AS cnt,
        ROUND(avg(price), 2) AS avg_price,
        min(price) AS min_price,
        max(price) AS max_price,
        ROUND(stddev(price), 2) AS price_stddev
    FROM diamonds
    GROUP BY cut
    ORDER BY avg_price DESC
    """))
    Console.WriteLine(await df.ToStringAsync());
Console.WriteLine();

// Query 4: Parameterized query with partition pruning
Console.WriteLine("=== Premium Diamonds: Large, High-Quality, Ideal Cut ===");
using (var df = await context.SqlAsync("""
    SELECT carat, color, clarity, price
    FROM diamonds
    WHERE cut = $cut
      AND carat >= $min_carat
    ORDER BY price DESC
    LIMIT 10
    """,
    [("cut", "Ideal"), ("min_carat", 2.0)]))
    Console.WriteLine(await df.ToStringAsync());
Console.WriteLine();

// Query 5: Computed columns with CollectAsync for Arrow batch access
Console.WriteLine("=== Top 5 Best Value Diamonds (lowest price per carat) ===");
using (var df = await context.SqlAsync("""
    SELECT
        carat,
        cut,
        color,
        clarity,
        price,
        ROUND(CAST(price AS DOUBLE) / carat, 2) AS price_per_carat
    FROM diamonds
    WHERE carat > 0.5
    ORDER BY price_per_carat ASC
    LIMIT 5
    """))
{
    using var collected = await df.CollectAsync();
    Console.WriteLine(string.Join("\t", collected.Schema.FieldsList.Select(f => f.Name)));
    foreach (var batch in collected.Batches)
    {
        for (var r = 0; r < batch.Length; r++)
        {
            for (var c = 0; c < batch.ColumnCount; c++)
            {
                var v = batch.Column(c) switch
                {
                    StringArray a => (object?)a.GetString(r),
                    DoubleArray a => a.GetValue(r),
                    Int64Array a => a.GetValue(r),
                    FloatArray a => a.GetValue(r),
                    _ => batch.Column(c).GetType().Name
                };
                Console.Write($"{v}\t");
            }
            Console.WriteLine();
        }
    }
}
Console.WriteLine();

// Query 6: Write aggregated results to local parquet
Console.WriteLine("=== Writing Summary to Local Parquet ===");
using (var df = await context.SqlAsync("""
    SELECT
        cut,
        color,
        count(*) AS cnt,
        ROUND(avg(price), 2) AS avg_price,
        ROUND(avg(carat), 3) AS avg_carat
    FROM diamonds
    GROUP BY cut, color
    ORDER BY cut, color
    """))
{
    var outputPath = Path.Combine(Directory.GetCurrentDirectory(), "output", "diamond_summary.parquet");
    Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
    await df.WriteParquetAsync(outputPath);
    Console.WriteLine($"Written to: {outputPath}");
}

Console.WriteLine();
Console.WriteLine("Done.");
