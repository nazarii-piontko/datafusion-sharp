using Dapper;
using DataFusionSharp;
using DataFusionSharp.Data;

using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

// Test DataFusionSharp API
Console.WriteLine("=== Test DataFusionSharp API ===");

using var df = await context.SqlAsync("SELECT s.value AS id, sin(s.value) AS value FROM generate_series(1, 10) AS s");

await df.ShowAsync();

Console.WriteLine($"Total rows: {await df.CountAsync()}");

// Test DataFusionSharp.Data API
Console.WriteLine("=== Test DataFusionSharp.Data API ===");

await using var connection = context.AsConnection(leaveOpen: true);

var ids = await connection.QueryAsync<long>("SELECT s.value AS id FROM generate_series(1, 10) AS s");

Console.WriteLine($"IDs: {string.Join(", ", ids)}");
