using DataFusionSharp;

using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

using var df = await context.SqlAsync("SELECT s.value AS id, sin(s.value) AS value FROM generate_series(1, 10) AS s");

await df.ShowAsync();

Console.WriteLine($"Total rows: {await df.CountAsync()}");
