using DataFusionSharp.Tests;

Console.WriteLine("=== Test started ===");

var test = new StressTests();
try
{
    Console.WriteLine("=== Running ConcurrentSessions_HandleMultipleQueries_Successfully with Query_WithStream ===");
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithStream);
    
    Console.WriteLine("=== Running ConcurrentSessions_HandleMultipleQueries_Successfully with Query_WithCollect ===");
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithCollect);
}
finally
{
    test.Dispose();
}

Console.WriteLine("=== Forcing GC to check for memory leaks ===");
GC.Collect(2, GCCollectionMode.Forced, true, true);

Console.WriteLine("=== Test completed ===");
