using DataFusionSharp.Tests;

var test = new StressTests();
try
{
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithStream);
    await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithCollect);
}
finally
{
    test.Dispose();
}

GC.Collect(2, GCCollectionMode.Forced, true, true);
