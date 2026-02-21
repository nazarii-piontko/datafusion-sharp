using DataFusionSharp.Tests;

using var test = new StressTests();
await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithStream);
await test.ConcurrentSessions_HandleMultipleQueries_Successfully(StressTestsQueries.Query_WithCollect);
