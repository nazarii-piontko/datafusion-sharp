using Dapper;
using DataFusionSharp;
using DataFusionSharp.Data;
using DataFusionSharp.ObjectStore;
using Microsoft.Extensions.Logging;

// Setup logging
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Information)
        .AddSimpleConsole(o => o.IncludeScopes = true);
});
DataFusionNativeLogger.ConfigureLogger(loggerFactory.CreateLogger("DataFusionSharp"), LogLevel.Debug);

// Init runtime and session
using var runtime = DataFusionRuntime.Create();
using var session = runtime.CreateSessionContext();

// Register public S3 bucket — no credentials needed
session.RegisterS3ObjectStore("s3://pudl.catalyst.coop", new S3ObjectStoreOptions
{
    BucketName = "pudl.catalyst.coop",
    Region = "us-west-2",
    SkipSignature = true,
});

// Register Parquet tables from the PUDL (Public Utility Data Liberation) project.
// These contain cleaned U.S. Energy Information Administration data:
//   - utilities:  utility companies (name, state, entity type)
//   - plants:     power plants (name, location, coordinates)
//   - generators: individual generator units (capacity, fuel type, technology, output)
Console.WriteLine("Registering Parquet tables from S3...");

await session.RegisterParquetAsync(
    "utilities",
    "s3://pudl.catalyst.coop/stable/out_eia__yearly_utilities.parquet");
Console.WriteLine("  ✓ utilities");

await session.RegisterParquetAsync(
    "plants",
    "s3://pudl.catalyst.coop/stable/out_eia__yearly_plants.parquet");
Console.WriteLine("  ✓ plants");

await session.RegisterParquetAsync(
    "generators",
    "s3://pudl.catalyst.coop/stable/out_eia__yearly_generators.parquet");
Console.WriteLine("  ✓ generators");

// Create ADO.NET connection — this is what Dapper uses
await using var connection = session.AsConnection();


// QueryAsync<T> – three-way JOIN to rank utilities by total installed capacity
Console.WriteLine("\n=== Top utilities by generation capacity ===\n");

var topUtilities = await connection.QueryAsync<UtilityCapacity>("""
    SELECT
        u.utility_name_eia AS UtilityName,
        u.state AS State,
        COUNT(DISTINCT p.plant_id_eia) AS PlantCount,
        ROUND(CAST(SUM(g.capacity_mw) AS DOUBLE), 1) AS TotalCapacityMw
    FROM generators g
        JOIN plants p
            ON g.plant_id_eia = p.plant_id_eia
            AND g.report_date = p.report_date
        JOIN utilities u
            ON p.utility_id_eia = u.utility_id_eia
            AND p.report_date = u.report_date
    WHERE g.report_date = '2026-01-01'
        AND g.operational_status = 'existing'
    GROUP BY u.utility_name_eia, u.state
    ORDER BY TotalCapacityMw DESC
    LIMIT 15
    """);

foreach (var utility in topUtilities)
    Console.WriteLine($"  {utility.UtilityName} ({utility.State}): {utility.TotalCapacityMw:N1} MW across {utility.PlantCount} plants");


// QueryAsync<T> – energy mix by state and fuel type
Console.WriteLine("\n=== Energy mix by state and fuel type ===\n");

var energyMix = await connection.QueryAsync<StateEnergyMix>("""
    SELECT
        p.state AS State,
        CAST(g.fuel_type_code_pudl AS VARCHAR) AS FuelType,
        COUNT(*) AS GeneratorCount,
        ROUND(CAST(SUM(g.capacity_mw) AS DOUBLE), 1) AS TotalCapacityMw,
        ROUND(CAST(AVG(g.capacity_factor) AS DOUBLE), 3) AS AvgCapacityFactor
    FROM generators g
        JOIN plants p
            ON g.plant_id_eia = p.plant_id_eia
            AND g.report_date = p.report_date
    WHERE g.report_date = '2026-01-01'
        AND g.operational_status = 'existing'
        AND p.state IS NOT NULL
    GROUP BY p.state, g.fuel_type_code_pudl
    ORDER BY TotalCapacityMw DESC
    LIMIT 20
    """);

foreach (var row in energyMix)
    Console.WriteLine($"  {row.State} - {row.FuelType}: {row.TotalCapacityMw:N1} MW ({row.GeneratorCount} generators)");


// QueryAsync<T> – three-way JOIN to find the largest individual generators
Console.WriteLine("\n=== Largest generators in the country ===\n");

var largestGenerators = await connection.QueryAsync<TopGenerator>("""
    SELECT
        p.plant_name_eia AS PlantName,
        p.city AS City,
        p.state AS State,
        u.utility_name_eia AS UtilityName,
        g.technology_description AS Technology,
        g.capacity_mw AS CapacityMw,
        ROUND(CAST(g.net_generation_mwh AS DOUBLE), 0) AS NetGenerationMwh,
        ROUND(CAST(g.capacity_factor AS DOUBLE), 3) AS CapacityFactor
    FROM generators g
        JOIN plants p
            ON g.plant_id_eia = p.plant_id_eia
            AND g.report_date = p.report_date
        JOIN utilities u
            ON p.utility_id_eia = u.utility_id_eia
            AND p.report_date = u.report_date
    WHERE g.report_date = '2026-01-01'
        AND g.operational_status = 'existing'
        AND g.capacity_mw IS NOT NULL
    ORDER BY g.capacity_mw DESC
    LIMIT 10
    """);

foreach (var gen in largestGenerators)
    Console.WriteLine($"  {gen.PlantName} ({gen.State}): {gen.CapacityMw:N0} MW - {gen.Technology} [{gen.UtilityName}]");


// Parameterized query – Dapper anonymous objects with @param syntax
Console.WriteLine("\n=== Parameterized query — Texas generators >= 100 MW ===\n");

var state = "TX";
var minCapacityMw = 100f;

var texasPlants = await connection.QueryAsync<TopGenerator>("""
    SELECT
        p.plant_name_eia AS PlantName,
        p.city AS City,
        p.state AS State,
        u.utility_name_eia AS UtilityName,
        g.technology_description AS Technology,
        g.capacity_mw AS CapacityMw,
        ROUND(CAST(g.net_generation_mwh AS DOUBLE), 0) AS NetGenerationMwh,
        ROUND(CAST(g.capacity_factor AS DOUBLE), 3) AS CapacityFactor
    FROM generators g
        JOIN plants p
            ON g.plant_id_eia = p.plant_id_eia
            AND g.report_date = p.report_date
        JOIN utilities u
            ON p.utility_id_eia = u.utility_id_eia
            AND p.report_date = u.report_date
    WHERE g.report_date = '2026-01-01'
        AND g.operational_status = 'existing'
        AND p.state = @state
        AND g.capacity_mw >= @minCapacityMw
    ORDER BY g.capacity_mw DESC
    LIMIT 10
    """,
    new { state, minCapacityMw });

foreach (var plant in texasPlants)
    Console.WriteLine($"  {plant.PlantName}: {plant.CapacityMw:N0} MW - {plant.Technology} [{plant.UtilityName}]");


// ExecuteScalarAsync – single aggregate value
Console.WriteLine("\n=== Scalar query — California total capacity ===\n");

var totalCapacity = await connection.ExecuteScalarAsync<double>("""
    SELECT ROUND(CAST(SUM(g.capacity_mw) AS DOUBLE), 1)
    FROM generators g
        JOIN plants p
            ON g.plant_id_eia = p.plant_id_eia
            AND g.report_date = p.report_date
    WHERE g.report_date = '2026-01-01'
        AND g.operational_status = 'existing'
        AND p.state = @state
    """,
    new { state = "CA" });

Console.WriteLine($"  California total installed capacity: {totalCapacity:N1} MW");


// Model types

record UtilityCapacity(
    string UtilityName,
    string? State,
    long PlantCount,
    double TotalCapacityMw);

record StateEnergyMix(
    string State,
    string FuelType,
    long GeneratorCount,
    double TotalCapacityMw,
    double? AvgCapacityFactor);

record TopGenerator(
    string PlantName,
    string? City,
    string? State,
    string UtilityName,
    string? Technology,
    float CapacityMw,
    double? NetGenerationMwh,
    double? CapacityFactor);
