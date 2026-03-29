using System.Reflection;

namespace DataFusionSharp.Data.Tests;

internal static class DataSet
{
    private static readonly string DataDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!, "Data", "orders");

    public static string CustomersCsvPath => Path.Combine(DataDir, "csv", "customers.csv");
    public static string OrdersCsvPath => Path.Combine(DataDir, "csv", "orders.csv");
}

