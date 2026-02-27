using System.Reflection;

namespace DataFusionSharp.Tests;

internal static class DataSet
{
    private static readonly string DataDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!, "Data", "orders");
    
    public static string CustomersCsvPath => Path.Combine(DataDir, "csv", "customers.csv");
    public static string OrdersCsvPath => Path.Combine(DataDir, "csv", "orders.csv");
    
    public static string CustomersJsonPath => Path.Combine(DataDir, "json", "customers.json");
    public static string OrdersJsonPath => Path.Combine(DataDir, "json", "orders.json");
    
    public static string CustomersParquetPath => Path.Combine(DataDir, "parquet", "customers.parquet");
    public static string OrdersParquetPath => Path.Combine(DataDir, "parquet", "orders.parquet");

    public static string ProductsCsvDir => Path.Combine(DataDir, "csv", "products");
    public static string ProductsJsonDir => Path.Combine(DataDir, "json", "products");
}