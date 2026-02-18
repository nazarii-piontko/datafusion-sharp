using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

internal sealed class ParquetTests : FileFormatTests
{
    protected override string FileExtension => ".parquet";
    
    public ParquetTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync(string tableName = "customers")
    {
        return Context.RegisterParquetAsync(tableName, DataSet.CustomersParquetPath);
    }

    protected override Task RegisterOrdersTableAsync(string tableName = "orders")
    {
        return Context.RegisterParquetAsync(tableName, DataSet.OrdersParquetPath);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteParquetAsync(path);
    }
}