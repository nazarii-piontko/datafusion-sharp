namespace DataFusionSharp.Tests;

public sealed class ParquetTests : FormatTests
{
    protected override Task RegisterCustomersTableAsync()
    {
        return Context.RegisterParquetAsync("customers", DataSet.CustomersParquetPath);
    }

    protected override Task RegisterOrdersTableAsync()
    {
        return Context.RegisterParquetAsync("orders", DataSet.OrdersParquetPath);
    }
}