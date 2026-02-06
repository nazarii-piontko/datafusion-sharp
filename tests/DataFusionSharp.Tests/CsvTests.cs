namespace DataFusionSharp.Tests;

public sealed class CsvTests : FormatTests
{
    protected override Task RegisterCustomersTableAsync()
    {
        return Context.RegisterCsvAsync("customers", DataSet.CustomersCsvPath);
    }

    protected override Task RegisterOrdersTableAsync()
    {
        return Context.RegisterCsvAsync("orders", DataSet.OrdersCsvPath);
    }
}