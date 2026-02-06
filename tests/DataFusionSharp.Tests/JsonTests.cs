namespace DataFusionSharp.Tests;

public sealed class JsonTests : FormatTests
{
    protected override Task RegisterCustomersTableAsync()
    {
        return Context.RegisterJsonAsync("customers", DataSet.CustomersJsonPath);
    }

    protected override Task RegisterOrdersTableAsync()
    {
        return Context.RegisterJsonAsync("orders", DataSet.OrdersJsonPath);
    }
}
