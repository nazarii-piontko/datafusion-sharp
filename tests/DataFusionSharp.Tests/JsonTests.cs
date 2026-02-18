using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

internal sealed class JsonTests : FileFormatTests
{
    protected override string FileExtension => ".json";
    
    public JsonTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override Task RegisterCustomersTableAsync(string tableName = "customers")
    {
        return Context.RegisterJsonAsync(tableName, DataSet.CustomersJsonPath);
    }

    protected override Task RegisterOrdersTableAsync(string tableName = "orders")
    {
        return Context.RegisterJsonAsync(tableName, DataSet.OrdersJsonPath);
    }

    protected override Task WriteTableAsync(DataFrame dataFrame, string path)
    {
        return dataFrame.WriteJsonAsync(path);
    }
}
