namespace DataFusionSharp.Tests;

public class InteropTests
{
    [Fact]
    public void Initialize_Returns()
    {
        DataFusion.Initialize();
    }
    
    [Fact]
    public void Shutdown_Returns()
    {
        DataFusion.Initialize();
        DataFusion.Shutdown();
    }

    [Fact]
    public void SessionContext_CanCreate()
    {
        DataFusion.Initialize();

        using var ctx = new SessionContext();
        Assert.NotNull(ctx);
    }

    [Fact]
    public void Sql_SelectOne_ReturnsDataFrame()
    {
        DataFusion.Initialize();

        using var ctx = new SessionContext();
        using var df = ctx.Sql("SELECT 1 as value");

        Assert.NotNull(df);
    }

    [Fact]
    public void DataFrame_Show_Succeeds()
    {
        DataFusion.Initialize();

        using var ctx = new SessionContext();
        using var df = ctx.Sql("SELECT 1 as a, 2 as b");

        df.Show();
    }

    [Fact]
    public void DataFrame_Count_ReturnsCorrectCount()
    {
        DataFusion.Initialize();

        using var ctx = new SessionContext();
        using var df = ctx.Sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");

        var count = df.Count();
        Assert.Equal(3ul, count);
    }

    [Fact]
    public void Sql_InvalidQuery_ThrowsException()
    {
        DataFusion.Initialize();

        using var ctx = new SessionContext();

        var ex = Assert.Throws<DataFusionException>(() => ctx.Sql("INVALID SQL"));
        Assert.NotNull(ex.Message);
    }
}