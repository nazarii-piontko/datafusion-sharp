using Apache.Arrow;
using Apache.Arrow.Types;

namespace DataFusionSharp.Tests;

public sealed class RegisterBatchTests : IDisposable
{
    private readonly DataFusionRuntime _runtime;
    private readonly SessionContext _context;

    public RegisterBatchTests()
    {
        _runtime = DataFusionRuntime.Create();
        _context = _runtime.CreateSessionContext();
    }

    [Fact]
    public async Task RegisterBatch_CreatesQueryableTable()
    {
        // Arrange
        using var batch = CreateRecordBatch();

        // Act
        _context.RegisterBatch("test", batch);
        using var df = await _context.SqlAsync("SELECT * FROM test ORDER BY id");
        using var collected = await df.CollectAsync();

        // Assert
        var count = await df.CountAsync();
        Assert.Equal(2UL, count);
        
        var names = collected.Batches.SelectMany(b => b.Column("name").AsString()).ToList();
        Assert.Equal(["Alice", "Bob"], names);
    }

    [Fact]
    public void RegisterBatch_NullArguments_Throw()
    {
        // Arrange
        using var batch = CreateRecordBatch();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => _context.RegisterBatch(null!, batch));
        Assert.Throws<ArgumentNullException>(() => _context.RegisterBatch("test", null!));
    }

    [Fact]
    public async Task RegisterBatch_Deregister_MakesTableUnavailable()
    {
        // Arrange
        using var batch = CreateRecordBatch();

        // Act
        _context.RegisterBatch("test", batch);

        // Sanity check
        using (var dfBefore = await _context.SqlAsync("SELECT * FROM test"))
        {
            var countBefore = await dfBefore.CountAsync();
            Assert.Equal(2UL, countBefore);
        }

        await _context.DeregisterTableAsync("test");

        // Assert - table should no longer be available
        await Assert.ThrowsAsync<DataFusionException>(async () =>
        {
            using var df = await _context.SqlAsync("SELECT * FROM test");
            await df.CollectAsync();
        });
    }

    public void Dispose()
    {
        _context.Dispose();
        _runtime.Dispose();
    }
    
    private static RecordBatch CreateRecordBatch()
    {
        var idArray = new Int64Array.Builder().Append(1).Append(2).Build();
        var nameArray = new StringArray.Builder().Append("Alice").Append("Bob").Build();
        var fields = new[]
        {
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false)
        };
        var schema = new Schema(fields, []);
        
        return new RecordBatch(schema,[idArray, nameArray], 2);
    }
}



