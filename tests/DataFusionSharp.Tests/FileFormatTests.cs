using Xunit.Abstractions;

namespace DataFusionSharp.Tests;

/// <summary>
/// Base class for testing different data formats (CSV, JSON, Parquet) in DataFusion.
/// This includes tests for registering tables from different formats, querying them, and writing results back to disk.
/// </summary>
public abstract class FileFormatTests : IDisposable
{
    /// <summary>
    /// Test output helper for logging test information, warnings, and errors during test execution.
    /// </summary>
    protected ITestOutputHelper TestOutputHelper { get; }

    /// <summary>
    /// Shared DataFusion runtime instance used for all tests in this class.
    /// Initialized once per test class instance and disposed at the end.
    /// </summary>
    protected DataFusionRuntime Runtime { get; }
    
    /// <summary>
    /// Shared session context used for executing SQL queries in the tests.
    /// Initialized once per test class instance and disposed at the end.
    /// </summary>
    protected SessionContext Context { get; }
    
    protected abstract string FileExtension { get; }

    protected FileFormatTests(ITestOutputHelper testOutputHelper)
    {
        TestOutputHelper = testOutputHelper;
        Runtime = DataFusionRuntime.Create();
        Context = Runtime.CreateSessionContext();
    }

    ~FileFormatTests()
    {
        Dispose(false);
    }

    protected abstract Task RegisterCustomersTableAsync(string tableName = "customers");
    
    protected abstract Task RegisterOrdersTableAsync(string tableName = "orders");
    
    protected abstract Task WriteTableAsync(DataFrame dataFrame, string path);
    
    protected string GenerateTempFileName(string fileNamePart = "")
    {
        return Path.Combine(Path.GetTempPath(), $"datafusion-sharp-write-test-output-{fileNamePart}-{Guid.NewGuid():N}{FileExtension}");
    }

    [Fact]
    public async Task RegisterTableAsync_CompletesSuccessfully()
    {
        // Arrange

        // Act & Assert
        await RegisterCustomersTableAsync();
    }

    [Theory]
    [InlineData("customers")]
    [InlineData("клієнти")]
    public async Task QueryTable_ReturnsData(string tableName)
    {
        // Arrange
        await RegisterCustomersTableAsync(tableName);

        // Act
        using var df = await Context.SqlAsync($"SELECT * FROM {tableName}");
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
    }
    
    [Fact]
    public async Task QueryMultipleTables_ReturnsData()
    {
        // Arrange
        await RegisterCustomersTableAsync();
        await RegisterOrdersTableAsync();

        // Act
        using var df = await Context.SqlAsync(
            """
            SELECT 
                c.customer_id,
                COUNT(o.order_id) as order_count
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id
            ORDER BY c.customer_id
            """);
        var count = await df.CountAsync();

        // Assert
        Assert.True(count > 0);
    }
    
    [Theory]
    [InlineData("customers")]
    [InlineData("клієнти")]
    public async Task WriteAsync_WritesFileSuccessfully(string fileNamePart)
    {
        // Arrange
        await RegisterCustomersTableAsync();
        using var df = await Context.SqlAsync("SELECT * FROM customers ORDER BY customer_id DESC LIMIT 2");
        var tempPath = GenerateTempFileName(fileNamePart);
        
        try
        {
            // Act
            await WriteTableAsync(df, tempPath);

            // Assert
            Assert.True(File.Exists(tempPath), "Output file should be created");
            Assert.True(new FileInfo(tempPath).Length > 0);
        }
        finally
        {
            // Cleanup
            if (File.Exists(tempPath))
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch
                {
                    TestOutputHelper.WriteLine($"Warning: Failed to delete temporary file {tempPath}. It may be locked or in use by another process.");
                    // Ignore exceptions during cleanup to avoid test failures due to file locks or other issues
                }
            }
        }
    }

    [Fact]
    public async Task WriteAsync_WithEmptyPath_ThrowsArgumentException()
    {
        // Arrange
        using var df = await Context.SqlAsync("SELECT 1");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => WriteTableAsync(df, string.Empty));
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
            return;
        
        Context.Dispose();
        Runtime.Dispose();
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}